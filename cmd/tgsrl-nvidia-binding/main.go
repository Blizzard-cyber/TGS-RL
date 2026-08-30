// Command tgsrl-nvidia-binding provides the durable binding endpoint used by
// NVIDIA Driver v2. It records only scheduler-authorized binding metadata and
// never claims CUDA execution or isolation on its own.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia/bindinghelper"
)

const helperIdentity = "tgsrl-nvidia-binding"

type repeatedString []string

func (values *repeatedString) String() string { return strings.Join(*values, ",") }
func (values *repeatedString) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("value must not be empty")
	}
	*values = append(*values, value)
	return nil
}

type mutationArgs struct {
	sandbox               string
	target                string
	binding               string
	generation            uint64
	idempotencyKey        string
	devices               repeatedString
	share                 float64
	mpsServerPID          uint
	stepIndex             int
	expectedActions       int
	transactionGeneration uint64
	planDigest            string
	commandDigest         string
	actionID              string
	planID                string
	recoverable           bool
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(argv []string, stdout io.Writer) error {
	if len(argv) == 0 {
		return errors.New("command is required: capabilities, discover, receipts, bind, or release")
	}
	statePath := os.Getenv("TGSRL_NVIDIA_BINDING_STATE")
	if strings.TrimSpace(statePath) == "" {
		statePath = filepath.Join(os.TempDir(), "tgsrl-nvidia-binding", "state.json")
	}
	store, err := bindinghelper.NewStore(statePath)
	if err != nil {
		return err
	}
	switch argv[0] {
	case "capabilities":
		if err := requireCSVFormat(argv[1:]); err != nil {
			return err
		}
		features := []string{"bind", "release", "durable_receipts", "generation_fence", "idempotency"}
		if hasAnyLiveMPSPID() {
			features = append(features, "mps_profile_pid")
		}
		_, err := fmt.Fprintln(stdout, strings.Join(append([]string{helperIdentity, "1"}, features...), ","))
		return err
	case "discover", "receipts":
		if err := requireCSVFormat(argv[1:]); err != nil {
			return err
		}
		state, err := store.Snapshot()
		if err != nil {
			return err
		}
		if argv[0] == "discover" {
			refreshMPSPIDs(&state)
			return bindinghelper.WriteBindingsCSV(stdout, state)
		}
		return bindinghelper.WriteReceiptsCSV(stdout, state)
	case "bind", "release":
		parsed, err := parseMutationArgs(argv[0], argv[1:])
		if err != nil {
			return err
		}
		receipt := receiptFor(argv[0], parsed)
		if argv[0] == "release" {
			return store.ReleaseBinding(parsed.sandbox, parsed.target, parsed.generation, receipt)
		}
		binding := bindinghelper.Binding{SandboxID: parsed.sandbox, BindingID: parsed.binding, Generation: parsed.generation, DeviceIDs: parsed.devices, Share: parsed.share, IdempotencyKey: parsed.idempotencyKey, MPSServerPID: uint32(parsed.mpsServerPID)}
		if err := store.ApplyBinding(binding, receipt); err != nil {
			return err
		}
		return bindinghelper.WriteBindingsCSV(stdout, bindinghelper.State{Bindings: map[string]bindinghelper.Binding{binding.SandboxID: binding}})
	default:
		return fmt.Errorf("unsupported command %q", argv[0])
	}
}

func parseMutationArgs(command string, argv []string) (mutationArgs, error) {
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var result mutationArgs
	fs.StringVar(&result.sandbox, "sandbox", "", "sandbox identity")
	fs.StringVar(&result.target, "target", "", "allocation target")
	fs.StringVar(&result.binding, "binding", "", "binding identity")
	fs.Uint64Var(&result.generation, "generation", 0, "sandbox generation fence")
	fs.StringVar(&result.idempotencyKey, "idempotency-key", "", "idempotency key")
	fs.Var(&result.devices, "device", "stable GPU device identity")
	fs.Float64Var(&result.share, "share", 1, "initial device share")
	fs.IntVar(&result.stepIndex, "step-index", 0, "transaction step index")
	fs.IntVar(&result.expectedActions, "expected-actions", 1, "transaction action count")
	fs.Uint64Var(&result.transactionGeneration, "transaction-generation", 1, "transaction generation")
	fs.StringVar(&result.planDigest, "plan-digest", "standalone", "plan digest")
	fs.StringVar(&result.commandDigest, "command-digest", "", "action digest")
	fs.StringVar(&result.actionID, "action-id", "", "action identity")
	fs.StringVar(&result.planID, "plan-id", "", "plan identity")
	fs.BoolVar(&result.recoverable, "recoverable", false, "publish this receipt for transaction recovery")
	if err := fs.Parse(argv); err != nil {
		return mutationArgs{}, err
	}
	if fs.NArg() != 0 {
		return mutationArgs{}, errors.New("unexpected positional arguments")
	}
	if strings.TrimSpace(result.sandbox) == "" || result.generation == 0 || strings.TrimSpace(result.idempotencyKey) == "" {
		return mutationArgs{}, errors.New("sandbox, generation, and idempotency-key are required")
	}
	if result.stepIndex < 0 || result.expectedActions <= 0 || result.stepIndex >= result.expectedActions || result.transactionGeneration == 0 || strings.TrimSpace(result.planDigest) == "" {
		return mutationArgs{}, errors.New("transaction receipt identity is invalid")
	}
	if command == "bind" && (strings.TrimSpace(result.binding) == "" || len(result.devices) == 0) {
		return mutationArgs{}, errors.New("bind requires binding and at least one device")
	}
	var mpsServerPID uint32
	if command == "bind" {
		mpsServerPID = resolveMPSPID(result.sandbox)
	}
	if command == "release" && strings.TrimSpace(result.target) == "" {
		return mutationArgs{}, errors.New("release requires target")
	}
	if result.commandDigest == "" {
		if command == "bind" {
			result.commandDigest = bindinghelper.BindingRequestDigest(bindinghelper.Binding{SandboxID: result.sandbox, BindingID: result.binding, Generation: result.generation, DeviceIDs: result.devices, Share: result.share, IdempotencyKey: result.idempotencyKey, MPSServerPID: mpsServerPID})
		} else {
			result.commandDigest = bindinghelper.ReleaseRequestDigest(result.sandbox, result.target, result.generation)
		}
	}
	if result.actionID == "" {
		result.actionID = result.commandDigest
	}
	if result.planID == "" {
		result.planID = "standalone"
	}
	result.mpsServerPID = uint(mpsServerPID)
	return result, nil
}

func receiptFor(_ string, args mutationArgs) bindinghelper.Receipt {
	return bindinghelper.Receipt{StepIndex: args.stepIndex, ExpectedActions: args.expectedActions, PlanDigest: args.planDigest, ActionID: args.actionID, PlanID: args.planID, IdempotencyKey: args.idempotencyKey, SandboxID: args.sandbox, TransactionGeneration: args.transactionGeneration, ActionGeneration: args.generation, Succeeded: true, CommandDigest: args.commandDigest, Recoverable: args.recoverable}
}

func requireCSVFormat(argv []string) error {
	fs := flag.NewFlagSet("format", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	format := fs.String("format", "", "output format")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if fs.NArg() != 0 || *format != "csv" {
		return errors.New("--format=csv is required")
	}
	return nil
}

func configuredMPSPIDDirectory() string {
	directory := strings.TrimSpace(os.Getenv("TGSRL_NVIDIA_MPS_PID_DIR"))
	return directory
}

func resolveMPSPID(sandboxID string) uint32 {
	directory := configuredMPSPIDDirectory()
	if directory == "" || filepath.Base(sandboxID) != sandboxID {
		return 0
	}
	raw, err := os.ReadFile(filepath.Join(directory, sandboxID+".pid"))
	if err != nil {
		return 0
	}
	pid, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 32)
	if err != nil || pid == 0 {
		return 0
	}
	if err := syscall.Kill(int(pid), 0); err != nil && !errors.Is(err, syscall.EPERM) {
		return 0
	}
	return uint32(pid)
}

func refreshMPSPIDs(state *bindinghelper.State) {
	for sandboxID, binding := range state.Bindings {
		binding.MPSServerPID = resolveMPSPID(sandboxID)
		state.Bindings[sandboxID] = binding
	}
}

func hasAnyLiveMPSPID() bool {
	directory := configuredMPSPIDDirectory()
	if directory == "" {
		return false
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pid") {
			continue
		}
		sandboxID := strings.TrimSuffix(entry.Name(), ".pid")
		if resolveMPSPID(sandboxID) > 0 {
			return true
		}
	}
	return false
}
