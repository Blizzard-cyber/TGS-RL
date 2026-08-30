// Command tgsrl-nvidia-mig coordinates safe MIG replacement with a registered
// managed worker. It targets an already discovered MIG instance and never
// destroys or creates MIG topology behind the scheduler's device ledger.
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia/bindinghelper"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia/runtimehelper"
)

const helperIdentity = "tgsrl-nvidia-mig"

type mutationArgs struct {
	runtimehelper.ActionRequest
	parentUUID      string
	sourceUUID      string
	sourceBindingID string
	targetUUID      string
	profile         string
	bindingID       string
	share           float64
	timeout         time.Duration
}

var executeNVIDIACommand = executeCommand
var lookupExecutable = exec.LookPath

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, runtimehelper.ErrOutcomeUnknown) {
			os.Exit(75)
		}
		os.Exit(1)
	}
}

func run(argv []string, stdout io.Writer) error {
	if len(argv) == 0 {
		return errors.New("command is required: capabilities, inventory, receipts, rebind, or recreate")
	}
	switch argv[0] {
	case "capabilities":
		if err := requireCSVFormat(argv[1:]); err != nil {
			return err
		}
		if _, err := lookupExecutable(nvidiaSMIBinary()); err != nil {
			_, err := fmt.Fprintln(stdout, helperIdentity+",1")
			return err
		}
		controller, err := runtimeController()
		if err != nil {
			return err
		}
		state, err := controller.Discover(context.Background())
		if err != nil {
			_, err := fmt.Fprintln(stdout, helperIdentity+",1")
			return err
		}
		// Capability discovery is also the MIG backend's restart reconciliation
		// point. Complete a binding-authority write that may have been interrupted
		// after the managed worker already reached the replacement generation.
		if err := synchronizePendingBindings(state); err != nil {
			return err
		}
		if !hasManagedWorker(state) {
			_, err := fmt.Fprintln(stdout, helperIdentity+",1")
			return err
		}
		_, err = fmt.Fprintln(stdout, helperIdentity+",1,rebind,recreate,durable_receipts,generation_fence,idempotency,safe_point,checkpoint,stop,restore,readiness")
		return err
	case "inventory":
		if err := requireCSVFormat(argv[1:]); err != nil {
			return err
		}
		return runInventory(stdout)
	case "receipts":
		if err := requireCSVFormat(argv[1:]); err != nil {
			return err
		}
		controller, err := runtimeController()
		if err != nil {
			return err
		}
		state, err := controller.Discover(context.Background())
		if err != nil {
			return err
		}
		if err := synchronizePendingBindings(state); err != nil {
			return err
		}
		state, err = controller.Discover(context.Background())
		if err != nil {
			return err
		}
		return runtimehelper.WriteReceiptsCSV(stdout, state)
	case "rebind", "recreate":
		args, err := parseMutation(argv[0], argv[1:])
		if err != nil {
			return err
		}
		controller, err := runtimeController()
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), args.timeout)
		defer cancel()
		worker, err := controller.Reconfigure(ctx, args.ActionRequest, func(ctx context.Context, _ runtimehelper.Worker) (string, error) {
			return executeMutation(ctx, args)
		})
		if err != nil {
			return err
		}
		if err := synchronizeBinding(args, worker); err != nil {
			return fmt.Errorf("%w: synchronize binding authority: %v", runtimehelper.ErrOutcomeUnknown, err)
		}
		_, err = fmt.Fprintf(stdout, "%s,%s,%s,%d\n", args.parentUUID, worker.DeviceID, args.profile, worker.Generation)
		return err
	default:
		return fmt.Errorf("unsupported command %q", argv[0])
	}
}

func synchronizePendingBindings(state runtimehelper.State) error {
	store, err := runtimehelper.NewStore(runtimeStatePath())
	if err != nil {
		return err
	}
	pending, err := store.BindingSyncPending()
	if err != nil {
		return err
	}
	for _, receipt := range pending {
		worker, ok := state.Workers[receipt.SandboxID]
		if !ok || worker.Generation != receipt.TargetGeneration || worker.BindingID != receipt.TargetBindingID || worker.DeviceID != receipt.TargetDeviceID || worker.Share != receipt.TargetShare {
			return fmt.Errorf("runtime state does not match pending binding synchronization for %q", receipt.IdempotencyKey)
		}
		args := mutationArgs{ActionRequest: runtimehelper.ActionRequest{SandboxID: receipt.SandboxID, IdempotencyKey: receipt.IdempotencyKey, TransactionGeneration: receipt.TransactionGeneration, PlanDigest: receipt.PlanDigest, CommandDigest: receipt.CommandDigest, ActionID: receipt.ActionID, PlanID: receipt.PlanID}, parentUUID: receipt.TargetParentID, sourceUUID: receipt.SourceDeviceID, sourceBindingID: receipt.SourceBindingID, targetUUID: receipt.TargetDeviceID, profile: receipt.TargetProfile, bindingID: receipt.TargetBindingID, share: receipt.TargetShare}
		args.Generation, args.TargetGeneration = receipt.ActionGeneration, receipt.TargetGeneration
		if err := synchronizeBinding(args, worker); err != nil {
			return err
		}
	}
	return nil
}

func hasManagedWorker(state runtimehelper.State) bool {
	for _, worker := range state.Workers {
		if worker.ControlSocket != "" && worker.BindingID != "" && strings.HasPrefix(worker.DeviceID, "MIG-") && worker.Share > 0 && worker.State != "failed" && worker.State != "terminated" {
			return true
		}
	}
	return false
}

func runtimeController() (*runtimehelper.Controller, error) {
	path := runtimeStatePath()
	store, err := runtimehelper.NewStore(path)
	if err != nil {
		return nil, err
	}
	return runtimehelper.NewController(store)
}

func runtimeStatePath() string {
	if path := strings.TrimSpace(os.Getenv("TGSRL_NVIDIA_RUNTIME_STATE")); path != "" {
		return path
	}
	return filepath.Join(os.TempDir(), "tgsrl-nvidia-runtime", "state.json")
}

func runInventory(stdout io.Writer) error {
	rows, err := discoverInventory(context.Background())
	if err != nil {
		return err
	}
	writer := csv.NewWriter(stdout)
	for _, row := range rows {
		if err := writer.Write(row[:3]); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func parseMutation(operation string, argv []string) (mutationArgs, error) {
	fs := flag.NewFlagSet(operation, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	result := mutationArgs{ActionRequest: runtimehelper.ActionRequest{Operation: operation}, timeout: 30 * time.Second}
	fs.StringVar(&result.SandboxID, "sandbox", "", "sandbox identity")
	fs.StringVar(&result.parentUUID, "parent-uuid", "", "target parent GPU UUID")
	fs.StringVar(&result.sourceUUID, "source-uuid", "", "current source MIG UUID")
	fs.StringVar(&result.sourceBindingID, "source-binding", "", "current binding identity")
	fs.StringVar(&result.targetUUID, "target-uuid", "", "existing target MIG UUID")
	fs.StringVar(&result.profile, "profile", "", "requested MIG profile")
	fs.StringVar(&result.bindingID, "binding", "", "replacement binding identity")
	fs.Float64Var(&result.share, "share", 1, "replacement accelerator share")
	fs.Uint64Var(&result.Generation, "generation", 0, "current worker generation fence")
	fs.Uint64Var(&result.TargetGeneration, "target-generation", 0, "replacement worker generation")
	fs.StringVar(&result.IdempotencyKey, "idempotency-key", "", "idempotency key")
	fs.IntVar(&result.StepIndex, "step-index", 0, "transaction step index")
	fs.IntVar(&result.ExpectedActions, "expected-actions", 1, "transaction action count")
	fs.Uint64Var(&result.TransactionGeneration, "transaction-generation", 1, "transaction generation")
	fs.StringVar(&result.PlanDigest, "plan-digest", "standalone", "plan digest")
	fs.StringVar(&result.CommandDigest, "command-digest", "", "action digest")
	fs.StringVar(&result.ActionID, "action-id", "", "action identity")
	fs.StringVar(&result.PlanID, "plan-id", "", "plan identity")
	fs.BoolVar(&result.Recoverable, "recoverable", false, "publish this receipt for transaction recovery")
	fs.DurationVar(&result.timeout, "timeout", 30*time.Second, "MIG transaction timeout")
	if err := fs.Parse(argv); err != nil {
		return mutationArgs{}, err
	}
	if fs.NArg() != 0 || result.SandboxID == "" || result.parentUUID == "" || result.sourceBindingID == "" || !strings.HasPrefix(result.sourceUUID, "MIG-") || !strings.HasPrefix(result.targetUUID, "MIG-") || result.profile == "" || result.bindingID == "" || result.share <= 0 || result.share > 1 || result.Generation == 0 || result.TargetGeneration == 0 || result.IdempotencyKey == "" || result.timeout <= 0 {
		return mutationArgs{}, errors.New("MIG mutation requires sandbox, parent-uuid, source binding/UUID, target binding/UUID, profile, valid share, source/target generations, idempotency-key, and positive timeout")
	}
	if (operation == "rebind" && result.sourceUUID == result.targetUUID) || (operation == "recreate" && result.sourceUUID != result.targetUUID) {
		return mutationArgs{}, fmt.Errorf("%s source and target MIG UUIDs are inconsistent", operation)
	}
	if result.TargetGeneration != result.Generation+1 {
		return mutationArgs{}, errors.New("MIG mutation target generation must be the next generation")
	}
	result.TargetDeviceID, result.TargetProfile, result.TargetParentID, result.TargetBindingID, result.TargetShare = result.targetUUID, result.profile, result.parentUUID, result.bindingID, result.share
	result.SourceBindingID, result.SourceDeviceID = result.sourceBindingID, result.sourceUUID
	if result.CommandDigest == "" {
		result.CommandDigest = runtimehelper.RequestDigest(operation, result.SandboxID, result.Generation, result.parentUUID, result.sourceBindingID, result.sourceUUID, result.targetUUID, result.profile, result.bindingID, strconv.FormatFloat(result.share, 'g', -1, 64))
	}
	if result.ActionID == "" {
		result.ActionID = result.CommandDigest
	}
	if result.PlanID == "" {
		result.PlanID = "standalone"
	}
	return result, nil
}

func synchronizeBinding(args mutationArgs, worker runtimehelper.Worker) error {
	path := strings.TrimSpace(os.Getenv("TGSRL_NVIDIA_BINDING_STATE"))
	if path == "" {
		return errors.New("TGSRL_NVIDIA_BINDING_STATE is required")
	}
	store, err := bindinghelper.NewStore(path)
	if err != nil {
		return err
	}
	key := args.IdempotencyKey + ":binding"
	binding := bindinghelper.Binding{SandboxID: args.SandboxID, BindingID: args.bindingID, Generation: worker.Generation, DeviceIDs: []string{worker.DeviceID}, Share: args.share, IdempotencyKey: key}
	receipt := bindinghelper.Receipt{StepIndex: 0, ExpectedActions: 1, PlanDigest: "mig-binding-sync:" + args.PlanDigest, ActionID: args.ActionID + ":binding", PlanID: "mig-binding-sync:" + args.PlanID, IdempotencyKey: key, SandboxID: args.SandboxID, TransactionGeneration: args.TransactionGeneration, ActionGeneration: worker.Generation, Succeeded: true, CommandDigest: args.CommandDigest}
	if err := store.ReplaceBinding(bindinghelper.ReplaceRequest{SandboxID: args.SandboxID, SourceBindingID: args.sourceBindingID, SourceDeviceID: args.sourceUUID, SourceGeneration: args.Generation, Target: binding}, receipt); err != nil {
		return err
	}
	runtimeStore, err := runtimehelper.NewStore(runtimeStatePath())
	if err != nil {
		return err
	}
	return runtimeStore.MarkBindingSynchronized(args.IdempotencyKey)
}

func executeMutation(ctx context.Context, args mutationArgs) (string, error) {
	rows, err := discoverInventory(ctx)
	if err != nil {
		return "", err
	}
	sourceFound := false
	for _, row := range rows {
		if row[0] == args.sourceUUID {
			sourceFound = true
		}
		if row[0] == args.targetUUID && row[1] == args.profile && row[3] == args.parentUUID {
			if sourceFound || args.sourceUUID == args.targetUUID {
				return args.targetUUID, nil
			}
		}
	}
	if !sourceFound {
		return "", fmt.Errorf("source MIG device %q is unavailable", args.sourceUUID)
	}
	for _, row := range rows {
		if row[0] == args.targetUUID && row[1] == args.profile && row[3] == args.parentUUID {
			return args.targetUUID, nil
		}
	}
	return "", fmt.Errorf("target MIG device %q with profile %q is unavailable on parent %q", args.targetUUID, args.profile, args.parentUUID)
}

func discoverInventory(ctx context.Context) ([][]string, error) {
	output, err := executeNVIDIACommand(ctx, []string{nvidiaSMIBinary(), "-L"})
	if err != nil {
		return nil, err
	}
	devices, err := parseMIGList(output)
	if err != nil {
		return nil, err
	}
	rows := make([][]string, 0, len(devices))
	for _, device := range devices {
		memoryMB, err := profileMemoryMiB(device.profile)
		if err != nil {
			return nil, fmt.Errorf("MIG device %q: %w", device.uuid, err)
		}
		rows = append(rows, []string{device.uuid, device.profile, strconv.FormatUint(memoryMB, 10), device.parent})
	}
	return rows, nil
}

type migDevice struct {
	parent  string
	profile string
	uuid    string
}

func parseMIGList(output []byte) ([]migDevice, error) {
	parent := ""
	result := make([]migDevice, 0)
	seen := make(map[string]struct{})
	for _, raw := range strings.Split(string(output), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "GPU ") {
			parent = uuidFromLine(line, "GPU-")
			if parent == "" {
				return nil, fmt.Errorf("invalid GPU inventory row %q", line)
			}
			continue
		}
		if !strings.HasPrefix(line, "MIG ") {
			continue
		}
		uuid := uuidFromLine(line, "MIG-")
		deviceAt := strings.Index(line, " Device ")
		if parent == "" || uuid == "" || deviceAt < len("MIG ") {
			return nil, fmt.Errorf("invalid MIG inventory row %q", line)
		}
		profile := strings.TrimSpace(line[len("MIG "):deviceAt])
		if profile == "" {
			return nil, fmt.Errorf("MIG inventory row %q has no profile", line)
		}
		if _, duplicate := seen[uuid]; duplicate {
			return nil, fmt.Errorf("MIG inventory duplicates UUID %q", uuid)
		}
		seen[uuid] = struct{}{}
		result = append(result, migDevice{parent: parent, profile: profile, uuid: uuid})
	}
	return result, nil
}

func profileMemoryMiB(profile string) (uint64, error) {
	lower := strings.ToLower(strings.TrimSpace(profile))
	end := strings.LastIndex(lower, "gb")
	if end <= 0 {
		return 0, fmt.Errorf("profile %q has no memory capacity", profile)
	}
	start := end - 1
	for start >= 0 && lower[start] >= '0' && lower[start] <= '9' {
		start--
	}
	gib, err := strconv.ParseUint(lower[start+1:end], 10, 63)
	if err != nil || gib == 0 || gib > ^uint64(0)>>10 {
		return 0, fmt.Errorf("profile %q has invalid memory capacity", profile)
	}
	return gib << 10, nil
}

func uuidFromLine(line, prefix string) string {
	marker := "(UUID: "
	start, end := strings.LastIndex(line, marker), strings.LastIndex(line, ")")
	if start < 0 || end <= start+len(marker) {
		return ""
	}
	uuid := strings.TrimSpace(line[start+len(marker) : end])
	if !strings.HasPrefix(uuid, prefix) {
		return ""
	}
	return uuid
}

func parseInventory(output []byte) ([][]string, error) {
	reader := csv.NewReader(bytes.NewReader(output))
	reader.TrimLeadingSpace = true
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse MIG inventory: %w", err)
	}
	seen := make(map[string]struct{}, len(rows))
	for index, row := range rows {
		if len(row) != 4 {
			return nil, fmt.Errorf("MIG inventory row %d has %d fields, want 4", index+1, len(row))
		}
		for field := range row {
			row[field] = strings.TrimSpace(row[field])
		}
		memoryMB, parseErr := strconv.ParseUint(row[2], 10, 63)
		if parseErr != nil || !strings.HasPrefix(row[0], "MIG-") || row[1] == "" || memoryMB == 0 || !strings.HasPrefix(row[3], "GPU-") {
			return nil, fmt.Errorf("MIG inventory row %d is invalid", index+1)
		}
		if _, duplicate := seen[row[0]]; duplicate {
			return nil, fmt.Errorf("MIG inventory duplicates UUID %q", row[0])
		}
		seen[row[0]] = struct{}{}
	}
	return rows, nil
}

func executeCommand(ctx context.Context, argv []string) ([]byte, error) {
	command := exec.CommandContext(ctx, argv[0], argv[1:]...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		var execError *exec.Error
		if (errors.As(err, &execError) && errors.Is(execError.Err, exec.ErrNotFound)) || errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: MIG executor is unavailable", runtimehelper.ErrRequestNotDelivered)
		}
		return nil, fmt.Errorf("MIG executor failed: %s: %w", strings.TrimSpace(stderr.String()), err)
	}
	return output, nil
}

func nvidiaSMIBinary() string {
	if binary := strings.TrimSpace(os.Getenv("TGSRL_NVIDIA_SMI")); binary != "" {
		return binary
	}
	return "nvidia-smi"
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
