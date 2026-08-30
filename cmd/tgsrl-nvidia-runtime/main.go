// Command tgsrl-nvidia-runtime controls registered local worker processes.
// Pause/resume/sleep can use POSIX signals; offload requires a managed-worker
// Unix socket that confirms safe-point, checkpoint, offload, reload, and readiness.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia/runtimehelper"
)

const helperIdentity = "tgsrl-nvidia-runtime"

type actionArgs struct {
	runtimehelper.ActionRequest
	timeout time.Duration
}

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
		return errors.New("command is required: capabilities, register, unregister, discover, receipts, pause, resume, sleep, or offload")
	}
	statePath := strings.TrimSpace(os.Getenv("TGSRL_NVIDIA_RUNTIME_STATE"))
	if statePath == "" {
		statePath = filepath.Join(os.TempDir(), "tgsrl-nvidia-runtime", "state.json")
	}
	store, err := runtimehelper.NewStore(statePath)
	if err != nil {
		return err
	}
	controller, err := runtimehelper.NewController(store)
	if err != nil {
		return err
	}
	switch argv[0] {
	case "capabilities":
		if err := requireCSVFormat(argv[1:]); err != nil {
			return err
		}
		_, err := fmt.Fprintln(stdout, helperIdentity+",1,pause,resume,sleep,offload,durable_receipts,generation_fence,idempotency,safe_point,checkpoint,reload,readiness")
		return err
	case "register":
		worker, err := parseWorker(argv[1:])
		if err != nil {
			return err
		}
		return controller.Register(nil, worker)
	case "unregister":
		fs := flag.NewFlagSet("unregister", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		sandbox := fs.String("sandbox", "", "sandbox identity")
		generation := fs.Uint64("generation", 0, "worker generation")
		if err := fs.Parse(argv[1:]); err != nil {
			return err
		}
		if fs.NArg() != 0 || strings.TrimSpace(*sandbox) == "" || *generation == 0 {
			return errors.New("unregister requires sandbox and generation")
		}
		return store.Unregister(*sandbox, *generation)
	case "discover", "receipts":
		if err := requireCSVFormat(argv[1:]); err != nil {
			return err
		}
		state, err := controller.Discover(nil)
		if err != nil {
			return err
		}
		if argv[0] == "discover" {
			return runtimehelper.WriteWorkersCSV(stdout, state)
		}
		return runtimehelper.WriteReceiptsCSV(stdout, state)
	case "pause", "resume", "sleep", "offload":
		args, err := parseAction(argv[0], argv[1:])
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), args.timeout)
		defer cancel()
		_, err = controller.Apply(ctx, args.ActionRequest)
		return err
	default:
		return fmt.Errorf("unsupported command %q", argv[0])
	}
}

func parseWorker(argv []string) (runtimehelper.Worker, error) {
	fs := flag.NewFlagSet("register", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var worker runtimehelper.Worker
	var priority int64
	fs.StringVar(&worker.SandboxID, "sandbox", "", "sandbox identity")
	fs.Uint64Var(&worker.Generation, "generation", 0, "worker generation")
	fs.IntVar(&worker.PID, "pid", 0, "worker process id")
	fs.Int64Var(&priority, "priority", 0, "worker priority")
	fs.StringVar(&worker.ControlSocket, "control-socket", "", "managed-worker Unix socket")
	fs.StringVar(&worker.SafePointFile, "safe-point-file", "", "safe-point marker file")
	fs.StringVar(&worker.ReadinessFile, "readiness-file", "", "readiness marker file")
	fs.StringVar(&worker.BindingID, "binding", "", "current binding identity")
	fs.StringVar(&worker.DeviceID, "device", "", "current device identity")
	fs.Float64Var(&worker.Share, "share", 0, "current accelerator share")
	if err := fs.Parse(argv); err != nil {
		return runtimehelper.Worker{}, err
	}
	if fs.NArg() != 0 || worker.SandboxID == "" || worker.Generation == 0 || worker.PID <= 0 {
		return runtimehelper.Worker{}, errors.New("register requires sandbox, generation, and pid")
	}
	if priority < -1<<31 || priority > 1<<31-1 {
		return runtimehelper.Worker{}, errors.New("priority exceeds int32")
	}
	worker.Priority = int32(priority)
	return worker, nil
}

func parseAction(operation string, argv []string) (actionArgs, error) {
	fs := flag.NewFlagSet(operation, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	result := actionArgs{ActionRequest: runtimehelper.ActionRequest{Operation: operation}, timeout: 15 * time.Second}
	fs.StringVar(&result.SandboxID, "sandbox", "", "sandbox identity")
	fs.Uint64Var(&result.Generation, "generation", 0, "worker generation fence")
	fs.StringVar(&result.IdempotencyKey, "idempotency-key", "", "idempotency key")
	fs.IntVar(&result.StepIndex, "step-index", 0, "transaction step index")
	fs.IntVar(&result.ExpectedActions, "expected-actions", 1, "transaction action count")
	fs.Uint64Var(&result.TransactionGeneration, "transaction-generation", 1, "transaction generation")
	fs.StringVar(&result.PlanDigest, "plan-digest", "standalone", "plan digest")
	fs.StringVar(&result.CommandDigest, "command-digest", "", "action digest")
	fs.StringVar(&result.ActionID, "action-id", "", "action identity")
	fs.StringVar(&result.PlanID, "plan-id", "", "plan identity")
	fs.BoolVar(&result.Recoverable, "recoverable", false, "publish this receipt for transaction recovery")
	fs.DurationVar(&result.timeout, "timeout", 15*time.Second, "managed-worker response timeout")
	if err := fs.Parse(argv); err != nil {
		return actionArgs{}, err
	}
	if fs.NArg() != 0 || result.SandboxID == "" || result.Generation == 0 || result.IdempotencyKey == "" || result.timeout <= 0 {
		return actionArgs{}, errors.New("lifecycle action requires sandbox, generation, idempotency-key, and positive timeout")
	}
	if result.CommandDigest == "" {
		result.CommandDigest = runtimehelper.RequestDigest(operation, result.SandboxID, result.Generation)
	}
	if result.ActionID == "" {
		result.ActionID = result.CommandDigest
	}
	if result.PlanID == "" {
		result.PlanID = "standalone"
	}
	return result, nil
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
