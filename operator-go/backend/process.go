package backend

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	runtimehelper "github.com/Blizzard-cyber/TGS-RL/internal/managedworker"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

// ProcessConfig selects the local execution substrate used by CPU integration.
// It is deliberately separate from fake mode: fake remains an in-memory
// contract backend, while process mode launches and controls real child
// processes through the same managed-worker registry used by Kubernetes.
type ProcessConfig struct {
	BootstrapBinary string
	StateDirectory  string
}

var processHostEnvironmentAllowlist = map[string]struct{}{
	"DYLD_LIBRARY_PATH": {},
	"LANG":              {},
	"LC_ALL":            {},
	"LD_LIBRARY_PATH":   {},
	"PATH":              {},
	"PYTHONPATH":        {},
	"SSL_CERT_DIR":      {},
	"SSL_CERT_FILE":     {},
	"TEMP":              {},
	"TMP":               {},
	"TMPDIR":            {},
	"TZ":                {},
	"VIRTUAL_ENV":       {},
}

type managedProcess struct {
	command    *exec.Cmd
	generation uint64
	done       chan struct{}
	err        error
}

// ProcessBackend materializes compiled bundles as local managed processes.
// The inner fake backend retains the Kubernetes object/control contract; all
// RUNNING and terminal process state comes from the worker registry.
type ProcessBackend struct {
	inner     *FakeBackend
	bootstrap string
	stateDir  string

	operationMu sync.Mutex
	mu          sync.Mutex
	processes   map[string]*managedProcess
}

func NewProcess(config ProcessConfig) (*ProcessBackend, error) {
	bootstrap := strings.TrimSpace(config.BootstrapBinary)
	if bootstrap == "" {
		return nil, fmt.Errorf("process backend requires a bootstrap binary")
	}
	resolved, err := exec.LookPath(bootstrap)
	if err != nil {
		return nil, fmt.Errorf("resolve process bootstrap binary: %w", err)
	}
	stateDir := strings.TrimSpace(config.StateDirectory)
	if stateDir == "" || !filepath.IsAbs(stateDir) {
		return nil, fmt.Errorf("process backend requires an absolute state directory")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("create process backend state directory: %w", err)
	}
	return &ProcessBackend{
		inner:     NewFake(),
		bootstrap: resolved,
		stateDir:  stateDir,
		processes: make(map[string]*managedProcess),
	}, nil
}

func (b *ProcessBackend) Apply(ctx context.Context, bundle *api.Bundle) (*ApplyResult, error) {
	b.operationMu.Lock()
	defer b.operationMu.Unlock()
	result, err := b.inner.Apply(ctx, bundle)
	if err != nil {
		return nil, err
	}
	if err := b.ensureProcess(ctx, result.Bundle); err != nil {
		return nil, err
	}
	return result, nil
}

func (b *ProcessBackend) Get(ctx context.Context, key string) (*api.Bundle, bool, error) {
	return b.inner.Get(ctx, key)
}

func (b *ProcessBackend) List(ctx context.Context) ([]*api.Bundle, error) {
	return b.inner.List(ctx)
}

func (b *ProcessBackend) Cleanup(ctx context.Context, key string, generation uint64) error {
	b.operationMu.Lock()
	defer b.operationMu.Unlock()
	b.terminate(key, generation)
	return b.inner.Cleanup(ctx, key, generation)
}

func (b *ProcessBackend) DiscoverCapabilities(ctx context.Context) (compiler.CapabilitySet, error) {
	return b.inner.DiscoverCapabilities(ctx)
}

func (b *ProcessBackend) SetControlStatePath(path string) error {
	return b.inner.SetControlStatePath(path)
}

func (b *ProcessBackend) RestoreControlMetadata(ctx context.Context) error {
	return b.inner.RestoreControlMetadata(ctx)
}

func (b *ProcessBackend) Run(ctx context.Context) error {
	<-ctx.Done()
	b.mu.Lock()
	processes := make([]*managedProcess, 0, len(b.processes))
	for _, process := range b.processes {
		processes = append(processes, process)
	}
	b.mu.Unlock()
	for _, process := range processes {
		terminateManagedProcess(process, 5*time.Second)
	}
	return nil
}

func (b *ProcessBackend) Control(ctx context.Context, request ControlRequest) (*ControlResult, error) {
	b.operationMu.Lock()
	defer b.operationMu.Unlock()
	if err := validateControlRequest(request); err != nil {
		return nil, err
	}
	if len(request.Targets) != 1 {
		return nil, fmt.Errorf("%w: process backend requires exactly one target", ErrInvalidControl)
	}
	operation := map[tgsrlv1.JobCommandType]string{
		tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE:     "pause",
		tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:    "resume",
		tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP:      "stop",
		tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE: "terminate",
	}[request.Action]
	if operation == "" {
		return nil, fmt.Errorf("%w: unsupported process lifecycle action %s", ErrInvalidControl, request.Action)
	}
	for _, target := range request.Targets {
		bundle, token, registryURL, err := b.bindingCredential(ctx, request, target)
		if err != nil {
			return nil, err
		}
		_, err = runtimehelper.ApplyRemoteWorkerAction(ctx, registryURL, token, runtimehelper.WorkerActionRequest{
			Action:         operation,
			SandboxID:      target.SandboxID,
			Generation:     target.ExpectedGeneration,
			IdempotencyKey: request.IdempotencyKey + ":" + target.SandboxID,
		})
		if err != nil {
			return nil, fmt.Errorf("control process bundle %q: %w", bundle.Key, err)
		}
	}
	result, err := b.inner.Control(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("%w: managed worker changed before backend control commit: %v", ErrControlOutcomeAmbiguous, err)
	}
	return result, nil
}

func (b *ProcessBackend) Snapshot(ctx context.Context, bundle *api.Bundle) (*ObservationSnapshot, bool, error) {
	b.operationMu.Lock()
	defer b.operationMu.Unlock()
	base, terminal, err := b.inner.Snapshot(ctx, bundle)
	if err != nil || base == nil || terminal {
		return base, terminal, err
	}
	base.WorkerRegistrationRequired = true
	base.PodReady = false
	base.JobActive = 1
	for _, target := range bundle.RuntimeTargets {
		token, registryURL, err := processCredential(bundle, target.SandboxID)
		if err != nil {
			return nil, false, err
		}
		worker, found, err := runtimehelper.GetRemoteWorker(ctx, registryURL, token, runtimehelper.WorkerStatusRequest{SandboxID: target.SandboxID, Generation: target.Generation})
		if err != nil {
			return nil, false, err
		}
		if !found {
			if process := b.processFor(bundle.Key, bundle.Generation); process != nil {
				select {
				case <-process.done:
					base.JobActive = 0
					base.JobFailed = 1
					base.Reason = "managed process exited before registration"
					if process.err != nil {
						base.Reason += ": " + process.err.Error()
					}
					return base, true, nil
				default:
				}
			}
			base.Reason = "waiting for managed-worker registration"
			return base, false, nil
		}
		switch worker.State {
		case "running":
			base.PodReady = worker.Ready
			base.JobPaused = false
		case "paused", "sleeping":
			base.JobActive = 0
			base.JobPaused = true
		case "failed":
			base.JobActive = 0
			base.JobFailed = 1
			base.Reason = worker.Detail
			return base, true, nil
		case "terminated":
			base.JobActive = 0
			if worker.LastOperation == "stop" {
				base.Reason = "waiting for committed process stop metadata"
				return base, false, nil
			}
			base.JobDeleted = true
			base.Reason = worker.Detail
			return base, true, nil
		default:
			return nil, false, fmt.Errorf("unsupported registered worker state %q", worker.State)
		}
	}
	base.Reason = "managed worker observed through registry"
	return base, false, nil
}

func (b *ProcessBackend) bindingCredential(ctx context.Context, request ControlRequest, target ControlTarget) (*api.Bundle, string, string, error) {
	bundles, err := b.inner.List(ctx)
	if err != nil {
		return nil, "", "", err
	}
	for _, bundle := range bundles {
		if !request.GlobalTargetLookup && (bundle.SourceRunID != request.RunID || bundle.SourceJobID != request.JobID) {
			continue
		}
		for _, candidate := range bundle.RuntimeTargets {
			if candidate.RuntimeUnitID == target.RuntimeUnitID && candidate.SandboxID == target.SandboxID && candidate.Generation == target.ExpectedGeneration {
				token, registryURL, err := processCredential(bundle, target.SandboxID)
				return bundle, token, registryURL, err
			}
		}
	}
	return nil, "", "", ErrRuntimeNotFound
}

func processCredential(bundle *api.Bundle, sandboxID string) (string, string, error) {
	if bundle == nil || len(bundle.Job.Spec.Template.Spec.Containers) != 1 {
		return "", "", fmt.Errorf("process bundle requires one workload container")
	}
	values := environmentValues(bundle.Job.Spec.Template.Spec.Containers[0].Env)
	if values["TGSRL_SANDBOX_ID"] != sandboxID || values["TGSRL_WORKER_REGISTRY_TOKEN"] == "" || values["TGSRL_WORKER_REGISTRY_URL"] == "" {
		return "", "", fmt.Errorf("process bundle lacks scoped worker registry credentials")
	}
	return values["TGSRL_WORKER_REGISTRY_TOKEN"], values["TGSRL_WORKER_REGISTRY_URL"], nil
}

func (b *ProcessBackend) ensureProcess(ctx context.Context, bundle *api.Bundle) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if bundle == nil || len(bundle.RuntimeTargets) != 1 || len(bundle.Job.Spec.Template.Spec.Containers) != 1 {
		return fmt.Errorf("process backend requires one target and one workload container per bundle")
	}
	target := bundle.RuntimeTargets[0]
	b.mu.Lock()
	if current := b.processes[bundle.Key]; current != nil {
		select {
		case <-current.done:
			if current.generation == bundle.Generation {
				b.mu.Unlock()
				return fmt.Errorf("process generation %d already exited", current.generation)
			}
			delete(b.processes, bundle.Key)
		default:
			if current.generation == bundle.Generation {
				b.mu.Unlock()
				return nil
			}
			b.mu.Unlock()
			return fmt.Errorf("process generation %d is still running", current.generation)
		}
	}
	b.mu.Unlock()
	token, registryURL, err := processCredential(bundle, target.SandboxID)
	if err != nil {
		return err
	}
	registered, found, err := runtimehelper.GetRemoteWorker(ctx, registryURL, token, runtimehelper.WorkerStatusRequest{SandboxID: target.SandboxID, Generation: target.Generation})
	if err != nil {
		return fmt.Errorf("reconcile managed process registration: %w", err)
	}
	if found {
		if registered.State == "failed" || registered.State == "terminated" {
			return fmt.Errorf("process generation %d is already %s", registered.Generation, registered.State)
		}
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if current := b.processes[bundle.Key]; current != nil {
		return fmt.Errorf("process launch raced with generation %d", current.generation)
	}
	container := bundle.Job.Spec.Template.Spec.Containers[0]
	argv := append([]string(nil), container.Command...)
	argv = append(argv, container.Args...)
	if len(argv) == 0 || filepath.Base(argv[0]) != "tgsrl-worker-bootstrap" {
		return fmt.Errorf("process workload is not wrapped by managed-worker bootstrap")
	}
	argv[0] = b.bootstrap
	for index := 1; index+1 < len(argv); index++ {
		if argv[index] == "--listen" {
			argv[index+1] = "127.0.0.1:0"
			break
		}
	}
	root := filepath.Join(b.stateDir, safeProcessName(target.SandboxID), strconv.FormatUint(bundle.Generation, 10))
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	environment := environmentValues(container.Env)
	environment["TGSRL_POD_IP"] = "127.0.0.1"
	environment["TGSRL_POD_UID"] = "process-" + safeProcessName(target.SandboxID) + "-" + strconv.FormatUint(bundle.Generation, 10)
	if environment["TGSRL_VERL_CONTROL_SOCKET"] == "" {
		environment["TGSRL_VERL_CONTROL_SOCKET"] = filepath.Join(root, "verl.sock")
	}
	if environment["TGSRL_VERL_TRACE_PATH"] == "" {
		environment["TGSRL_VERL_TRACE_PATH"] = filepath.Join(root, "verl.ndjson")
	}
	if environment["TGSRL_VERL_STATE_PATH"] == "" {
		environment["TGSRL_VERL_STATE_PATH"] = filepath.Join(root, "verl-state.json")
	}
	if environment["TGSRL_VERL_CHECKPOINT_ROOT"] == "" {
		environment["TGSRL_VERL_CHECKPOINT_ROOT"] = filepath.Join(root, "checkpoints")
	}
	command := exec.Command(argv[0], argv[1:]...)
	command.Env = mergedEnvironment(environment)
	command.Dir = environment["TGSRL_WORKING_DIRECTORY"]
	logFile, err := os.OpenFile(filepath.Join(root, "workload.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	command.Stdout, command.Stderr = logFile, logFile
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return fmt.Errorf("start managed process: %w", err)
	}
	process := &managedProcess{command: command, generation: bundle.Generation, done: make(chan struct{})}
	b.processes[bundle.Key] = process
	go func() {
		process.err = command.Wait()
		_ = logFile.Close()
		close(process.done)
	}()
	return nil
}

func (b *ProcessBackend) processFor(key string, generation uint64) *managedProcess {
	b.mu.Lock()
	defer b.mu.Unlock()
	process := b.processes[key]
	if process == nil || process.generation != generation {
		return nil
	}
	return process
}

func (b *ProcessBackend) terminate(key string, generation uint64) {
	b.mu.Lock()
	process := b.processes[key]
	if process != nil && process.generation == generation {
		delete(b.processes, key)
	} else {
		process = nil
	}
	b.mu.Unlock()
	if process != nil {
		terminateManagedProcess(process, 5*time.Second)
	}
}

func terminateManagedProcess(process *managedProcess, timeout time.Duration) {
	if process == nil || process.command == nil || process.command.Process == nil {
		return
	}
	select {
	case <-process.done:
		return
	default:
	}
	_ = syscall.Kill(-process.command.Process.Pid, syscall.SIGTERM)
	select {
	case <-process.done:
	case <-time.After(timeout):
		_ = syscall.Kill(-process.command.Process.Pid, syscall.SIGKILL)
		<-process.done
	}
}

func environmentValues(values []api.EnvVar) map[string]string {
	result := make(map[string]string, len(values))
	for _, value := range values {
		if value.ValueFrom == nil {
			result[value.Name] = value.Value
		}
	}
	return result
}

func sortedEnvironment(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for key, value := range values {
		result = append(result, key+"="+value)
	}
	// os/exec does not require sorting, but deterministic child environments
	// make process evidence and tests stable.
	sort.Strings(result)
	return result
}

func mergedEnvironment(overrides map[string]string) []string {
	values := make(map[string]string, len(os.Environ())+len(overrides))
	for _, item := range os.Environ() {
		key, value, found := strings.Cut(item, "=")
		_, allowed := processHostEnvironmentAllowlist[key]
		if found && allowed {
			values[key] = value
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	return sortedEnvironment(values)
}

func safeProcessName(value string) string {
	value = strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, value)
	value = strings.Trim(value, "-")
	if value == "" {
		return "worker"
	}
	return value
}

var _ Backend = (*ProcessBackend)(nil)
