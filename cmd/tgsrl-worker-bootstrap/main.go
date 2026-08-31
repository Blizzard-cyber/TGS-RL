// Command tgsrl-worker-bootstrap runs as workload PID 1, supervises one user
// process, and registers its exact identity with the TGS-RL runtime registry.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider/nvidia/runtimehelper"
)

var deviceUUIDPattern = regexp.MustCompile(`(?:GPU|MIG)-[A-Za-z0-9][A-Za-z0-9./_-]*`)

type config struct {
	registryURL      string
	registryToken    string
	listenAddress    string
	advertiseHost    string
	controlSocket    string
	safePointFile    string
	readinessFile    string
	mpsPIDDirectory  string
	verifyDevices    bool
	deviceCommand    string
	registrationWait time.Duration
	controlTimeout   time.Duration
	shutdownWait     time.Duration
	command          []string
	signals          <-chan os.Signal
}

type supervisor struct {
	mu             sync.Mutex
	worker         runtimehelper.Worker
	controlToken   string
	controlSocket  string
	safePointFile  string
	readinessFile  string
	controlTimeout time.Duration
	receipts       map[string]receipt
	registered     bool
}

type receipt struct {
	digest   string
	response runtimehelper.ControlResponse
}

type processWait struct {
	done chan struct{}
	err  error
}

type mpsPIDRecord struct {
	Generation   uint64 `json:"generation"`
	PID          uint32 `json:"pid"`
	ProcessToken string `json:"process_token"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(argv []string) error {
	if len(argv) > 0 && argv[0] == "install" {
		return install(argv[1:])
	}
	cfg, err := parseConfig(argv)
	if err != nil {
		return err
	}
	return runWorker(cfg)
}

func install(argv []string) error {
	fs := flag.NewFlagSet("install", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	target := fs.String("target", "", "installation target path")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if fs.NArg() != 0 || strings.TrimSpace(*target) == "" {
		return errors.New("install requires --target")
	}
	cleanTarget := filepath.Clean(*target)
	if !filepath.IsAbs(cleanTarget) || filepath.Base(cleanTarget) != "tgsrl-worker-bootstrap" || strings.Contains(cleanTarget, "..") {
		return errors.New("install target must be an absolute tgsrl-worker-bootstrap path")
	}
	self, err := os.Executable()
	if err != nil {
		return err
	}
	input, err := os.Open(self)
	if err != nil {
		return err
	}
	defer input.Close()
	if err := os.MkdirAll(filepath.Dir(cleanTarget), 0o755); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(cleanTarget), ".bootstrap-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := io.Copy(temp, input); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Chmod(0o755); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, cleanTarget); err != nil {
		return err
	}
	ok = true
	return nil
}

func parseConfig(argv []string) (config, error) {
	separator := -1
	for index, value := range argv {
		if value == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || separator == len(argv)-1 {
		return config{}, errors.New("bootstrap requires -- followed by the workload command")
	}
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg := config{}
	fs.StringVar(&cfg.registryURL, "registry-url", env("TGSRL_WORKER_REGISTRY_URL"), "worker registry base URL")
	fs.StringVar(&cfg.registryToken, "registry-token", env("TGSRL_WORKER_REGISTRY_TOKEN"), "worker registry token")
	fs.StringVar(&cfg.listenAddress, "listen", firstNonEmpty(env("TGSRL_WORKER_LISTEN"), "0.0.0.0:50092"), "worker control listen address")
	fs.StringVar(&cfg.advertiseHost, "advertise-host", env("TGSRL_POD_IP"), "worker control address advertised to the registry")
	fs.StringVar(&cfg.controlSocket, "control-socket", firstNonEmpty(env("TGSRL_WORKER_CONTROL_SOCKET"), env("TGSRL_VERL_CONTROL_SOCKET")), "cooperative worker Unix socket")
	fs.StringVar(&cfg.safePointFile, "safe-point-file", env("TGSRL_WORKER_SAFE_POINT_FILE"), "signal-mode safe-point marker")
	fs.StringVar(&cfg.readinessFile, "readiness-file", env("TGSRL_WORKER_READINESS_FILE"), "signal-mode readiness marker")
	fs.StringVar(&cfg.mpsPIDDirectory, "mps-pid-dir", env("TGSRL_NVIDIA_MPS_PID_DIR"), "optional directory for generation-fenced MPS server PID publication")
	fs.BoolVar(&cfg.verifyDevices, "verify-device-identities", envBool("TGSRL_VERIFY_DEVICE_IDENTITIES"), "verify allocated UUIDs with nvidia-smi")
	fs.StringVar(&cfg.deviceCommand, "device-command", firstNonEmpty(env("TGSRL_DEVICE_IDENTITY_COMMAND"), "nvidia-smi"), "device identity executable")
	fs.DurationVar(&cfg.registrationWait, "registration-timeout", 30*time.Second, "registration and control readiness timeout")
	fs.DurationVar(&cfg.controlTimeout, "control-timeout", 30*time.Second, "maximum duration of one cooperative worker request")
	fs.DurationVar(&cfg.shutdownWait, "shutdown-timeout", 30*time.Second, "time to wait before escalating a forwarded termination signal")
	if err := fs.Parse(argv[:separator]); err != nil {
		return config{}, err
	}
	if fs.NArg() != 0 || cfg.registryURL == "" || cfg.registryToken == "" || cfg.advertiseHost == "" || cfg.registrationWait <= 0 || cfg.controlTimeout <= 0 || cfg.shutdownWait <= 0 {
		return config{}, errors.New("bootstrap requires registry URL/token, advertise host, and positive timeouts")
	}
	cfg.command = append([]string(nil), argv[separator+1:]...)
	return cfg, nil
}

func runWorker(cfg config) error {
	if cfg.shutdownWait <= 0 {
		cfg.shutdownWait = 30 * time.Second
	}
	if cfg.controlTimeout <= 0 {
		cfg.controlTimeout = 30 * time.Second
	}
	worker, err := workerFromEnvironment()
	if err != nil {
		return err
	}
	if cfg.verifyDevices {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.registrationWait)
		err = verifyDeviceIdentities(ctx, cfg.deviceCommand, worker.AllDeviceIDs())
		cancel()
		if err != nil {
			return err
		}
	}
	command := exec.Command(cfg.command[0], cfg.command[1:]...)
	command.Env = workloadEnvironment()
	command.Dir = strings.TrimSpace(os.Getenv("TGSRL_WORKING_DIRECTORY"))
	command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		return fmt.Errorf("start workload: %w", err)
	}
	waiter := &processWait{done: make(chan struct{})}
	go func() {
		waiter.err = command.Wait()
		close(waiter.done)
	}()
	worker.PID = command.Process.Pid
	worker.ProcessToken, err = runtimehelper.ProcessToken(context.Background(), worker.PID)
	if err != nil {
		terminateProcess(waiter, worker.PID, syscall.SIGKILL, cfg.shutdownWait)
		return err
	}
	worker.InstanceID = env("TGSRL_POD_UID")
	if worker.InstanceID == "" {
		worker.InstanceID, err = randomToken(16)
		if err != nil {
			terminateProcess(waiter, worker.PID, syscall.SIGKILL, cfg.shutdownWait)
			return err
		}
	}
	controlToken, err := randomToken(32)
	if err != nil {
		terminateProcess(waiter, worker.PID, syscall.SIGKILL, cfg.shutdownWait)
		return err
	}
	worker.ControlToken = controlToken
	worker.State, worker.Ready = "running", true
	if cfg.mpsPIDDirectory != "" {
		worker.MPSServerPID, err = discoverMPSServerPID()
		if err != nil {
			terminateProcess(waiter, worker.PID, syscall.SIGKILL, cfg.shutdownWait)
			return err
		}
		worker.MPSServerProcessToken, err = runtimehelper.ProcessToken(context.Background(), int(worker.MPSServerPID))
		if err != nil {
			terminateProcess(waiter, worker.PID, syscall.SIGKILL, cfg.shutdownWait)
			return err
		}
	}
	supervisor := &supervisor{worker: worker, controlToken: controlToken, controlSocket: cfg.controlSocket, safePointFile: cfg.safePointFile, readinessFile: cfg.readinessFile, controlTimeout: cfg.controlTimeout, receipts: make(map[string]receipt)}
	listener, err := net.Listen("tcp", cfg.listenAddress)
	if err != nil {
		terminateProcess(waiter, worker.PID, syscall.SIGKILL, cfg.shutdownWait)
		return err
	}
	defer listener.Close()
	worker.ControlURL = "http://" + net.JoinHostPort(cfg.advertiseHost, strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)) + "/v1/control"
	supervisor.worker.ControlURL = worker.ControlURL
	server := &http.Server{Handler: supervisor, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	go func() { _ = server.Serve(listener) }()
	if worker.MPSServerPID > 0 {
		if err := writeMPSPID(cfg.mpsPIDDirectory, worker.SandboxID, worker.Generation, worker.MPSServerPID, worker.MPSServerProcessToken); err != nil {
			_ = server.Close()
			terminateProcess(waiter, worker.PID, syscall.SIGKILL, cfg.shutdownWait)
			return err
		}
	}
	registrationCtx, cancel := context.WithTimeout(context.Background(), cfg.registrationWait)
	err = waitForWorkerReady(registrationCtx, supervisor, waiter)
	if err == nil {
		err = registerWithRetry(registrationCtx, cfg.registryURL, cfg.registryToken, supervisor.snapshot(), waiter)
	}
	cancel()
	if err != nil {
		_ = server.Close()
		terminateProcess(waiter, worker.PID, syscall.SIGKILL, cfg.shutdownWait)
		if worker.MPSServerPID > 0 {
			_ = cleanupMPSPID(cfg.mpsPIDDirectory, worker.SandboxID, worker.Generation, worker.MPSServerPID, worker.MPSServerProcessToken)
		}
		terminal := supervisor.snapshot()
		terminal.State, terminal.Ready, terminal.ExitCode, terminal.Detail = "failed", false, -1, "worker registration failed"
		reportCtx, reportCancel := context.WithTimeout(context.Background(), time.Second)
		_ = runtimehelper.ReportRemoteWorkerExit(reportCtx, cfg.registryURL, cfg.registryToken, terminal)
		reportCancel()
		return fmt.Errorf("register workload: %w", err)
	}
	supervisor.mu.Lock()
	supervisor.registered = true
	supervisor.mu.Unlock()

	signals := cfg.signals
	var ownedSignals chan os.Signal
	if signals == nil {
		ownedSignals = make(chan os.Signal, 2)
		signal.Notify(ownedSignals, syscall.SIGTERM, syscall.SIGINT)
		defer signal.Stop(ownedSignals)
		signals = ownedSignals
	}
	var waitErr error
	terminatedByBootstrap := false
	select {
	case <-waiter.done:
		waitErr = waiter.err
	case received := <-signals:
		terminatedByBootstrap = true
		_ = signalGroup(worker.PID, received.(syscall.Signal))
		select {
		case <-waiter.done:
		case <-time.After(cfg.shutdownWait):
			_ = signalGroup(worker.PID, syscall.SIGKILL)
			<-waiter.done
		}
		waitErr = waiter.err
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.shutdownWait)
	if server.Shutdown(shutdownCtx) != nil {
		_ = server.Close()
	}
	shutdownCancel()
	if worker.MPSServerPID > 0 {
		_ = cleanupMPSPID(cfg.mpsPIDDirectory, worker.SandboxID, worker.Generation, worker.MPSServerPID, worker.MPSServerProcessToken)
	}
	exitCode := command.ProcessState.ExitCode()
	state := "terminated"
	detail := "workload completed"
	current := supervisor.snapshot()
	if current.State == "terminated" {
		terminatedByBootstrap = true
	}
	if !terminatedByBootstrap && (waitErr != nil || exitCode != 0) {
		state, detail = "failed", firstNonEmpty(errorText(waitErr), "workload exited unsuccessfully")
	}
	supervisor.mu.Lock()
	supervisor.worker.State, supervisor.worker.Ready, supervisor.worker.ExitCode, supervisor.worker.Detail = state, false, exitCode, detail
	terminal := supervisor.worker
	supervisor.mu.Unlock()
	for attempt := 0; attempt < 5; attempt++ {
		reportCtx, reportCancel := context.WithTimeout(context.Background(), 5*time.Second)
		reportErr := runtimehelper.ReportRemoteWorkerExit(reportCtx, cfg.registryURL, cfg.registryToken, terminal)
		reportCancel()
		if reportErr == nil {
			break
		}
		if attempt == 4 {
			fmt.Fprintln(os.Stderr, "report workload exit:", reportErr)
		} else {
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
	}
	if terminatedByBootstrap {
		return nil
	}
	if waitErr != nil {
		return waitErr
	}
	if exitCode != 0 {
		return fmt.Errorf("workload exited with code %d", exitCode)
	}
	return nil
}

func workerFromEnvironment() (runtimehelper.Worker, error) {
	generation, err := strconv.ParseUint(env("TGSRL_GENERATION"), 10, 64)
	if err != nil || generation == 0 {
		return runtimehelper.Worker{}, errors.New("TGSRL_GENERATION must be positive")
	}
	share := 0.0
	if raw := env("TGSRL_ACCELERATOR_SHARE"); raw != "" {
		share, err = strconv.ParseFloat(raw, 64)
		if err != nil {
			return runtimehelper.Worker{}, errors.New("TGSRL_ACCELERATOR_SHARE must be numeric")
		}
	}
	worker := runtimehelper.Worker{RunID: env("TGSRL_RUN_ID"), JobID: env("TGSRL_JOB_ID"), TraceID: env("TGSRL_TRACE_ID"), RuntimeUnitID: env("TGSRL_RUNTIME_UNIT_ID"), SandboxID: env("TGSRL_SANDBOX_ID"), BindingID: env("TGSRL_BINDING_ID"), Generation: generation, DeviceIDs: splitCSV(env("TGSRL_DEVICE_IDS")), Share: share}
	if worker.RunID == "" || worker.JobID == "" || worker.RuntimeUnitID == "" || worker.SandboxID == "" || worker.BindingID == "" {
		return runtimehelper.Worker{}, errors.New("workload identity environment is incomplete")
	}
	if len(worker.DeviceIDs) == 1 {
		worker.DeviceID = worker.DeviceIDs[0]
	}
	return worker, nil
}

func (s *supervisor) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/readyz" {
		if request.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		worker := s.snapshot()
		status := s.handle(request.Context(), runtimehelper.ControlRequest{Action: "status", SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: "readiness"})
		s.mu.Lock()
		ready := s.registered && status.Accepted && status.Ready && status.State == "running"
		s.mu.Unlock()
		if !ready {
			http.Error(w, "worker is not registered and ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if request.URL.Path != "/v1/control" {
		http.NotFound(w, request)
		return
	}
	if request.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	provided, expected := []byte(request.Header.Get(runtimehelper.WorkerControlTokenHeader)), []byte(s.controlToken)
	if len(provided) != len(expected) || subtle.ConstantTimeCompare(provided, expected) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var control runtimehelper.ControlRequest
	decoder := json.NewDecoder(io.LimitReader(request.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&control); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	response := s.handle(request.Context(), control)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *supervisor) handle(ctx context.Context, request runtimehelper.ControlRequest) runtimehelper.ControlResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		timeout := s.controlTimeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	generationMatches := request.Generation == s.worker.Generation || request.Action == "reload" && request.Generation == s.worker.Generation+1
	if request.SandboxID != s.worker.SandboxID || !generationMatches {
		return s.response(false, "worker identity or generation mismatch")
	}
	if request.Action == "status" {
		if s.controlSocket != "" {
			response, err := callUnixControl(ctx, s.controlSocket, request)
			if err != nil {
				return s.response(false, err.Error())
			}
			if !response.Accepted || response.Generation != 0 && response.Generation != s.worker.Generation {
				return s.response(false, firstNonEmpty(response.Error, "cooperative worker identity mismatch"))
			}
			s.worker.State, s.worker.SafePoint, s.worker.Offloaded, s.worker.Ready = response.State, response.SafePoint, response.Offloaded, response.Ready
			s.worker.CheckpointRef = response.CheckpointRef
		} else {
			if token, err := runtimehelper.ProcessToken(ctx, s.worker.PID); err != nil || token != s.worker.ProcessToken {
				return s.response(false, "worker process identity changed")
			}
			s.worker.SafePoint, _ = readMarker(s.safePointFile)
			if s.readinessFile == "" {
				s.worker.Ready = true
			} else {
				s.worker.Ready, _ = readMarker(s.readinessFile)
			}
		}
		return s.response(true, "")
	}
	if request.IdempotencyKey == "" {
		return s.response(false, "idempotency_key is required")
	}
	digestBytes, _ := json.Marshal(request)
	digest := fmt.Sprintf("%x", sha256.Sum256(digestBytes))
	if previous, exists := s.receipts[request.IdempotencyKey]; exists {
		if previous.digest != digest {
			return s.response(false, "idempotency key was reused with different content")
		}
		return previous.response
	}
	response := s.apply(ctx, request)
	if response.Accepted || !strings.Contains(response.Error, "outcome is not confirmed") {
		s.receipts[request.IdempotencyKey] = receipt{digest: digest, response: response}
	}
	return response
}

func (s *supervisor) apply(ctx context.Context, request runtimehelper.ControlRequest) runtimehelper.ControlResponse {
	if s.controlSocket != "" {
		response, err := callUnixControl(ctx, s.controlSocket, request)
		if err != nil {
			return s.response(false, "worker mutation outcome is not confirmed: "+err.Error())
		}
		if response.Accepted {
			s.worker.State, s.worker.SafePoint, s.worker.Offloaded, s.worker.Ready = response.State, response.SafePoint, response.Offloaded, response.Ready
			s.worker.CheckpointRef = response.CheckpointRef
			if response.Generation != 0 {
				s.worker.Generation = response.Generation
			}
			if response.BindingID != "" {
				s.worker.BindingID = response.BindingID
			}
			if response.DeviceID != "" {
				s.worker.DeviceID, s.worker.DeviceIDs = response.DeviceID, []string{response.DeviceID}
			}
			if response.Share != 0 {
				s.worker.Share = response.Share
			}
		}
		identity := s.response(response.Accepted, response.Error)
		identity.State, identity.SafePoint, identity.Offloaded, identity.Ready, identity.CheckpointRef = response.State, response.SafePoint, response.Offloaded, response.Ready, response.CheckpointRef
		return identity
	}
	switch request.Action {
	case "prepare_pause":
		value, ok := readMarker(s.safePointFile)
		if !ok || !value {
			return s.response(false, "signal-managed worker is not at a safe point")
		}
		s.worker.SafePoint, s.worker.Ready = true, false
	case "pause", "sleep":
		if !s.worker.SafePoint {
			return s.response(false, request.Action+" requires a safe point")
		}
		if err := signalGroup(s.worker.PID, syscall.SIGSTOP); err != nil {
			return s.response(false, err.Error())
		}
		s.worker.State, s.worker.Ready = map[bool]string{true: "sleeping", false: "paused"}[request.Action == "sleep"], false
	case "resume":
		if err := signalGroup(s.worker.PID, syscall.SIGCONT); err != nil {
			return s.response(false, err.Error())
		}
		if s.readinessFile != "" {
			value, ok := readMarker(s.readinessFile)
			if !ok || !value {
				return s.response(false, "worker readiness is not confirmed")
			}
		}
		s.worker.State, s.worker.SafePoint, s.worker.Ready = "running", false, true
	case "stop":
		if !request.PreserveProcess {
			if err := signalGroup(s.worker.PID, syscall.SIGTERM); err != nil {
				return s.response(false, err.Error())
			}
		}
		s.worker.State, s.worker.Ready = "terminated", false
	default:
		return s.response(false, request.Action+" requires a cooperative worker control socket")
	}
	return s.response(true, "")
}

func (s *supervisor) response(accepted bool, detail string) runtimehelper.ControlResponse {
	deviceID := s.worker.DeviceID
	if deviceID == "" && len(s.worker.DeviceIDs) == 1 {
		deviceID = s.worker.DeviceIDs[0]
	}
	return runtimehelper.ControlResponse{Accepted: accepted, State: s.worker.State, Generation: s.worker.Generation, SafePoint: s.worker.SafePoint, Offloaded: s.worker.Offloaded, Ready: s.worker.Ready, CheckpointRef: s.worker.CheckpointRef, BindingID: s.worker.BindingID, DeviceID: deviceID, Share: s.worker.Share, InstanceID: s.worker.InstanceID, PID: s.worker.PID, ProcessToken: s.worker.ProcessToken, Error: detail}
}

func (s *supervisor) snapshot() runtimehelper.Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	worker := s.worker
	worker.DeviceIDs = append([]string(nil), worker.DeviceIDs...)
	return worker
}

func callUnixControl(ctx context.Context, socketPath string, request runtimehelper.ControlRequest) (runtimehelper.ControlResponse, error) {
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	if err != nil {
		return runtimehelper.ControlResponse{}, err
	}
	defer connection.Close()
	if deadline, ok := ctx.Deadline(); ok {
		_ = connection.SetDeadline(deadline)
	}
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return runtimehelper.ControlResponse{}, err
	}
	var response runtimehelper.ControlResponse
	if err := json.NewDecoder(bufio.NewReader(io.LimitReader(connection, 64<<10))).Decode(&response); err != nil {
		return runtimehelper.ControlResponse{}, err
	}
	return response, nil
}

func verifyDeviceIdentities(ctx context.Context, command string, expected []string) error {
	if len(expected) == 0 {
		return errors.New("device identity verification requires expected device IDs")
	}
	output, err := exec.CommandContext(ctx, command, "-L").CombinedOutput()
	if err != nil {
		return fmt.Errorf("read visible NVIDIA devices: %w: %s", err, strings.TrimSpace(string(output)))
	}
	wantPrefix := "GPU-"
	if strings.HasPrefix(expected[0], "MIG-") {
		wantPrefix = "MIG-"
	}
	seen := make(map[string]bool)
	for _, match := range deviceUUIDPattern.FindAllString(string(output), -1) {
		if strings.HasPrefix(match, wantPrefix) {
			seen[match] = true
		}
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("visible %s device count %d does not match allocation count %d", strings.TrimSuffix(wantPrefix, "-"), len(seen), len(expected))
	}
	for _, deviceID := range expected {
		if !seen[deviceID] {
			return fmt.Errorf("allocated device %q is not visible to the worker", deviceID)
		}
	}
	return nil
}

func waitForWorkerReady(ctx context.Context, supervisor *supervisor, waiter *processWait) error {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-waiter.done:
			return fmt.Errorf("workload exited before registration: %s", firstNonEmpty(errorText(waiter.err), "clean exit"))
		default:
		}
		worker := supervisor.snapshot()
		token, err := runtimehelper.ProcessToken(ctx, worker.PID)
		if err != nil {
			return err
		}
		if token != worker.ProcessToken {
			return errors.New("worker process identity changed before registration")
		}
		if supervisor.controlSocket != "" {
			response := supervisor.handle(ctx, runtimehelper.ControlRequest{Action: "status", SandboxID: worker.SandboxID, Generation: worker.Generation, IdempotencyKey: "bootstrap-readiness"})
			if response.Accepted {
				return nil
			}
		} else {
			ready := true
			if supervisor.readinessFile != "" {
				var available bool
				ready, available = readMarker(supervisor.readinessFile)
				ready = available && ready
			}
			if ready {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func registerWithRetry(ctx context.Context, registryURL, token string, worker runtimehelper.Worker, waiter *processWait) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		select {
		case <-waiter.done:
			return fmt.Errorf("workload exited before registration: %s", firstNonEmpty(errorText(waiter.err), "clean exit"))
		default:
		}
		if err := runtimehelper.RegisterRemoteWorker(ctx, registryURL, token, worker); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("worker registration did not converge: %w: %v", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func terminateProcess(waiter *processWait, pid int, signal syscall.Signal, timeout time.Duration) {
	select {
	case <-waiter.done:
		return
	default:
	}
	_ = signalGroup(pid, signal)
	select {
	case <-waiter.done:
	case <-time.After(timeout):
		_ = signalGroup(pid, syscall.SIGKILL)
		select {
		case <-waiter.done:
		case <-time.After(timeout):
		}
	}
}

func workloadEnvironment() []string {
	result := make([]string, 0, len(os.Environ()))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "TGSRL_WORKER_REGISTRY_TOKEN" {
			continue
		}
		result = append(result, entry)
	}
	return result
}

func discoverMPSServerPID() (uint32, error) {
	values := map[uint32]bool{}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, fmt.Errorf("read process table for NVIDIA MPS server: %w", err)
	}
	for _, entry := range entries {
		pid, parseErr := strconv.ParseUint(entry.Name(), 10, 32)
		if parseErr != nil || pid == 0 {
			continue
		}
		command, readErr := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if readErr != nil {
			continue
		}
		executable, _, _ := strings.Cut(string(command), "\x00")
		if filepath.Base(strings.TrimSpace(executable)) == "nvidia-cuda-mps-server" {
			values[uint32(pid)] = true
		}
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("expected exactly one visible NVIDIA MPS server, found %d", len(values))
	}
	for pid := range values {
		return pid, nil
	}
	return 0, errors.New("NVIDIA MPS server PID is unavailable")
}

func writeMPSPID(directory, sandboxID string, generation uint64, pid uint32, processToken string) error {
	if filepath.Base(sandboxID) != sandboxID || generation == 0 || pid == 0 || strings.TrimSpace(processToken) == "" {
		return errors.New("MPS PID publication identity is invalid")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	target := filepath.Join(directory, sandboxID+".pid")
	temp, err := os.CreateTemp(directory, ".mps-pid-*")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tempPath)
		}
	}()
	payload, err := json.Marshal(mpsPIDRecord{Generation: generation, PID: pid, ProcessToken: processToken})
	if err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(append(payload, '\n')); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, target); err != nil {
		return err
	}
	ok = true
	return nil
}

func cleanupMPSPID(directory, sandboxID string, generation uint64, pid uint32, processToken string) error {
	target := filepath.Join(directory, sandboxID+".pid")
	data, err := os.ReadFile(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var record mpsPIDRecord
	if json.Unmarshal(data, &record) != nil || record != (mpsPIDRecord{Generation: generation, PID: pid, ProcessToken: processToken}) {
		return nil
	}
	return os.Remove(target)
}

func signalGroup(pid int, value syscall.Signal) error {
	if pid <= 0 {
		return errors.New("worker PID must be positive")
	}
	return syscall.Kill(-pid, value)
}

func readMarker(path string) (bool, bool) {
	if strings.TrimSpace(path) == "" {
		return false, false
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return false, false
	}
	value := strings.ToLower(strings.TrimSpace(string(data)))
	return value == "1" || value == "true" || value == "ready" || value == "yes", true
}

func randomToken(size int) (string, error) {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func splitCSV(value string) []string {
	var result []string
	seen := map[string]bool{}
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item != "" && !seen[item] {
			seen[item] = true
			result = append(result, item)
		}
	}
	sort.Strings(result)
	return result
}

func env(key string) string { return strings.TrimSpace(os.Getenv(key)) }

func envBool(key string) bool {
	switch strings.ToLower(env(key)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
