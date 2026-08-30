package nvidia

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	commandNvidiaMPSControl = "nvidia-cuda-mps-control"
	commandNvidiaMPSDaemon  = "nvidia-cuda-mps-server"
	defaultCommandTimeout   = 15 * time.Second
	defaultMPSPipeDirectory = "/tmp/nvidia-mps"
	defaultMPSLogDirectory  = "/tmp/nvidia-log"
)

// LocalDriverV2Options configures the composed NVIDIA mutation driver.
type LocalDriverV2Options struct {
	Executor              CommandExecutor
	Inventory             InventoryBackend
	Partition             PartitionBackend
	Runtime               RuntimeBackend
	Binding               BindingBackend
	PartitionMode         PartitionMode
	CommandTimeout        time.Duration
	DryRun                bool
	Audit                 AuditSink
	Now                   func() time.Time
	MPSPipeDirectory      string
	MPSLogDirectory       string
	EnableRuntimeCommands bool
	BindingHelperBinary   string
	BindingStatePath      string
	MPSPIDDirectory       string
	RuntimeHelperBinary   string
	RuntimeStatePath      string
	MIGHelperBinary       string
}

// LocalDriverV2 composes independent inventory, partition, runtime, and binding backends.
type LocalDriverV2 struct {
	inventory InventoryBackend
	partition PartitionBackend
	runtime   RuntimeBackend
	binding   BindingBackend
	timeout   time.Duration
	dryRun    bool
	audit     AuditSink
	now       func() time.Time

	mu                  sync.Mutex
	sequence            uint64
	lastInventory       *InventorySnapshot
	lastPartitions      *PartitionSnapshot
	lastBindings        *BindingSnapshot
	runtimeStatus       *RuntimeBackendStatus
	lastCapabilities    *tgsrlv1.CapabilitySet
	capabilityRevision  uint64
	executions          map[string]v2ExecutionRecord
	mutationLocks       map[string]*sync.Mutex
	observedGenerations map[string]uint64
	recoveredActions    map[string]RecoveredAction
}

type v2ExecutionRecord struct {
	action *tgsrlv1.Action
	result *ActionExecution
}

// V2Driver exposes discovery, dry-run, audit, and recovered helper receipts.
type V2Driver interface {
	Driver
	PlanAction(context.Context, *DriverState, *tgsrlv1.Action) (*ActionExecution, error)
	RollbackAction(context.Context, *DriverState, *tgsrlv1.Action) (*ActionExecution, error)
	Inventory() *InventorySnapshot
	Partitions() *PartitionSnapshot
	AuditRecords() []AuditRecord
	RecoveredActions() []RecoveredAction
}

var _ V2Driver = (*LocalDriverV2)(nil)

type noOpBindingBackend struct{}

func (noOpBindingBackend) Discover(context.Context, *InventorySnapshot, *PartitionSnapshot) (*BindingSnapshot, error) {
	return &BindingSnapshot{Available: true}, nil
}

func (noOpBindingBackend) Apply(_ context.Context, request BackendActionRequest) (*BackendActionResult, error) {
	return nil, v2ActionError(request.Action, ErrorCodeUnavailable, "nvidia binding backend is not configured", base.ErrFailedPrecondition)
}

// NewLocalDriverV2 constructs a mutation-capable driver. MPS is the default;
// MIG is used only when explicitly requested and never silently downgraded.
func NewLocalDriverV2(options LocalDriverV2Options) (*LocalDriverV2, error) {
	if options.Executor == nil {
		options.Executor = NewExecCommandExecutor()
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.CommandTimeout <= 0 {
		options.CommandTimeout = defaultCommandTimeout
	}
	if options.PartitionMode == "" {
		options.PartitionMode = PartitionModeMPS
	}
	if err := validatePartitionMode(options.PartitionMode); err != nil {
		return nil, err
	}
	if options.Inventory == nil {
		options.Inventory = NewCommandInventoryBackend(options.Executor, options.CommandTimeout, options.Now)
	}
	if options.Partition == nil {
		switch options.PartitionMode {
		case PartitionModeMPS:
			options.Partition = NewMPSBackend(options.Executor, options.CommandTimeout, options.MPSPipeDirectory, options.MPSLogDirectory)
		case PartitionModeMIG:
			options.Partition = NewMIGBackendWithConfig(options.Executor, options.CommandTimeout, options.MIGHelperBinary, options.RuntimeStatePath, options.BindingStatePath)
		}
	}
	if options.Partition == nil {
		return nil, fmt.Errorf("%w: partition backend is required", base.ErrInvalidArgument)
	}
	if options.Partition.Mode() != options.PartitionMode {
		return nil, fmt.Errorf("%w: partition backend mode %q does not match requested mode %q", base.ErrInvalidArgument, options.Partition.Mode(), options.PartitionMode)
	}
	if dryRunAware, ok := options.Partition.(PartitionDryRunConfig); ok {
		dryRunAware.SetDryRun(options.DryRun)
	}
	if options.Runtime == nil {
		if options.EnableRuntimeCommands || options.RuntimeHelperBinary != "" || options.RuntimeStatePath != "" {
			options.Runtime = NewCommandRuntimeBackendWithConfig(options.Executor, options.CommandTimeout, options.RuntimeHelperBinary, options.RuntimeStatePath)
		} else {
			options.Runtime = NewUnavailableRuntimeBackend("nvidia runtime control backend is not configured")
		}
	}
	if options.Binding == nil {
		if options.BindingStatePath != "" || options.BindingHelperBinary != "" {
			options.Binding = NewCommandBindingBackendWithConfig(options.Executor, options.CommandTimeout, options.BindingHelperBinary, options.BindingStatePath, options.MPSPIDDirectory)
		} else {
			options.Binding = noOpBindingBackend{}
		}
	}
	if options.Audit == nil {
		options.Audit = NewInMemoryAuditSink(256)
	}
	return &LocalDriverV2{
		inventory: options.Inventory, partition: options.Partition, runtime: options.Runtime, binding: options.Binding,
		timeout: options.CommandTimeout, dryRun: options.DryRun, audit: options.Audit, now: options.Now,
		executions:          make(map[string]v2ExecutionRecord),
		mutationLocks:       make(map[string]*sync.Mutex),
		observedGenerations: make(map[string]uint64),
		recoveredActions:    make(map[string]RecoveredAction),
	}, nil
}

// ID identifies the composed local v2 implementation.
func (*LocalDriverV2) ID() string { return "local-v2" }

// Probe performs authoritative restart discovery and publishes stable UUID identities.
func (d *LocalDriverV2) Probe(ctx context.Context) (*ProbeResult, error) {
	ctx, cancel := d.withTimeout(ctx)
	defer cancel()
	auditContext := context.WithoutCancel(ctx)
	inventory, err := d.inventory.Discover(ctx)
	if err != nil {
		reason := discoveryFailureReason(err)
		_ = d.recordAudit(auditContext, AuditRecord{Operation: "discover", Status: AuditStatusFailed, ErrorCode: ErrorCodeUnavailable, ErrorMessage: reason})
		return &ProbeResult{Available: false, Reason: reason, Capabilities: unavailableCapabilities(reason)}, nil
	}
	if inventory == nil || len(inventory.Devices) == 0 {
		reason := "no usable nvidia devices discovered"
		_ = d.recordAudit(auditContext, AuditRecord{Operation: "discover", Status: AuditStatusFailed, ErrorCode: ErrorCodeUnavailable, ErrorMessage: reason})
		return &ProbeResult{Available: false, Reason: reason, Capabilities: unavailableCapabilities(reason)}, nil
	}
	partitions, err := d.partition.Discover(ctx, inventory)
	if err != nil || partitions == nil || !partitions.Available {
		reason := partitionUnavailableReason(d.partition.Mode(), partitions, err)
		d.rememberDiscovery(inventory, partitions)
		_ = d.recordAudit(auditContext, AuditRecord{Operation: "discover", PartitionMode: d.partition.Mode(), Status: AuditStatusFailed, ErrorCode: ErrorCodeUnavailable, ErrorMessage: reason})
		return &ProbeResult{Available: false, Reason: reason, Capabilities: v2UnavailableCapabilities(reason, d.partition.Mode(), inventory)}, nil
	}
	bindingSnapshot, err := d.binding.Discover(ctx, inventory, partitions)
	if err != nil {
		inventory.Warnings = append(inventory.Warnings, "binding discovery unavailable: "+boundedDiagnostic([]byte(err.Error())))
		bindingSnapshot = &BindingSnapshot{}
	}
	if bindingSnapshot == nil || !bindingSnapshot.Available {
		reason := "nvidia binding backend is unavailable"
		if bindingSnapshot != nil && strings.TrimSpace(bindingSnapshot.Reason) != "" {
			reason = strings.TrimSpace(bindingSnapshot.Reason)
		}
		inventory.Warnings = append(inventory.Warnings, reason)
		bindingSnapshot = &BindingSnapshot{}
	}
	if partitions.RequiresBindingMetadata && !bindingSnapshot.SupportsMPSProfiles {
		partitions.SupportedActions = nil
		inventory.Warnings = append(inventory.Warnings, "MPS profile actions unavailable: binding backend cannot discover MPS server profiles")
	}
	bindings := cloneBindings(bindingSnapshot.Bindings)
	if err := validateDiscoveredBindings(bindings, inventory, partitions); err != nil {
		reason := "invalid nvidia binding discovery: " + err.Error()
		d.rememberDiscovery(inventory, partitions)
		_ = d.recordAudit(auditContext, AuditRecord{Operation: "discover", PartitionMode: d.partition.Mode(), Status: AuditStatusFailed, ErrorCode: ErrorCodeUnavailable, ErrorMessage: reason})
		return &ProbeResult{Available: false, Reason: reason, Capabilities: v2UnavailableCapabilities(reason, d.partition.Mode(), inventory)}, nil
	}
	recoveredReceipts := make([]RecoveredAction, 0)
	if receiptBackend, ok := d.binding.(ReceiptBackend); ok {
		receipts, receiptErr := receiptBackend.DiscoverReceipts(ctx)
		if receiptErr != nil {
			inventory.Warnings = append(inventory.Warnings, "action receipt discovery unavailable: "+boundedDiagnostic([]byte(receiptErr.Error())))
		} else {
			recoveredReceipts = append(recoveredReceipts, receipts...)
		}
	}
	runtimeStatus, err := d.runtime.Discover(ctx)
	if err != nil {
		inventory.Warnings = append(inventory.Warnings, "runtime discovery unavailable: "+boundedDiagnostic([]byte(err.Error())))
		runtimeStatus = &RuntimeBackendStatus{}
	}
	var runtimeActions []tgsrlv1.ActionType
	if runtimeStatus != nil && runtimeStatus.Available {
		runtimeActions = append(runtimeActions, runtimeStatus.SupportedActions...)
	}
	if receiptBackend, ok := d.runtime.(ReceiptBackend); ok {
		receipts, receiptErr := receiptBackend.DiscoverReceipts(ctx)
		if receiptErr != nil {
			inventory.Warnings = append(inventory.Warnings, "runtime receipt discovery unavailable: "+boundedDiagnostic([]byte(receiptErr.Error())))
		} else {
			recoveredReceipts = append(recoveredReceipts, receipts...)
		}
	}
	if receiptBackend, ok := d.partition.(ReceiptBackend); ok {
		receipts, receiptErr := receiptBackend.DiscoverReceipts(ctx)
		if receiptErr != nil {
			inventory.Warnings = append(inventory.Warnings, "partition receipt discovery unavailable: "+boundedDiagnostic([]byte(receiptErr.Error())))
		} else {
			recoveredReceipts = append(recoveredReceipts, receipts...)
		}
	}
	if err := d.rememberReceipts(recoveredReceipts); err != nil {
		inventory.Warnings = append(inventory.Warnings, "action receipt discovery rejected: "+err.Error())
		d.rememberReceipts(nil) //nolint:errcheck
	}
	d.rememberBackends(bindingSnapshot, runtimeStatus)
	capabilities := d.versionCapabilities(v2Capabilities(inventory, partitions, bindingSnapshot.SupportedActions, runtimeActions, d.dryRun))
	devices := inventoryDevices(inventory, partitions, capabilities)
	d.rememberDiscovery(inventory, partitions)
	if err := d.recordAudit(auditContext, AuditRecord{Operation: "discover", PartitionMode: d.partition.Mode(), Status: AuditStatusSucceeded}); err != nil {
		reason := "nvidia audit write failed: " + err.Error()
		return &ProbeResult{Available: false, Reason: reason, Capabilities: v2UnavailableCapabilities(reason, d.partition.Mode(), inventory)}, nil
	}
	sandboxes := discoveredSandboxes(bindings, d.now())
	if runtimeStatus != nil && runtimeStatus.SandboxesAuthoritative {
		sandboxes = mergeRuntimeDiscovery(runtimeStatus.Sandboxes, sandboxes, d.now())
	}
	return &ProbeResult{Available: true, Devices: devices, Capabilities: capabilities, Sandboxes: sandboxes, SandboxesAuthoritative: runtimeStatus != nil && runtimeStatus.SandboxesAuthoritative}, nil
}

// ExecuteAction dispatches an action to exactly one backend and records a sanitized audit.
func (d *LocalDriverV2) ExecuteAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	return d.executeAction(ctx, state, action, d.dryRun, true, true, nil)
}

func (d *LocalDriverV2) executeAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action, dryRun, cache, checkGeneration bool, receipt *HelperReceiptContext) (*ActionExecution, error) {
	if err := validateDriverAction(action); err != nil {
		return nil, err
	}
	mutationLocks := d.mutationLocksFor(state, action)
	for _, lock := range mutationLocks {
		lock.Lock()
	}
	defer func() {
		for index := len(mutationLocks) - 1; index >= 0; index-- {
			mutationLocks[index].Unlock()
		}
	}()
	key := strings.TrimSpace(action.GetIdempotencyKey())
	if cache && key != "" {
		d.mu.Lock()
		record, exists := d.executions[key]
		recovered, recoveredExists := d.recoveredActions[key]
		d.mu.Unlock()
		if exists {
			if !proto.Equal(record.action, action) {
				return nil, v2ActionError(action, base.ErrorCodeIdempotencyConflict, "idempotency key was reused with different NVIDIA action content", base.ErrIdempotencyConflict)
			}
			return cloneActionExecution(record.result), nil
		}
		if recoveredExists {
			digest, digestErr := actionDigest(action)
			if digestErr != nil {
				return nil, digestErr
			}
			actionGeneration := recovered.ActionGeneration
			if actionGeneration == 0 {
				actionGeneration = recovered.Generation
			}
			if recovered.ActionID != action.GetActionId() || recovered.PlanID != action.GetPlanId() || recovered.SandboxID != actionSandboxID(action) || actionGeneration != receiptActionGeneration(action) || recovered.CommandDigest == "" || recovered.CommandDigest != digest {
				return nil, v2ActionError(action, base.ErrorCodeIdempotencyConflict, "recovered idempotency receipt does not match NVIDIA action identity", base.ErrIdempotencyConflict)
			}
			if recovered.Succeeded {
				return &ActionExecution{Detail: "action recovered from durable NVIDIA helper receipt"}, nil
			}
			return nil, v2ActionError(action, firstNonEmpty(recovered.ErrorCode, base.ErrorCodeFailedPrecondition), firstNonEmpty(recovered.ErrorMessage, "recovered NVIDIA action failed"), base.ErrFailedPrecondition)
		}
	}
	if checkGeneration && action.GetExpectedGeneration() != 0 && state != nil {
		sandbox := state.Sandboxes[actionSandboxID(action)]
		if sandbox.SandboxID != "" && sandbox.Generation != action.GetExpectedGeneration() {
			return nil, v2ActionError(action, base.ErrorCodeGenerationConflict, "nvidia backend generation fence failed", base.ErrFailedPrecondition)
		}
	}
	if checkGeneration && action.GetExpectedSnapshotRevision() != 0 && state != nil && state.Revision != 0 && state.Revision != action.GetExpectedSnapshotRevision() {
		return nil, v2ActionError(action, base.ErrorCodeRevisionConflict, "nvidia backend provider revision fence failed", base.ErrFailedPrecondition)
	}
	if checkGeneration && action.GetExpectedGeneration() != 0 {
		d.mu.Lock()
		observedGeneration := d.observedGenerations[actionSandboxID(action)]
		d.mu.Unlock()
		if observedGeneration != 0 && observedGeneration != action.GetExpectedGeneration() {
			return nil, v2ActionError(action, base.ErrorCodeGenerationConflict, "nvidia backend generation changed before command dispatch", base.ErrFailedPrecondition)
		}
	}
	request := BackendActionRequest{State: cloneDriverState(state), Action: cloneAction(action), Partitions: d.partitions(), Binding: d.bindingFor(actionSandboxID(action)), DryRun: dryRun, Receipt: cloneHelperReceiptContext(receipt)}
	if !d.supportsAction(action.GetActionType()) {
		return nil, v2ActionError(action, ErrorCodeUnavailable, "nvidia backend did not advertise the requested mutation", base.ErrFailedPrecondition)
	}
	backend, err := d.backendFor(action.GetActionType())
	if err != nil {
		return nil, v2ActionError(action, base.ErrorCodeUnsupported, err.Error(), base.ErrUnsupported)
	}
	if !dryRun {
		preparedAudit := auditForBackendResult(d.nextSequence(), d.now(), d.partition.Mode(), false, action, nil, nil)
		preparedAudit.Status = AuditStatusPlanned
		if auditErr := d.recordAudit(ctx, preparedAudit); auditErr != nil {
			return nil, v2ActionError(action, base.ErrorCodeFailedPrecondition, "nvidia audit intent write failed: "+auditErr.Error(), base.ErrFailedPrecondition)
		}
	}
	executionContext, cancel := d.withTimeout(ctx)
	defer cancel()
	result, err := backend.Apply(executionContext, request)
	if err == nil && result == nil {
		err = fmt.Errorf("%w: nvidia backend returned no action result", base.ErrFailedPrecondition)
	}
	audit := auditForBackendResult(d.nextSequence(), d.now(), d.partition.Mode(), dryRun, action, result, err)
	if auditErr := d.recordAudit(context.WithoutCancel(executionContext), audit); auditErr != nil {
		return nil, v2ActionError(action, base.ErrorCodeFailedPrecondition, "nvidia audit write failed: "+auditErr.Error(), base.ErrFailedPrecondition)
	}
	if err != nil {
		if result == nil {
			return nil, normalizeV2BackendError(action, err)
		}
		execution := &ActionExecution{Detail: result.Detail, Commands: cloneCommands(result.Commands), DryRun: dryRun, Binding: cloneDiscoveredBinding(result.Binding), ObservedShare: cloneFloat64(result.ObservedShare), MutationMayHaveApplied: result.MutationMayHaveApplied}
		return execution, normalizeV2BackendError(action, err)
	}
	if !dryRun && (action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE || action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_RESIZE) && result.ObservedShare == nil {
		return nil, v2ActionError(action, ErrorCodeUnavailable, "NVIDIA share mutation returned no authoritative readback", base.ErrFailedPrecondition)
	}
	if d.partition.Mode() == PartitionModeMIG && (action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_REBIND || action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_RECREATE) {
		if err := d.refreshAfterMIGMutation(executionContext, action, result); err != nil {
			return nil, err
		}
	}
	if !dryRun && result.Binding != nil {
		d.rememberBindingMutation(action.GetActionType(), result.Binding)
	}
	execution := &ActionExecution{Detail: result.Detail, Commands: cloneCommands(result.Commands), DryRun: dryRun, Binding: cloneDiscoveredBinding(result.Binding), ObservedShare: cloneFloat64(result.ObservedShare), MutationMayHaveApplied: result.MutationMayHaveApplied}
	if cache && key != "" {
		d.mu.Lock()
		d.executions[key] = v2ExecutionRecord{action: cloneAction(action), result: cloneActionExecution(execution)}
		d.observedGenerations[actionSandboxID(action)] = resultingGeneration(action)
		d.mu.Unlock()
	}
	return execution, nil
}

func (d *LocalDriverV2) rememberBindingMutation(actionType tgsrlv1.ActionType, binding *DiscoveredBinding) {
	if binding == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastBindings == nil {
		d.lastBindings = &BindingSnapshot{Available: true}
	}
	bindings := d.lastBindings.Bindings[:0]
	for _, current := range d.lastBindings.Bindings {
		if current.SandboxID != binding.SandboxID {
			bindings = append(bindings, current)
		}
	}
	if actionType != tgsrlv1.ActionType_ACTION_TYPE_RELEASE {
		bindings = append(bindings, *cloneDiscoveredBinding(binding))
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].SandboxID < bindings[j].SandboxID })
	d.lastBindings.Bindings = bindings
	d.observedGenerations[binding.SandboxID] = binding.Generation
}

func actionDigest(action *tgsrlv1.Action) (string, error) {
	if action == nil {
		return "", fmt.Errorf("%w: action is required", base.ErrInvalidArgument)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(action)
	if err != nil {
		return "", fmt.Errorf("marshal NVIDIA action digest: %w", err)
	}
	digest := sha256.Sum256(wire)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func (d *LocalDriverV2) refreshAfterMIGMutation(ctx context.Context, action *tgsrlv1.Action, result *BackendActionResult) error {
	d.mu.Lock()
	inventory := cloneInventory(d.lastInventory)
	previousPartitions := clonePartitions(d.lastPartitions)
	d.mu.Unlock()
	if inventory == nil {
		return v2ActionError(action, ErrorCodeUnavailable, "MIG inventory is unavailable after mutation", base.ErrFailedPrecondition)
	}
	partitions, err := d.partition.Discover(ctx, inventory)
	if err != nil || partitions == nil || !partitions.Available {
		return v2ActionError(action, ErrorCodeUnavailable, partitionUnavailableReason(PartitionModeMIG, partitions, err), base.ErrFailedPrecondition)
	}
	requested := action.GetBinding().GetResources()
	target, err := migTargetPartition(previousPartitions, action)
	if err != nil {
		return err
	}
	if result.Binding == nil || len(result.Binding.DeviceIDs) != 1 {
		return v2ActionError(action, ErrorCodeUnavailable, "MIG helper returned no authoritative replacement UUID", base.ErrFailedPrecondition)
	}
	receiptDeviceID := result.Binding.DeviceIDs[0]
	var candidates []Partition
	for _, partition := range partitions.Partitions {
		if partition.ID != receiptDeviceID || partition.ParentUUID != target.ParentUUID {
			continue
		}
		if target.Profile != "" && partition.Profile != target.Profile {
			continue
		}
		if requested != nil && requested.GetMemoryBytes() > 0 && partition.MemoryBytes < requested.GetMemoryBytes() {
			continue
		}
		candidates = append(candidates, partition)
	}
	if len(candidates) == 0 {
		return v2ActionError(action, ErrorCodeUnavailable, "MIG mutation completed but no matching partition was rediscovered", base.ErrFailedPrecondition)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].ID < candidates[j].ID })
	result.Binding.DeviceIDs = []string{candidates[0].ID}
	d.rememberDiscovery(inventory, partitions)
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (d *LocalDriverV2) mutationLocksFor(state *DriverState, action *tgsrlv1.Action) []*sync.Mutex {
	keys := map[string]struct{}{"sandbox:" + actionSandboxID(action): {}}
	deviceIDs := append([]string(nil), action.GetBinding().GetDeviceIds()...)
	if state != nil {
		deviceIDs = append(deviceIDs, state.Sandboxes[actionSandboxID(action)].Binding.GetDeviceIds()...)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, deviceID := range deviceIDs {
		parentID := deviceID
		if d.lastPartitions != nil {
			for _, partition := range d.lastPartitions.Partitions {
				if partition.ID == deviceID && partition.ParentUUID != "" {
					parentID = partition.ParentUUID
					break
				}
			}
		}
		if parentID != "" {
			keys["gpu:"+parentID] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	locks := make([]*sync.Mutex, 0, len(ordered))
	for _, key := range ordered {
		lock := d.mutationLocks[key]
		if lock == nil {
			lock = &sync.Mutex{}
			d.mutationLocks[key] = lock
		}
		locks = append(locks, lock)
	}
	return locks
}

// Reconcile rediscovers hardware, partitions, capabilities, and bindings.
func (d *LocalDriverV2) Reconcile(ctx context.Context, state *DriverState, _ *tgsrlv1.PlacementPlan) (*ReconcileState, error) {
	probe, err := d.Probe(ctx)
	if err != nil {
		return nil, err
	}
	devices := cloneDevices(probe.Devices)
	if !probe.Available {
		devices = unavailableDevices(probe.Reason)
	}
	d.mu.Lock()
	runtimeAuthoritative := d.runtimeStatus != nil && d.runtimeStatus.SandboxesAuthoritative
	d.mu.Unlock()
	sandboxes := cloneSandboxSlice(probe.Sandboxes)
	if !runtimeAuthoritative {
		sandboxes = mergeDiscoveredBindings(state, probe.Sandboxes, d.now())
	}
	return &ReconcileState{
		Healthy: probe.Available, Reason: probe.Reason, Devices: devices, Capabilities: cloneCapabilities(probe.Capabilities),
		Sandboxes: sandboxes, SandboxesAuthoritative: runtimeAuthoritative,
	}, nil
}

// PlanAction validates and returns the exact argv-only commands without side effects.
func (d *LocalDriverV2) PlanAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	return d.executeAction(ctx, state, action, true, false, true, nil)
}

// Inventory returns the last detached inventory snapshot.
func (d *LocalDriverV2) Inventory() *InventorySnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	return cloneInventory(d.lastInventory)
}

// Partitions returns the last detached partition snapshot.
func (d *LocalDriverV2) Partitions() *PartitionSnapshot {
	return d.partitions()
}

// AuditRecords returns records when the configured sink supports snapshots.
func (d *LocalDriverV2) AuditRecords() []AuditRecord {
	snapshotter, ok := d.audit.(interface{ Records() []AuditRecord })
	if !ok {
		return nil
	}
	return snapshotter.Records()
}

// RecoveredActions returns detached durable helper receipts.
func (d *LocalDriverV2) RecoveredActions() []RecoveredAction {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]RecoveredAction, 0, len(d.recoveredActions))
	for _, receipt := range d.recoveredActions {
		result = append(result, receipt)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].PlanID != result[j].PlanID {
			return result[i].PlanID < result[j].PlanID
		}
		return result[i].ActionID < result[j].ActionID
	})
	return result
}

// RollbackAction executes provider-authorized compensation against its captured
// before-image without applying the forward action's generation fence again.
func (d *LocalDriverV2) RollbackAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action) (*ActionExecution, error) {
	return d.executeAction(ctx, state, action, false, true, false, nil)
}

func (d *LocalDriverV2) executeTransactionAction(ctx context.Context, state *DriverState, action *tgsrlv1.Action, receipt *HelperReceiptContext) (*ActionExecution, error) {
	return d.executeAction(ctx, state, action, d.dryRun, true, true, receipt)
}

type actionBackend interface {
	Apply(context.Context, BackendActionRequest) (*BackendActionResult, error)
}

func (d *LocalDriverV2) backendFor(actionType tgsrlv1.ActionType) (actionBackend, error) {
	switch actionType {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		return d.binding, nil
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionType_ACTION_TYPE_RESIZE, tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		return d.partition, nil
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionType_ACTION_TYPE_RESUME, tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		return d.runtime, nil
	default:
		return nil, fmt.Errorf("unsupported nvidia action %s", actionType)
	}
}

func (d *LocalDriverV2) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if d.timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d.timeout)
}

func (d *LocalDriverV2) rememberDiscovery(inventory *InventorySnapshot, partitions *PartitionSnapshot) {
	d.mu.Lock()
	d.lastInventory = cloneInventory(inventory)
	d.lastPartitions = clonePartitions(partitions)
	d.mu.Unlock()
}

func (d *LocalDriverV2) versionCapabilities(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.capabilityRevision == 0 {
		d.capabilityRevision = 1
	} else if !equalCapabilityContent(d.lastCapabilities, capabilities) {
		d.capabilityRevision++
	}
	capabilities.Revision = d.capabilityRevision
	if d.lastCapabilities != nil && equalCapabilityContent(d.lastCapabilities, capabilities) {
		capabilities.MeasuredAt = d.lastCapabilities.GetMeasuredAt()
	}
	for _, component := range capabilities.ComponentVersions {
		component.Revision = d.capabilityRevision
	}
	for _, evidence := range capabilities.Evidence {
		evidence.Revision = d.capabilityRevision
	}
	d.lastCapabilities = cloneCapabilities(capabilities)
	return capabilities
}

func (d *LocalDriverV2) rememberBackends(bindings *BindingSnapshot, runtimeStatus *RuntimeBackendStatus) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if bindings != nil {
		copy := *bindings
		copy.Bindings = cloneBindings(bindings.Bindings)
		copy.SupportedActions = append([]tgsrlv1.ActionType(nil), bindings.SupportedActions...)
		d.lastBindings = &copy
		for _, binding := range bindings.Bindings {
			d.observedGenerations[binding.SandboxID] = binding.Generation
		}
		if mps, ok := d.partition.(*MPSBackend); ok {
			mps.restoreProfiles(bindings.Bindings, d.now())
		}
	}
	if runtimeStatus != nil {
		copy := *runtimeStatus
		copy.SupportedActions = append([]tgsrlv1.ActionType(nil), runtimeStatus.SupportedActions...)
		copy.Sandboxes = cloneSandboxSlice(runtimeStatus.Sandboxes)
		d.runtimeStatus = &copy
	}
}

func (d *LocalDriverV2) rememberReceipts(receipts []RecoveredAction) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	next := make(map[string]RecoveredAction, len(receipts))
	for _, receipt := range receipts {
		if receipt.IdempotencyKey != "" {
			if _, exists := next[receipt.IdempotencyKey]; exists {
				return fmt.Errorf("duplicate recovered idempotency key %q", receipt.IdempotencyKey)
			}
			next[receipt.IdempotencyKey] = receipt
		}
	}
	d.recoveredActions = next
	return nil
}

func (d *LocalDriverV2) bindingFor(sandboxID string) *DiscoveredBinding {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.lastBindings == nil {
		return nil
	}
	for index := range d.lastBindings.Bindings {
		if d.lastBindings.Bindings[index].SandboxID == sandboxID {
			return cloneDiscoveredBinding(&d.lastBindings.Bindings[index])
		}
	}
	return nil
}

func resultingGeneration(action *tgsrlv1.Action) uint64 {
	if action == nil {
		return 0
	}
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		if generation := action.GetBinding().GetGeneration(); generation != 0 {
			return generation
		}
		return action.GetExpectedGeneration() + 1
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		if generation := action.GetBinding().GetGeneration(); generation != 0 {
			return generation
		}
	}
	return action.GetExpectedGeneration()
}

func receiptActionGeneration(action *tgsrlv1.Action) uint64 {
	if action != nil && action.GetActionType() == tgsrlv1.ActionType_ACTION_TYPE_BIND && action.GetBinding().GetGeneration() != 0 {
		return action.GetBinding().GetGeneration()
	}
	return action.GetExpectedGeneration()
}

func (d *LocalDriverV2) supportsAction(actionType tgsrlv1.ActionType) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch actionType {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND, tgsrlv1.ActionType_ACTION_TYPE_RELEASE:
		return d.lastBindings != nil && containsActionType(d.lastBindings.SupportedActions, actionType)
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE, tgsrlv1.ActionType_ACTION_TYPE_RESIZE, tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		return d.lastPartitions != nil && containsActionType(d.lastPartitions.SupportedActions, actionType)
	case tgsrlv1.ActionType_ACTION_TYPE_PAUSE, tgsrlv1.ActionType_ACTION_TYPE_RESUME, tgsrlv1.ActionType_ACTION_TYPE_SLEEP, tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		return d.runtimeStatus != nil && containsActionType(d.runtimeStatus.SupportedActions, actionType)
	default:
		return false
	}
}

func (d *LocalDriverV2) partitions() *PartitionSnapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	return clonePartitions(d.lastPartitions)
}

func (d *LocalDriverV2) nextSequence() uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sequence++
	return d.sequence
}

func (d *LocalDriverV2) recordAudit(ctx context.Context, record AuditRecord) error {
	if record.Sequence == 0 {
		record.Sequence = d.nextSequence()
	}
	if record.OccurredAt.IsZero() {
		record.OccurredAt = d.now()
	}
	return d.audit.Record(context.WithoutCancel(ctx), cloneAuditRecord(record))
}

func cloneDriverState(state *DriverState) *DriverState {
	if state == nil {
		return &DriverState{Sandboxes: map[string]base.Sandbox{}}
	}
	result := &DriverState{Healthy: state.Healthy, HealthReason: state.HealthReason, Revision: state.Revision, Devices: cloneDevices(state.Devices), Sandboxes: make(map[string]base.Sandbox, len(state.Sandboxes))}
	for sandboxID, sandbox := range state.Sandboxes {
		result.Sandboxes[sandboxID] = cloneSandbox(sandbox)
	}
	return result
}

func cloneActionExecution(execution *ActionExecution) *ActionExecution {
	if execution == nil {
		return nil
	}
	result := *execution
	result.Commands = cloneCommands(execution.Commands)
	result.Binding = cloneDiscoveredBinding(execution.Binding)
	result.ObservedShare = cloneFloat64(execution.ObservedShare)
	return &result
}

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneHelperReceiptContext(value *HelperReceiptContext) *HelperReceiptContext {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func partitionUnavailableReason(mode PartitionMode, snapshot *PartitionSnapshot, err error) string {
	if err != nil {
		return fmt.Sprintf("nvidia %s unavailable: %s", mode, boundedDiagnostic([]byte(err.Error())))
	}
	if snapshot != nil && strings.TrimSpace(snapshot.Reason) != "" {
		return fmt.Sprintf("nvidia %s unavailable: %s", mode, strings.TrimSpace(snapshot.Reason))
	}
	return fmt.Sprintf("nvidia %s unavailable", mode)
}

func discoveryFailureReason(err error) string {
	var commandErr *CommandError
	if errors.As(err, &commandErr) {
		switch commandErr.Kind {
		case CommandFailureUnavailable:
			return commandName(commandErr.Argv) + " unavailable"
		case CommandFailureTimeout:
			return commandName(commandErr.Argv) + " timed out"
		}
	}
	return "nvidia inventory discovery failed: " + boundedDiagnostic([]byte(err.Error()))
}

func normalizeV2BackendError(action *tgsrlv1.Action, err error) error {
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		return err
	}
	switch commandErr.Kind {
	case CommandFailureUnavailable:
		return v2ActionError(action, ErrorCodeUnavailable, commandErr.Error(), base.ErrFailedPrecondition)
	case CommandFailureTimeout:
		return v2ActionError(action, base.ErrorCodeDeadlineExceeded, commandErr.Error(), errors.Join(base.ErrDeadlineExceeded, commandErr))
	case CommandFailureCanceled:
		return v2ActionError(action, base.ErrorCodeDeadlineExceeded, commandErr.Error(), errors.Join(context.Canceled, commandErr))
	case CommandFailureInvalid:
		return v2ActionError(action, base.ErrorCodeInvalidArgument, commandErr.Error(), base.ErrInvalidArgument)
	default:
		return v2ActionError(action, base.ErrorCodeFailedPrecondition, commandErr.Error(), base.ErrFailedPrecondition)
	}
}

func auditForBackendResult(sequence uint64, now time.Time, mode PartitionMode, dryRun bool, action *tgsrlv1.Action, result *BackendActionResult, err error) AuditRecord {
	record := AuditRecord{Sequence: sequence, Operation: "action", PartitionMode: mode, DryRun: dryRun, Status: AuditStatusSucceeded, OccurredAt: now}
	if action != nil {
		record.ActionID, record.SandboxID, record.Generation, record.IdempotencyKey = action.GetActionId(), actionSandboxID(action), action.GetExpectedGeneration(), action.GetIdempotencyKey()
	}
	if result != nil {
		record.Commands = cloneCommands(result.Commands)
		for _, commandResult := range result.Results {
			record.ExitCodes = append(record.ExitCodes, commandResult.ExitCode)
		}
	}
	if dryRun {
		record.Status = AuditStatusPlanned
	}
	if err != nil {
		record.Status, record.ErrorMessage = AuditStatusFailed, boundedDiagnostic([]byte(err.Error()))
		record.ErrorCode = errorCode(normalizeV2BackendError(action, err))
	}
	return record
}

func v2Capabilities(inventory *InventorySnapshot, partitions *PartitionSnapshot, bindingActions, runtimeActions []tgsrlv1.ActionType, dryRun bool) *tgsrlv1.CapabilitySet {
	capabilities := localCapabilities(inventory.DriverVersion, inventory.ObservedAt)
	capabilities.Attributes[base.CapabilityVersionAttributeKey(CapabilityName)] = "1.0.0"
	capabilities.Attributes["execution_mode"] = "driver-v2"
	capabilities.Attributes["partition_mode"] = string(partitions.Mode)
	capabilities.Attributes["dry_run"] = strconv.FormatBool(dryRun)
	capabilities.Names = append(capabilities.Names, partitionCapability(partitions.Mode))
	actions := append([]tgsrlv1.ActionType(nil), bindingActions...)
	actions = append(actions, partitions.SupportedActions...)
	actions = append(actions, runtimeActions...)
	capabilities.SupportedActions = stableActionNames(actions)
	capabilities.Limits["partitions"] = float64(len(partitions.Partitions))
	capabilities.Evidence = append(capabilities.Evidence, &tgsrlv1.CapabilityEvidence{
		EvidenceId: "nvidia-inventory", Source: commandNvidiaSMI, Revision: capabilities.GetRevision(), ObservedAt: timestamppb.New(inventory.ObservedAt), Collector: "nvidia-driver-v2", Detail: "physical GPU inventory discovered", Attributes: map[string]string{"device_count": strconv.Itoa(len(inventory.Devices)), "topology_warnings": strconv.Itoa(len(inventory.Warnings))},
	}, &tgsrlv1.CapabilityEvidence{
		EvidenceId: "nvidia-partition-" + string(partitions.Mode), Source: commandNvidiaSMI, Revision: capabilities.GetRevision(), ObservedAt: timestamppb.New(partitions.ObservedAt), Collector: "nvidia-driver-v2", Detail: "partition capability discovered", Attributes: map[string]string{"mode": string(partitions.Mode)},
	})
	return capabilities
}

func v2UnavailableCapabilities(reason string, mode PartitionMode, inventory *InventorySnapshot) *tgsrlv1.CapabilitySet {
	capabilities := unavailableCapabilities(reason)
	capabilities.Attributes["partition_mode"] = string(mode)
	if inventory != nil && inventory.DriverVersion != "" {
		capabilities.MeasuredAt = timestamppb.New(inventory.ObservedAt)
		capabilities.ComponentVersions = localCapabilities(inventory.DriverVersion, inventory.ObservedAt).ComponentVersions
	}
	return capabilities
}

func partitionCapability(mode PartitionMode) string {
	if mode == PartitionModeMIG {
		return CapabilityMIG
	}
	return CapabilityMPS
}

func inventoryDevices(inventory *InventorySnapshot, partitions *PartitionSnapshot, capabilities *tgsrlv1.CapabilitySet) []*tgsrlv1.Device {
	if partitions.Mode == PartitionModeMIG {
		return migDevices(inventory, partitions, capabilities)
	}
	partitionByParent := make(map[string][]Partition)
	for _, partition := range partitions.Partitions {
		partitionByParent[partition.ParentUUID] = append(partitionByParent[partition.ParentUUID], partition)
	}
	devices := make([]*tgsrlv1.Device, 0, len(inventory.Devices))
	for _, observed := range inventory.Devices {
		acceleratorUnits := 1.0
		if partitions.Mode == PartitionModeMIG {
			acceleratorUnits = float64(len(partitionByParent[observed.UUID]))
		}
		labels := map[string]string{"provider": ProviderID, "uuid": observed.UUID, "index": observed.Index, "name": observed.Name, "pci_bus_id": observed.PCIAddress, "numa_node": observed.NUMANode, "partition_mode": string(partitions.Mode)}
		for key, value := range observed.Topology {
			labels["topology."+key] = value
		}
		devices = append(devices, &tgsrlv1.Device{DeviceId: stableGPUDeviceID(observed.UUID), Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU, Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: &tgsrlv1.ResourceVector{AcceleratorUnits: acceleratorUnits, MemoryBytes: observed.MemoryBytes}, Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: acceleratorUnits, MemoryBytes: observed.MemoryBytes}, Capabilities: cloneCapabilities(capabilities), Labels: labels})
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].GetDeviceId() < devices[j].GetDeviceId() })
	return devices
}

func migDevices(inventory *InventorySnapshot, partitions *PartitionSnapshot, capabilities *tgsrlv1.CapabilitySet) []*tgsrlv1.Device {
	parents := make(map[string]InventoryDevice, len(inventory.Devices))
	for _, device := range inventory.Devices {
		parents[device.UUID] = device
	}
	devices := make([]*tgsrlv1.Device, 0, len(partitions.Partitions))
	for _, partition := range partitions.Partitions {
		parent := parents[partition.ParentUUID]
		memoryBytes := partition.MemoryBytes
		if memoryBytes == 0 {
			memoryBytes = parent.MemoryBytes
		}
		labels := map[string]string{"provider": ProviderID, "uuid": partition.ID, "parent_uuid": partition.ParentUUID, "profile": partition.Profile, "partition_mode": string(PartitionModeMIG), "pci_bus_id": parent.PCIAddress, "numa_node": parent.NUMANode}
		for key, value := range partition.Labels {
			labels[key] = value
		}
		devices = append(devices, &tgsrlv1.Device{DeviceId: partition.ID, Kind: tgsrlv1.DeviceKind_DEVICE_KIND_GPU, Health: tgsrlv1.DeviceHealth_DEVICE_HEALTH_READY, Capacity: &tgsrlv1.ResourceVector{AcceleratorUnits: 1, MemoryBytes: memoryBytes}, Allocatable: &tgsrlv1.ResourceVector{AcceleratorUnits: 1, MemoryBytes: memoryBytes}, Capabilities: cloneCapabilities(capabilities), Labels: labels})
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].GetDeviceId() < devices[j].GetDeviceId() })
	return devices
}

func discoveredSandboxes(bindings []DiscoveredBinding, now time.Time) []base.Sandbox {
	result := make([]base.Sandbox, 0, len(bindings))
	for _, binding := range bindings {
		if binding.SandboxID == "" || binding.Generation == 0 || len(binding.DeviceIDs) == 0 {
			continue
		}
		result = append(result, base.Sandbox{SandboxID: binding.SandboxID, State: base.SandboxStateBound, Generation: binding.Generation, Binding: &tgsrlv1.Binding{BindingId: binding.BindingID, SandboxId: binding.SandboxID, Generation: binding.Generation, DeviceIds: append([]string(nil), binding.DeviceIDs...)}, Share: binding.Share, StateChangedAt: now, UpdatedAt: now})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SandboxID < result[j].SandboxID })
	return result
}

func mergeDiscoveredBindings(state *DriverState, discovered []base.Sandbox, now time.Time) []base.Sandbox {
	if state == nil {
		return cloneSandboxSlice(discovered)
	}
	result := make([]base.Sandbox, 0, len(discovered))
	for _, bindingObservation := range discovered {
		current, exists := state.Sandboxes[bindingObservation.SandboxID]
		if !exists {
			result = append(result, cloneSandbox(bindingObservation))
			continue
		}
		current.Binding = cloneBinding(bindingObservation.Binding)
		current.Generation = bindingObservation.Generation
		current.Share = bindingObservation.Share
		current.UpdatedAt = now
		result = append(result, cloneSandbox(current))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SandboxID < result[j].SandboxID })
	return result
}

func mergeRuntimeDiscovery(runtimeSandboxes, bindingSandboxes []base.Sandbox, now time.Time) []base.Sandbox {
	bindings := make(map[string]base.Sandbox, len(bindingSandboxes))
	for _, sandbox := range bindingSandboxes {
		bindings[sandbox.SandboxID] = sandbox
	}
	result := make([]base.Sandbox, 0, len(runtimeSandboxes))
	for _, sandbox := range runtimeSandboxes {
		if binding, exists := bindings[sandbox.SandboxID]; exists {
			sandbox.Binding = cloneBinding(binding.Binding)
			sandbox.Share = binding.Share
		}
		sandbox.UpdatedAt = now
		result = append(result, cloneSandbox(sandbox))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SandboxID < result[j].SandboxID })
	return result
}

func validateDiscoveredBindings(bindings []DiscoveredBinding, inventory *InventorySnapshot, partitions *PartitionSnapshot) error {
	knownDevices := make(map[string]struct{})
	if partitions.Mode == PartitionModeMIG {
		for _, partition := range partitions.Partitions {
			knownDevices[partition.ID] = struct{}{}
		}
	} else {
		for _, device := range inventory.Devices {
			knownDevices[stableGPUDeviceID(device.UUID)] = struct{}{}
		}
	}
	seenBindings := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		if binding.SandboxID == "" || binding.BindingID == "" || binding.Generation == 0 || len(binding.DeviceIDs) == 0 {
			return fmt.Errorf("binding %q has incomplete identity", binding.BindingID)
		}
		if _, duplicate := seenBindings[binding.BindingID]; duplicate {
			return fmt.Errorf("duplicate binding_id %q", binding.BindingID)
		}
		seenBindings[binding.BindingID] = struct{}{}
		for _, deviceID := range binding.DeviceIDs {
			if _, exists := knownDevices[deviceID]; !exists {
				return fmt.Errorf("binding %q references unknown device %q", binding.BindingID, deviceID)
			}
		}
	}
	return nil
}

func stableGPUDeviceID(uuid string) string {
	uuid = strings.TrimSpace(uuid)
	if uuid == "" {
		return ""
	}
	return uuid
}

func validShare(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value > 0 && value <= 1
}
