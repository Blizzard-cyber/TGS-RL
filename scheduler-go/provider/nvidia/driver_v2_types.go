package nvidia

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
	"google.golang.org/protobuf/proto"
)

const (
	CapabilityMPS = "nvidia-mps"
	CapabilityMIG = "nvidia-mig"
)

const backendProtocolVersion = "1"

type backendHandshake struct {
	identity string
	version  string
	actions  []tgsrlv1.ActionType
	features map[string]bool
}

// PartitionMode identifies the selected NVIDIA isolation strategy.
type PartitionMode string

const (
	PartitionModeMPS PartitionMode = "mps"
	PartitionModeMIG PartitionMode = "mig"
)

// InventoryDevice is one physical GPU observation keyed by its stable UUID.
type InventoryDevice struct {
	UUID          string
	Index         string
	PCIAddress    string
	NUMANode      string
	Name          string
	DriverVersion string
	MemoryBytes   uint64
	MIGEnabled    bool
	ComputeMode   string
	Topology      map[string]string
}

// InventorySnapshot is a complete physical GPU inventory observation.
type InventorySnapshot struct {
	Devices       []InventoryDevice
	DriverVersion string
	ObservedAt    time.Time
	Warnings      []string
}

// Partition describes one scheduler-visible MPS or MIG resource.
type Partition struct {
	ID          string
	ParentUUID  string
	Profile     string
	MemoryBytes uint64
	Share       float64
	Labels      map[string]string
}

// PartitionSnapshot is one authoritative partition discovery result.
type PartitionSnapshot struct {
	Mode                    PartitionMode
	Available               bool
	Reason                  string
	Partitions              []Partition
	SupportedActions        []tgsrlv1.ActionType
	RequiresBindingMetadata bool
	ObservedAt              time.Time
}

// MPSProfile is the active-thread profile assigned to one sandbox generation.
type MPSProfile struct {
	SandboxID              string
	Generation             uint64
	ActiveThreadPercentage int
	UpdatedAt              time.Time
}

// DiscoveredBinding is durable backend state recovered after driver restart.
type DiscoveredBinding struct {
	SandboxID      string
	BindingID      string
	Generation     uint64
	DeviceIDs      []string
	Share          float64
	ServerPID      uint32
	IdempotencyKey string
}

// BindingSnapshot is an authoritative restart-discovery result.
type BindingSnapshot struct {
	Available           bool
	Reason              string
	Bindings            []DiscoveredBinding
	SupportedActions    []tgsrlv1.ActionType
	SupportsMPSProfiles bool
}

// RuntimeBackendStatus is the discovered runtime control surface.
type RuntimeBackendStatus struct {
	Available              bool
	Reason                 string
	SupportedActions       []tgsrlv1.ActionType
	Sandboxes              []base.Sandbox
	SandboxesAuthoritative bool
}

// RecoveredAction is one durable helper receipt used after provider restart.
type RecoveredAction struct {
	StepIndex        int
	ExpectedActions  int
	Committed        bool
	PlanDigest       string
	ActionID         string
	PlanID           string
	IdempotencyKey   string
	SandboxID        string
	Generation       uint64
	ActionGeneration uint64
	Succeeded        bool
	ErrorCode        string
	ErrorMessage     string
	CommandDigest    string
}

// BackendActionRequest contains the fenced state passed to one action backend.
type BackendActionRequest struct {
	State      *DriverState
	Action     *tgsrlv1.Action
	Partitions *PartitionSnapshot
	Binding    *DiscoveredBinding
	DryRun     bool
	Receipt    *HelperReceiptContext
}

// HelperReceiptContext carries transaction identity to command helpers so a
// completed host mutation can be recovered after the Scheduler process exits.
type HelperReceiptContext struct {
	StepIndex             int
	ExpectedActions       int
	TransactionGeneration uint64
	PlanDigest            string
	CommandDigest         string
}

// BackendActionResult contains the commands and recovered binding produced by a backend.
type BackendActionResult struct {
	Detail                 string
	Commands               []Command
	Results                []CommandResult
	Binding                *DiscoveredBinding
	ObservedShare          *float64
	MutationMayHaveApplied bool
}

// InventoryBackend discovers physical devices, topology, and driver identity.
type InventoryBackend interface {
	Discover(context.Context) (*InventorySnapshot, error)
}

// PartitionBackend discovers and mutates the selected MPS or MIG partition surface.
type PartitionBackend interface {
	Mode() PartitionMode
	Discover(context.Context, *InventorySnapshot) (*PartitionSnapshot, error)
	Apply(context.Context, BackendActionRequest) (*BackendActionResult, error)
}

// PartitionDryRunConfig lets the driver suppress discovery side effects.
type PartitionDryRunConfig interface {
	SetDryRun(bool)
}

// RuntimeBackend performs runtime lifecycle actions that are outside partition management.
type RuntimeBackend interface {
	Discover(context.Context) (*RuntimeBackendStatus, error)
	Apply(context.Context, BackendActionRequest) (*BackendActionResult, error)
}

// BindingBackend discovers and mutates sandbox-to-device bindings.
type BindingBackend interface {
	Discover(context.Context, *InventorySnapshot, *PartitionSnapshot) (*BindingSnapshot, error)
	Apply(context.Context, BackendActionRequest) (*BackendActionResult, error)
}

// ReceiptBackend discovers durable action outcomes for restart reconciliation.
type ReceiptBackend interface {
	DiscoverReceipts(context.Context) ([]RecoveredAction, error)
}

// AuditStatus is the terminal status of one driver audit record.
type AuditStatus string

const (
	AuditStatusPlanned   AuditStatus = "planned"
	AuditStatusSucceeded AuditStatus = "succeeded"
	AuditStatusFailed    AuditStatus = "failed"
)

// AuditRecord is an immutable, sanitized record of one discovery or action.
type AuditRecord struct {
	Sequence       uint64
	Operation      string
	ActionID       string
	SandboxID      string
	Generation     uint64
	IdempotencyKey string
	PartitionMode  PartitionMode
	DryRun         bool
	Status         AuditStatus
	Commands       []Command
	ExitCodes      []int
	ErrorCode      string
	ErrorMessage   string
	OccurredAt     time.Time
}

// AuditSink receives detached NVIDIA driver audit records.
type AuditSink interface {
	Record(context.Context, AuditRecord) error
}

// InMemoryAuditSink retains a bounded, race-safe audit history.
type InMemoryAuditSink struct {
	mu        sync.Mutex
	retention int
	records   []AuditRecord
}

// NewInMemoryAuditSink constructs a bounded audit sink.
func NewInMemoryAuditSink(retention int) *InMemoryAuditSink {
	if retention <= 0 {
		retention = 256
	}
	return &InMemoryAuditSink{retention: retention}
}

// Record appends a detached audit record.
func (s *InMemoryAuditSink) Record(ctx context.Context, record AuditRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, cloneAuditRecord(record))
	if len(s.records) > s.retention {
		s.records = append([]AuditRecord(nil), s.records[len(s.records)-s.retention:]...)
	}
	return nil
}

// Records returns the retained audit history without aliases.
func (s *InMemoryAuditSink) Records() []AuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]AuditRecord, len(s.records))
	for index, record := range s.records {
		result[index] = cloneAuditRecord(record)
	}
	return result
}

// UnavailableRuntimeBackend explicitly rejects unsupported runtime controls.
type UnavailableRuntimeBackend struct {
	reason string
}

// NewUnavailableRuntimeBackend constructs a fail-closed runtime backend.
func NewUnavailableRuntimeBackend(reason string) *UnavailableRuntimeBackend {
	if strings.TrimSpace(reason) == "" {
		reason = "nvidia runtime control backend is unavailable"
	}
	return &UnavailableRuntimeBackend{reason: reason}
}

// Discover reports an explicitly unavailable runtime surface.
func (b *UnavailableRuntimeBackend) Discover(context.Context) (*RuntimeBackendStatus, error) {
	return &RuntimeBackendStatus{Reason: b.reason}, nil
}

// Apply always fails explicitly instead of reporting a false success.
func (b *UnavailableRuntimeBackend) Apply(_ context.Context, request BackendActionRequest) (*BackendActionResult, error) {
	return nil, v2ActionError(request.Action, ErrorCodeUnavailable, b.reason, base.ErrFailedPrecondition)
}

func cloneInventory(snapshot *InventorySnapshot) *InventorySnapshot {
	if snapshot == nil {
		return nil
	}
	result := *snapshot
	result.Devices = make([]InventoryDevice, len(snapshot.Devices))
	for index, device := range snapshot.Devices {
		result.Devices[index] = device
		result.Devices[index].Topology = cloneStringMap(device.Topology)
	}
	result.Warnings = append([]string(nil), snapshot.Warnings...)
	return &result
}

func clonePartitions(snapshot *PartitionSnapshot) *PartitionSnapshot {
	if snapshot == nil {
		return nil
	}
	result := *snapshot
	result.Partitions = make([]Partition, len(snapshot.Partitions))
	for index, partition := range snapshot.Partitions {
		result.Partitions[index] = partition
		result.Partitions[index].Labels = cloneStringMap(partition.Labels)
	}
	result.SupportedActions = append([]tgsrlv1.ActionType(nil), snapshot.SupportedActions...)
	return &result
}

func cloneBindings(bindings []DiscoveredBinding) []DiscoveredBinding {
	result := make([]DiscoveredBinding, len(bindings))
	for index, binding := range bindings {
		result[index] = binding
		result[index].DeviceIDs = append([]string(nil), binding.DeviceIDs...)
	}
	return result
}

func cloneAuditRecord(record AuditRecord) AuditRecord {
	record.Commands = sanitizeAuditCommands(record.Commands)
	record.ExitCodes = append([]int(nil), record.ExitCodes...)
	if record.IdempotencyKey != "" {
		record.IdempotencyKey = "<redacted>"
	}
	return record
}

func sanitizeAuditCommands(commands []Command) []Command {
	result := make([]Command, len(commands))
	for index, command := range commands {
		result[index] = Command{Argv: redactArgv(command.Argv)}
		if len(command.Stdin) > 0 {
			result[index].Stdin = []byte("<redacted>")
		}
	}
	return result
}

func redactArgv(argv []string) []string {
	result := append([]string(nil), argv...)
	for index := 0; index < len(result)-1; index++ {
		switch result[index] {
		case "--idempotency-key", "--token", "--secret", "--password":
			result[index+1] = "<redacted>"
		}
	}
	return result
}

func cloneCommands(commands []Command) []Command {
	result := make([]Command, len(commands))
	for index, command := range commands {
		result[index] = cloneCommand(command)
	}
	return result
}

func cloneBackendResult(result *BackendActionResult) *BackendActionResult {
	if result == nil {
		return nil
	}
	cloned := *result
	cloned.Commands = cloneCommands(result.Commands)
	cloned.Results = make([]CommandResult, len(result.Results))
	for index, commandResult := range result.Results {
		cloned.Results[index] = cloneCommandResult(commandResult)
	}
	if result.Binding != nil {
		binding := *result.Binding
		binding.DeviceIDs = append([]string(nil), result.Binding.DeviceIDs...)
		cloned.Binding = &binding
	}
	if result.ObservedShare != nil {
		value := *result.ObservedShare
		cloned.ObservedShare = &value
	}
	return &cloned
}

func cloneDiscoveredBinding(binding *DiscoveredBinding) *DiscoveredBinding {
	if binding == nil {
		return nil
	}
	result := *binding
	result.DeviceIDs = append([]string(nil), binding.DeviceIDs...)
	return &result
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func stableActionNames(values []tgsrlv1.ActionType) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if name := actionNames[value]; name != "" {
			set[name] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func parseBackendHandshake(output []byte, expectedIdentity string, supported map[string]tgsrlv1.ActionType) (backendHandshake, error) {
	fields := strings.Split(strings.TrimSpace(string(output)), ",")
	if len(fields) < 3 {
		return backendHandshake{}, fmt.Errorf("backend capability response must contain identity, protocol version, and actions")
	}
	result := backendHandshake{identity: strings.TrimSpace(fields[0]), version: strings.TrimSpace(fields[1]), features: make(map[string]bool)}
	if result.identity != expectedIdentity {
		return backendHandshake{}, fmt.Errorf("backend identity %q does not match %q", result.identity, expectedIdentity)
	}
	if result.version != backendProtocolVersion {
		return backendHandshake{}, fmt.Errorf("backend protocol version %q is unsupported", result.version)
	}
	for _, value := range fields[2:] {
		value = strings.TrimSpace(value)
		if actionType, ok := supported[value]; ok {
			result.actions = append(result.actions, actionType)
			continue
		}
		if value != "" {
			result.features[value] = true
		}
	}
	return result, nil
}

func v2ActionError(action *tgsrlv1.Action, code, message string, cause error) error {
	if action == nil {
		return &base.Error{Code: code, Message: message, Cause: cause}
	}
	return &base.Error{Code: code, Message: message, PlanID: action.GetPlanId(), ActionID: action.GetActionId(), SandboxID: actionSandboxID(action), Cause: cause}
}

func protoEqualCapabilities(left, right *tgsrlv1.CapabilitySet) bool {
	return proto.Equal(left, right)
}

func validatePartitionMode(mode PartitionMode) error {
	switch mode {
	case PartitionModeMPS, PartitionModeMIG:
		return nil
	default:
		return fmt.Errorf("%w: unsupported nvidia partition mode %q", base.ErrInvalidArgument, mode)
	}
}
