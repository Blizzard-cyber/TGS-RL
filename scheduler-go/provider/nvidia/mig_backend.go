package nvidia

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	base "github.com/Blizzard-cyber/TGS-RL/scheduler-go/provider"
)

// MIGBackend manages explicitly selected MIG partitions.
type MIGBackend struct {
	executor CommandExecutor
	timeout  time.Duration
}

// NewMIGBackend constructs a MIG backend. It never falls back to MPS.
func NewMIGBackend(executor CommandExecutor, timeout time.Duration) *MIGBackend {
	if executor == nil {
		executor = NewExecCommandExecutor()
	}
	if timeout <= 0 {
		timeout = defaultCommandTimeout
	}
	return &MIGBackend{executor: executor, timeout: timeout}
}

// Mode identifies MIG.
func (*MIGBackend) Mode() PartitionMode { return PartitionModeMIG }

// Discover requires MIG mode, authoritative MIG UUIDs, capacities, profiles,
// and helper-supported mutation verbs.
func (b *MIGBackend) Discover(ctx context.Context, inventory *InventorySnapshot) (*PartitionSnapshot, error) {
	if inventory == nil || len(inventory.Devices) == 0 {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: "physical GPU inventory is empty"}, nil
	}
	for _, device := range inventory.Devices {
		if !device.MIGEnabled {
			return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: "MIG mode is disabled on GPU " + device.UUID, ObservedAt: inventory.ObservedAt}, nil
		}
	}
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	listCommand := Command{Argv: []string{commandNvidiaSMI, "-L"}}
	result, err := b.executor.Execute(executionContext, listCommand)
	if err != nil {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: boundedDiagnostic(result.Stderr), ObservedAt: inventory.ObservedAt}, err
	}
	partitions, err := parseMIGList(result.Stdout, inventory)
	if err != nil {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: err.Error(), ObservedAt: inventory.ObservedAt}, nil
	}
	if len(partitions) == 0 {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: "MIG mode is enabled but no MIG devices were discovered", ObservedAt: inventory.ObservedAt}, nil
	}
	capacityCommand := Command{Argv: []string{"tgsrl-nvidia-mig", "inventory", "--format=csv"}}
	capacityResult, capacityErr := b.executor.Execute(executionContext, capacityCommand)
	if capacityErr != nil {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: "MIG capacity discovery failed: " + capacityErr.Error(), ObservedAt: inventory.ObservedAt}, nil
	}
	if err := applyMIGCapacities(partitions, capacityResult.Stdout); err != nil {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: err.Error(), ObservedAt: inventory.ObservedAt}, nil
	}
	capabilityCommand := Command{Argv: []string{"tgsrl-nvidia-mig", "capabilities", "--format=csv"}}
	capabilityResult, capabilityErr := b.executor.Execute(executionContext, capabilityCommand)
	if capabilityErr != nil {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: "MIG helper unavailable: " + capabilityErr.Error(), ObservedAt: inventory.ObservedAt}, nil
	}
	handshake, handshakeErr := parseBackendHandshake(capabilityResult.Stdout, "tgsrl-nvidia-mig", map[string]tgsrlv1.ActionType{"rebind": tgsrlv1.ActionType_ACTION_TYPE_REBIND, "recreate": tgsrlv1.ActionType_ACTION_TYPE_RECREATE})
	if handshakeErr != nil {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: handshakeErr.Error(), ObservedAt: inventory.ObservedAt}, nil
	}
	if len(handshake.actions) == 0 ||
		!handshake.features["durable_receipts"] ||
		!handshake.features["generation_fence"] ||
		!handshake.features["idempotency"] ||
		!handshake.features["safe_point"] ||
		!handshake.features["checkpoint"] ||
		!handshake.features["stop"] ||
		!handshake.features["restore"] ||
		!handshake.features["readiness"] {
		return &PartitionSnapshot{Mode: PartitionModeMIG, Reason: "MIG helper is missing required transactional lifecycle capabilities", ObservedAt: inventory.ObservedAt}, nil
	}
	return &PartitionSnapshot{Mode: PartitionModeMIG, Available: true, Partitions: partitions, SupportedActions: handshake.actions, ObservedAt: inventory.ObservedAt}, nil
}

// Apply performs only explicit slow/L4 MIG mutations.
func (b *MIGBackend) Apply(ctx context.Context, request BackendActionRequest) (*BackendActionResult, error) {
	action := request.Action
	if action == nil {
		return nil, fmt.Errorf("%w: action is required", base.ErrInvalidArgument)
	}
	if action.GetTickKind() != tgsrlv1.TickKind_TICK_KIND_SLOW || action.GetLevel() != tgsrlv1.ActionLevel_ACTION_LEVEL_L4 {
		return nil, v2ActionError(action, ErrorCodeUnavailable, "MIG mutation requires an explicit slow-tick L4 action", base.ErrFailedPrecondition)
	}
	if request.Partitions == nil || request.Partitions.Mode != PartitionModeMIG || !request.Partitions.Available {
		return nil, v2ActionError(action, ErrorCodeUnavailable, "MIG partitions are unavailable", base.ErrFailedPrecondition)
	}
	if action.GetActionId() != "" && !containsActionType(request.Partitions.SupportedActions, action.GetActionType()) {
		return nil, v2ActionError(action, ErrorCodeUnavailable, "MIG helper did not advertise the requested mutation", base.ErrFailedPrecondition)
	}
	executionContext, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_REBIND, tgsrlv1.ActionType_ACTION_TYPE_RECREATE:
		partition, err := migTargetPartition(request.Partitions, action)
		if err != nil {
			return nil, err
		}
		command := Command{Argv: []string{"tgsrl-nvidia-mig", actionName(action.GetActionType()), "--sandbox", actionSandboxID(action), "--parent-uuid", partition.ParentUUID, "--target-uuid", partition.ID, "--profile", partition.Profile, "--generation", strconv.FormatUint(action.GetExpectedGeneration(), 10), "--idempotency-key", action.GetIdempotencyKey()}}
		result := &BackendActionResult{Detail: "MIG partition mutation applied", Commands: []Command{command}}
		if request.DryRun {
			return result, nil
		}
		commandResult, err := b.executor.Execute(executionContext, command)
		result.Results = []CommandResult{commandResult}
		if err != nil {
			return result, err
		}
		receipt, err := parseMIGMutationReceipt(commandResult.Stdout, action)
		if err != nil {
			return result, err
		}
		result.Binding = receipt
		return result, nil
	default:
		return nil, fmt.Errorf("%w: MIG backend does not support %s", base.ErrUnsupported, action.GetActionType())
	}
}

func parseMIGMutationReceipt(output []byte, action *tgsrlv1.Action) (*DiscoveredBinding, error) {
	fields := strings.Split(strings.TrimSpace(string(output)), ",")
	if len(fields) != 4 {
		return nil, v2ActionError(action, ErrorCodeUnavailable, "MIG helper did not return parent UUID, partition UUID, profile, and generation", base.ErrFailedPrecondition)
	}
	for index := range fields {
		fields[index] = strings.TrimSpace(fields[index])
	}
	generation, err := strconv.ParseUint(fields[3], 10, 64)
	if err != nil || generation == 0 || fields[0] == "" || !strings.HasPrefix(fields[1], "MIG-") || fields[2] == "" {
		return nil, v2ActionError(action, ErrorCodeUnavailable, "MIG helper returned an invalid mutation receipt", base.ErrFailedPrecondition)
	}
	return &DiscoveredBinding{SandboxID: actionSandboxID(action), BindingID: action.GetBinding().GetBindingId(), Generation: generation, DeviceIDs: []string{fields[1]}, Share: 1}, nil
}

func parseMIGList(output []byte, inventory *InventorySnapshot) ([]Partition, error) {
	parentUUID := ""
	partitions := make([]Partition, 0)
	seen := make(map[string]struct{})
	for _, rawLine := range strings.Split(string(output), "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "GPU ") {
			start, end := strings.LastIndex(line, "(UUID: "), strings.LastIndex(line, ")")
			if start < 0 || end <= start+7 {
				return nil, fmt.Errorf("invalid nvidia-smi GPU list row %q", line)
			}
			parentUUID = strings.TrimSpace(line[start+7 : end])
			continue
		}
		if !strings.HasPrefix(line, "MIG ") {
			continue
		}
		start, end := strings.LastIndex(line, "(UUID: "), strings.LastIndex(line, ")")
		if parentUUID == "" || start < 0 || end <= start+7 {
			return nil, fmt.Errorf("invalid nvidia-smi MIG list row %q", line)
		}
		uuid := strings.TrimSpace(line[start+7 : end])
		if !strings.HasPrefix(uuid, "MIG-") {
			return nil, fmt.Errorf("invalid MIG UUID %q", uuid)
		}
		if _, duplicate := seen[uuid]; duplicate {
			return nil, fmt.Errorf("duplicate MIG UUID %q", uuid)
		}
		seen[uuid] = struct{}{}
		profile := strings.TrimSpace(strings.TrimPrefix(line[:start], "MIG"))
		partitions = append(partitions, Partition{ID: uuid, ParentUUID: parentUUID, Profile: profile, Labels: map[string]string{"mode": string(PartitionModeMIG), "parent_uuid": parentUUID}})
	}
	parents := make(map[string]struct{}, len(inventory.Devices))
	for _, device := range inventory.Devices {
		parents[device.UUID] = struct{}{}
	}
	for _, partition := range partitions {
		if _, ok := parents[partition.ParentUUID]; !ok {
			return nil, fmt.Errorf("MIG device %q references unknown parent %q", partition.ID, partition.ParentUUID)
		}
	}
	return partitions, nil
}

func migTargetPartition(snapshot *PartitionSnapshot, action *tgsrlv1.Action) (Partition, error) {
	if action.GetBinding() == nil || len(action.GetBinding().GetDeviceIds()) != 1 {
		return Partition{}, fmt.Errorf("%w: MIG mutation requires one target device", base.ErrInvalidArgument)
	}
	targetID := action.GetBinding().GetDeviceIds()[0]
	for _, partition := range snapshot.Partitions {
		if partition.ID == targetID {
			return partition, nil
		}
	}
	return Partition{}, v2ActionError(action, ErrorCodeUnavailable, "target MIG device is unavailable", base.ErrFailedPrecondition)
}

func applyMIGCapacities(partitions []Partition, output []byte) error {
	rows := strings.Split(strings.TrimSpace(string(output)), "\n")
	capacities := make(map[string]Partition, len(rows))
	for row, raw := range rows {
		fields := strings.Split(raw, ",")
		if len(fields) != 3 {
			return fmt.Errorf("MIG capacity row %d has %d fields, want 3", row+1, len(fields))
		}
		for index := range fields {
			fields[index] = strings.TrimSpace(fields[index])
		}
		memoryMB, err := strconv.ParseUint(fields[2], 10, 63)
		if err != nil || memoryMB == 0 || memoryMB > ^uint64(0)>>20 {
			return fmt.Errorf("MIG capacity row %d has invalid memory.total %q", row+1, fields[2])
		}
		capacities[fields[0]] = Partition{ID: fields[0], Profile: fields[1], MemoryBytes: memoryMB << 20}
	}
	for index := range partitions {
		capacity, ok := capacities[partitions[index].ID]
		if !ok {
			return fmt.Errorf("MIG device %q has no authoritative capacity", partitions[index].ID)
		}
		partitions[index].Profile = capacity.Profile
		partitions[index].MemoryBytes = capacity.MemoryBytes
	}
	return nil
}

func actionName(actionType tgsrlv1.ActionType) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(actionType.String(), "ACTION_TYPE_")), "_", "-")
}
