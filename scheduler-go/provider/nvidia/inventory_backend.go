package nvidia

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var inventoryQueryFields = []string{
	"index", "uuid", "pci.bus_id", "memory.total", "name", "driver_version", "mig.mode.current", "compute_mode",
}

// CommandInventoryBackend discovers GPUs using nvidia-smi through CommandExecutor.
type CommandInventoryBackend struct {
	executor CommandExecutor
	timeout  time.Duration
	now      func() time.Time
}

// NewCommandInventoryBackend constructs the production inventory backend.
func NewCommandInventoryBackend(executor CommandExecutor, timeout time.Duration, now func() time.Time) *CommandInventoryBackend {
	if executor == nil {
		executor = NewExecCommandExecutor()
	}
	if timeout <= 0 {
		timeout = defaultCommandTimeout
	}
	if now == nil {
		now = time.Now
	}
	return &CommandInventoryBackend{executor: executor, timeout: timeout, now: now}
}

// Discover returns a complete, stable-identity inventory.
func (b *CommandInventoryBackend) Discover(ctx context.Context) (*InventorySnapshot, error) {
	ctx, cancel := commandContext(ctx, b.timeout)
	defer cancel()
	command, err := NewCommandBuilder(commandNvidiaSMI).Arg(
		"--query-gpu="+strings.Join(inventoryQueryFields, ","),
		"--format=csv,noheader,nounits",
	).Build()
	if err != nil {
		return nil, err
	}
	result, err := b.executor.Execute(ctx, command)
	if err != nil {
		return nil, err
	}
	snapshot, err := parseInventory(result.Stdout, b.now().UTC())
	if err != nil || len(snapshot.Devices) == 0 {
		return snapshot, err
	}
	topologyCommand := Command{Argv: []string{commandNvidiaSMI, "topo", "-m"}}
	topologyResult, topologyErr := b.executor.Execute(ctx, topologyCommand)
	if topologyErr != nil {
		snapshot.Warnings = append(snapshot.Warnings, "topology discovery unavailable: "+boundedDiagnostic([]byte(topologyErr.Error())))
		return snapshot, nil
	}
	if topologyErr := applyTopology(snapshot, topologyResult.Stdout); topologyErr != nil {
		snapshot.Warnings = append(snapshot.Warnings, "topology discovery invalid: "+topologyErr.Error())
	}
	return snapshot, nil
}

func parseInventory(output []byte, observedAt time.Time) (*InventorySnapshot, error) {
	if strings.TrimSpace(string(output)) == "" {
		return &InventorySnapshot{ObservedAt: observedAt}, nil
	}
	reader := csv.NewReader(strings.NewReader(strings.TrimSpace(string(output))))
	reader.TrimLeadingSpace = true
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse nvidia inventory: %w", err)
	}
	seenIndices := make(map[string]struct{}, len(records))
	seenUUIDs := make(map[string]struct{}, len(records))
	snapshot := &InventorySnapshot{ObservedAt: observedAt}
	for row, record := range records {
		if len(record) != len(inventoryQueryFields) {
			return nil, fmt.Errorf("nvidia inventory row %d has %d fields, want %d", row+1, len(record), len(inventoryQueryFields))
		}
		for index := range record {
			record[index] = strings.TrimSpace(record[index])
		}
		index, uuid := record[0], record[1]
		if index == "" || uuid == "" {
			return nil, fmt.Errorf("nvidia inventory row %d is missing index or uuid", row+1)
		}
		if _, duplicate := seenIndices[index]; duplicate {
			return nil, fmt.Errorf("nvidia inventory contains duplicate index %q", index)
		}
		if _, duplicate := seenUUIDs[uuid]; duplicate {
			return nil, fmt.Errorf("nvidia inventory contains duplicate uuid %q", uuid)
		}
		seenIndices[index], seenUUIDs[uuid] = struct{}{}, struct{}{}
		memoryMB, err := strconv.ParseUint(record[3], 10, 63)
		if err != nil || memoryMB == 0 || memoryMB > ^uint64(0)>>20 {
			return nil, fmt.Errorf("nvidia inventory row %d has invalid memory.total %q", row+1, record[3])
		}
		if !isAuthoritativeDriverVersion(record[5]) {
			return nil, fmt.Errorf("nvidia inventory row %d has unavailable driver_version", row+1)
		}
		if snapshot.DriverVersion == "" {
			snapshot.DriverVersion = record[5]
		} else if snapshot.DriverVersion != record[5] {
			return nil, fmt.Errorf("conflicting nvidia driver versions %q and %q", snapshot.DriverVersion, record[5])
		}
		migMode := strings.ToLower(strings.TrimSpace(record[6]))
		switch migMode {
		case "enabled":
		case "disabled", "n/a", "[n/a]":
			migMode = "disabled"
		default:
			return nil, fmt.Errorf("nvidia inventory row %d has unknown MIG mode %q", row+1, record[6])
		}
		snapshot.Devices = append(snapshot.Devices, InventoryDevice{
			UUID: uuid, Index: index, PCIAddress: record[2], MemoryBytes: memoryMB << 20, Name: record[4], DriverVersion: record[5], MIGEnabled: migMode == "enabled", ComputeMode: record[7], Topology: map[string]string{"pci_bus_id": record[2]},
		})
	}
	return snapshot, nil
}

func commandContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func applyTopology(snapshot *InventorySnapshot, output []byte) error {
	if snapshot == nil || strings.TrimSpace(string(output)) == "" {
		return fmt.Errorf("empty topology output")
	}
	byIndex := make(map[string]*InventoryDevice, len(snapshot.Devices))
	for index := range snapshot.Devices {
		byIndex[snapshot.Devices[index].Index] = &snapshot.Devices[index]
	}
	var headers []string
	for _, rawLine := range strings.Split(string(output), "\n") {
		fields := strings.Fields(rawLine)
		if len(fields) == 0 {
			continue
		}
		if !strings.HasPrefix(fields[0], "GPU") {
			if len(headers) == 0 {
				for _, field := range fields {
					if strings.HasPrefix(field, "GPU") {
						headers = append(headers, strings.TrimPrefix(field, "GPU"))
					}
				}
			}
			continue
		}
		if len(headers) == 0 {
			for _, observed := range snapshot.Devices {
				headers = append(headers, observed.Index)
			}
		}
		if len(fields) < len(headers)+2 {
			continue
		}
		device := byIndex[strings.TrimPrefix(fields[0], "GPU")]
		if device == nil {
			continue
		}
		for peerOffset, peerIndex := range headers {
			peer := byIndex[peerIndex]
			if peer != nil && fields[peerOffset+1] != "X" {
				device.Topology["link."+peer.UUID] = fields[peerOffset+1]
			}
		}
		device.Topology["cpu_affinity"] = fields[len(headers)+1]
		if len(fields) > len(headers)+2 {
			device.NUMANode = fields[len(headers)+2]
			device.Topology["numa_node"] = device.NUMANode
		}
	}
	return nil
}

func isCommandUnavailable(err error) bool {
	var commandErr *CommandError
	return errors.As(err, &commandErr) && commandErr.Kind == CommandFailureUnavailable
}
