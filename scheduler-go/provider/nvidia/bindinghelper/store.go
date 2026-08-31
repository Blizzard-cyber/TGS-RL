// Package bindinghelper implements the durable local state used by the
// tgsrl-nvidia-binding command. It records scheduler-authorized binding
// metadata; physical GPU enforcement remains the responsibility of the
// container/runtime integration that supplies the validated MPS server PID.
package bindinghelper

import (
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/Blizzard-cyber/TGS-RL/internal/helperstate"
)

const SchemaVersion = 2

const supersededErrorCode = "SUPERSEDED"

type Binding struct {
	SandboxID      string   `json:"sandbox_id"`
	BindingID      string   `json:"binding_id"`
	Generation     uint64   `json:"generation"`
	DeviceIDs      []string `json:"device_ids"`
	Share          float64  `json:"share"`
	IdempotencyKey string   `json:"idempotency_key"`
	MPSServerPID   uint32   `json:"mps_server_pid,omitempty"`
}

type Receipt struct {
	Operation             string `json:"operation"`
	StepIndex             int    `json:"step_index"`
	ExpectedActions       int    `json:"expected_actions"`
	Committed             bool   `json:"committed"`
	PlanDigest            string `json:"plan_digest"`
	ActionID              string `json:"action_id"`
	PlanID                string `json:"plan_id"`
	IdempotencyKey        string `json:"idempotency_key"`
	SandboxID             string `json:"sandbox_id"`
	TransactionGeneration uint64 `json:"transaction_generation"`
	ActionGeneration      uint64 `json:"action_generation"`
	Succeeded             bool   `json:"succeeded"`
	ErrorCode             string `json:"error_code,omitempty"`
	ErrorMessage          string `json:"error_message,omitempty"`
	CommandDigest         string `json:"command_digest"`
	RequestDigest         string `json:"request_digest"`
	Recoverable           bool   `json:"recoverable,omitempty"`
}

type State struct {
	SchemaVersion int                `json:"schema_version"`
	Bindings      map[string]Binding `json:"bindings"`
	Receipts      map[string]Receipt `json:"receipts"`
	Heads         map[string]string  `json:"heads"`
}

type Store struct {
	state *helperstate.Store[State]
}

type ReplaceRequest struct {
	SandboxID        string
	SourceBindingID  string
	SourceDeviceID   string
	SourceGeneration uint64
	Target           Binding
}

func NewStore(path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("binding state path is required")
	}
	state, err := helperstate.New(path, emptyState, validateState)
	if err != nil {
		return nil, err
	}
	return &Store{state: state}, nil
}

func (s *Store) Update(fn func(*State) error) error {
	return s.state.Update(fn)
}

func (s *Store) Snapshot() (State, error) {
	state, err := s.state.Snapshot()
	return cloneState(state), err
}

func (s *Store) ApplyBinding(binding Binding, receipt Receipt) error {
	return s.Update(func(state *State) error {
		if err := validateBinding(binding); err != nil {
			return err
		}
		if binding.SandboxID != receipt.SandboxID || binding.Generation != receipt.ActionGeneration || binding.IdempotencyKey != receipt.IdempotencyKey {
			return errors.New("binding and receipt identity must match")
		}
		receipt.RequestDigest = BindingRequestDigest(binding)
		receipt.Operation = "bind"
		if err := validateReceipt(receipt); err != nil {
			return err
		}
		if existing, ok := state.Receipts[receipt.IdempotencyKey]; ok {
			if sameReceiptRequest(existing, receipt) {
				current, exists := state.Bindings[binding.SandboxID]
				if exists && bindingsEqual(current, binding) {
					return nil
				}
				return errors.New("idempotent bind result has been superseded")
			}
			return errors.New("idempotency key was reused with different receipt content")
		}
		if current, ok := state.Bindings[binding.SandboxID]; ok {
			if binding.Generation < current.Generation {
				return fmt.Errorf("stale generation %d; current generation is %d", binding.Generation, current.Generation)
			}
			if binding.Generation == current.Generation && !bindingStateEqual(current, binding) {
				return fmt.Errorf("generation %d is already bound with different content", binding.Generation)
			}
		}
		state.Bindings[binding.SandboxID] = binding
		state.Receipts[receipt.IdempotencyKey] = receipt
		state.Heads[binding.SandboxID] = receipt.IdempotencyKey
		if err := validateReceiptSet(state.Receipts); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) ReleaseBinding(sandboxID, target string, generation uint64, receipt Receipt) error {
	return s.Update(func(state *State) error {
		if sandboxID != receipt.SandboxID || generation != receipt.ActionGeneration {
			return errors.New("release and receipt identity must match")
		}
		receipt.RequestDigest = ReleaseRequestDigest(sandboxID, target, generation)
		receipt.Operation = "release"
		if err := validateReceipt(receipt); err != nil {
			return err
		}
		if existing, ok := state.Receipts[receipt.IdempotencyKey]; ok {
			if sameReceiptRequest(existing, receipt) {
				if _, exists := state.Bindings[sandboxID]; !exists {
					return nil
				}
				return errors.New("idempotent release result has been superseded")
			}
			return errors.New("idempotency key was reused with different receipt content")
		}
		current, ok := state.Bindings[sandboxID]
		if !ok {
			return fmt.Errorf("sandbox %q is not bound", sandboxID)
		}
		if generation != current.Generation {
			return fmt.Errorf("generation fence failed: expected %d, current generation is %d", generation, current.Generation)
		}
		delete(state.Bindings, sandboxID)
		state.Receipts[receipt.IdempotencyKey] = receipt
		state.Heads[sandboxID] = receipt.IdempotencyKey
		if err := validateReceiptSet(state.Receipts); err != nil {
			return err
		}
		return nil
	})
}

func (s *Store) ReplaceBinding(request ReplaceRequest, receipt Receipt) error {
	return s.Update(func(state *State) error {
		target := request.Target
		if err := validateBinding(target); err != nil {
			return err
		}
		if target.SandboxID != request.SandboxID || target.Generation != request.SourceGeneration+1 || target.IdempotencyKey != receipt.IdempotencyKey || receipt.SandboxID != request.SandboxID || receipt.ActionGeneration != target.Generation {
			return errors.New("replacement binding and receipt identity must match")
		}
		receipt.Operation = "replace"
		receipt.RequestDigest = BindingRequestDigest(target)
		if err := validateReceipt(receipt); err != nil {
			return err
		}
		if existing, exists := state.Receipts[receipt.IdempotencyKey]; exists {
			if sameReceiptRequest(existing, receipt) && bindingsEqual(state.Bindings[request.SandboxID], target) {
				return nil
			}
			return errors.New("idempotency key was reused with different replacement content")
		}
		current, ok := state.Bindings[request.SandboxID]
		if !ok {
			return fmt.Errorf("sandbox %q is not bound", request.SandboxID)
		}
		if current.BindingID != request.SourceBindingID || len(current.DeviceIDs) != 1 || current.DeviceIDs[0] != request.SourceDeviceID || current.Generation != request.SourceGeneration {
			return errors.New("source binding fence failed")
		}
		state.Bindings[request.SandboxID] = target
		state.Receipts[receipt.IdempotencyKey] = receipt
		state.Heads[request.SandboxID] = receipt.IdempotencyKey
		return validateReceiptSet(state.Receipts)
	})
}

func sameReceiptRequest(left, right Receipt) bool {
	return left == right
}

func BindingRequestDigest(binding Binding) string {
	devices := append([]string(nil), binding.DeviceIDs...)
	sort.Strings(devices)
	material := strings.Join([]string{"bind", binding.SandboxID, binding.BindingID, strconv.FormatUint(binding.Generation, 10), strings.Join(devices, ";"), strconv.FormatFloat(binding.Share, 'g', -1, 64)}, "\x00")
	digest := sha256.Sum256([]byte(material))
	return fmt.Sprintf("sha256:%x", digest[:])
}

func ReleaseRequestDigest(sandboxID, target string, generation uint64) string {
	material := strings.Join([]string{"release", sandboxID, target, strconv.FormatUint(generation, 10)}, "\x00")
	digest := sha256.Sum256([]byte(material))
	return fmt.Sprintf("sha256:%x", digest[:])
}

func validateBinding(binding Binding) error {
	if strings.TrimSpace(binding.SandboxID) == "" || strings.TrimSpace(binding.BindingID) == "" || binding.Generation == 0 || strings.TrimSpace(binding.IdempotencyKey) == "" || len(binding.DeviceIDs) == 0 {
		return errors.New("binding requires sandbox, binding, generation, idempotency key, and devices")
	}
	seen := make(map[string]struct{}, len(binding.DeviceIDs))
	for _, device := range binding.DeviceIDs {
		device = strings.TrimSpace(device)
		if device == "" {
			return errors.New("binding device IDs must be non-empty")
		}
		if _, duplicate := seen[device]; duplicate {
			return fmt.Errorf("binding device ID %q is duplicated", device)
		}
		seen[device] = struct{}{}
	}
	if math.IsNaN(binding.Share) || math.IsInf(binding.Share, 0) || binding.Share <= 0 || binding.Share > 1 {
		return errors.New("binding share must be within (0,1]")
	}
	return nil
}

func validateReceipt(receipt Receipt) error {
	if (receipt.Operation != "bind" && receipt.Operation != "release" && receipt.Operation != "replace") || receipt.StepIndex < 0 || receipt.ExpectedActions <= 0 || receipt.StepIndex >= receipt.ExpectedActions || receipt.TransactionGeneration == 0 || receipt.ActionGeneration == 0 || strings.TrimSpace(receipt.PlanDigest) == "" || strings.TrimSpace(receipt.ActionID) == "" || strings.TrimSpace(receipt.PlanID) == "" || strings.TrimSpace(receipt.IdempotencyKey) == "" || strings.TrimSpace(receipt.SandboxID) == "" || strings.TrimSpace(receipt.CommandDigest) == "" || strings.TrimSpace(receipt.RequestDigest) == "" {
		return errors.New("receipt identity is incomplete")
	}
	if receipt.Committed {
		return errors.New("binding helper cannot mark provider receipts committed")
	}
	return nil
}

func validateReceiptSet(receipts map[string]Receipt) error {
	type planSet struct {
		digest   string
		expected int
		steps    map[int]string
		actions  map[string]string
	}
	plans := make(map[string]*planSet)
	for key, receipt := range receipts {
		if key != receipt.IdempotencyKey {
			return fmt.Errorf("receipt state key %q does not match idempotency key %q", key, receipt.IdempotencyKey)
		}
		if err := validateReceipt(receipt); err != nil {
			return fmt.Errorf("invalid receipt %q: %w", key, err)
		}
		if !receipt.Recoverable {
			continue
		}
		planKey := receipt.PlanID + "\x00" + strconv.FormatUint(receipt.TransactionGeneration, 10)
		set := plans[planKey]
		if set == nil {
			set = &planSet{digest: receipt.PlanDigest, expected: receipt.ExpectedActions, steps: make(map[int]string), actions: make(map[string]string)}
			plans[planKey] = set
		}
		if set.digest != receipt.PlanDigest || set.expected != receipt.ExpectedActions {
			return fmt.Errorf("transaction receipt set for plan %q is inconsistent", receipt.PlanID)
		}
		if previous, duplicate := set.steps[receipt.StepIndex]; duplicate {
			return fmt.Errorf("transaction receipt step %d for plan %q duplicates %q", receipt.StepIndex, receipt.PlanID, previous)
		}
		if previous, duplicate := set.actions[receipt.ActionID]; duplicate {
			return fmt.Errorf("transaction receipt action %q for plan %q duplicates %q", receipt.ActionID, receipt.PlanID, previous)
		}
		set.steps[receipt.StepIndex] = key
		set.actions[receipt.ActionID] = key
	}
	return nil
}

func bindingsEqual(left, right Binding) bool {
	return bindingStateEqual(left, right) && left.IdempotencyKey == right.IdempotencyKey
}

func bindingStateEqual(left, right Binding) bool {
	leftDevices := append([]string(nil), left.DeviceIDs...)
	rightDevices := append([]string(nil), right.DeviceIDs...)
	sort.Strings(leftDevices)
	sort.Strings(rightDevices)
	if len(leftDevices) != len(rightDevices) {
		return false
	}
	for index := range leftDevices {
		if leftDevices[index] != rightDevices[index] {
			return false
		}
	}
	return left.SandboxID == right.SandboxID && left.BindingID == right.BindingID && left.Generation == right.Generation && left.Share == right.Share
}

func emptyState() State {
	return State{SchemaVersion: SchemaVersion, Bindings: map[string]Binding{}, Receipts: map[string]Receipt{}, Heads: map[string]string{}}
}

func validateState(state State) error {
	if state.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported binding state schema version %d", state.SchemaVersion)
	}
	if state.Bindings == nil || state.Receipts == nil || state.Heads == nil {
		return errors.New("binding state maps are required")
	}
	for key, binding := range state.Bindings {
		if key != binding.SandboxID {
			return fmt.Errorf("binding state key %q does not match sandbox %q", key, binding.SandboxID)
		}
		if err := validateBinding(binding); err != nil {
			return fmt.Errorf("invalid binding %q: %w", key, err)
		}
	}
	if err := validateReceiptSet(state.Receipts); err != nil {
		return err
	}
	for sandboxID, key := range state.Heads {
		receipt, ok := state.Receipts[key]
		if !ok || receipt.SandboxID != sandboxID {
			return fmt.Errorf("binding head %q references invalid receipt %q", sandboxID, key)
		}
		binding, bound := state.Bindings[sandboxID]
		if receipt.Operation == "bind" || receipt.Operation == "replace" {
			if !bound || binding.IdempotencyKey != key || binding.Generation != receipt.ActionGeneration || BindingRequestDigest(binding) != receipt.RequestDigest {
				return fmt.Errorf("binding head %q does not match current binding state", sandboxID)
			}
		} else if bound {
			return fmt.Errorf("release head %q conflicts with current binding state", sandboxID)
		}
	}
	for key, receipt := range state.Receipts {
		if _, exists := state.Heads[receipt.SandboxID]; !exists {
			return fmt.Errorf("receipt %q has no sandbox head", key)
		}
	}
	return nil
}

func cloneState(state State) State {
	result := emptyState()
	for key, binding := range state.Bindings {
		binding.DeviceIDs = append([]string(nil), binding.DeviceIDs...)
		result.Bindings[key] = binding
	}
	for key, receipt := range state.Receipts {
		result.Receipts[key] = receipt
	}
	for sandboxID, key := range state.Heads {
		result.Heads[sandboxID] = key
	}
	return result
}

func WriteBindingsCSV(w io.Writer, state State) error {
	writer := csv.NewWriter(w)
	ids := make([]string, 0, len(state.Bindings))
	for id := range state.Bindings {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		binding := state.Bindings[id]
		devices := append([]string(nil), binding.DeviceIDs...)
		sort.Strings(devices)
		record := []string{binding.SandboxID, binding.BindingID, strconv.FormatUint(binding.Generation, 10), strings.Join(devices, ";"), strconv.FormatFloat(binding.Share, 'g', -1, 64), binding.IdempotencyKey}
		if binding.MPSServerPID > 0 {
			record = append(record, strconv.FormatUint(uint64(binding.MPSServerPID), 10))
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func WriteReceiptsCSV(w io.Writer, state State) error {
	writer := csv.NewWriter(w)
	activeTransactions := make(map[string]bool)
	for _, receipt := range state.Receipts {
		if !receipt.Recoverable {
			continue
		}
		key := transactionKey(receipt)
		if _, exists := activeTransactions[key]; !exists {
			activeTransactions[key] = true
		}
		head, exists := state.Receipts[state.Heads[receipt.SandboxID]]
		if !exists || !head.Recoverable || transactionKey(head) != key {
			activeTransactions[key] = false
		}
	}
	receipts := make([]Receipt, 0, len(state.Receipts))
	for _, receipt := range state.Receipts {
		if !receipt.Recoverable {
			continue
		}
		if !activeTransactions[transactionKey(receipt)] {
			receipt.Succeeded = false
			receipt.ErrorCode = supersededErrorCode
			receipt.ErrorMessage = "binding state no longer matches this transaction"
		}
		receipts = append(receipts, receipt)
	}
	sort.Slice(receipts, func(i, j int) bool {
		if receipts[i].PlanID != receipts[j].PlanID {
			return receipts[i].PlanID < receipts[j].PlanID
		}
		if receipts[i].TransactionGeneration != receipts[j].TransactionGeneration {
			return receipts[i].TransactionGeneration < receipts[j].TransactionGeneration
		}
		if receipts[i].StepIndex != receipts[j].StepIndex {
			return receipts[i].StepIndex < receipts[j].StepIndex
		}
		return receipts[i].IdempotencyKey < receipts[j].IdempotencyKey
	})
	for _, receipt := range receipts {
		if err := writer.Write([]string{strconv.Itoa(receipt.StepIndex), strconv.Itoa(receipt.ExpectedActions), strconv.FormatBool(receipt.Committed), receipt.PlanDigest, receipt.ActionID, receipt.PlanID, receipt.IdempotencyKey, receipt.SandboxID, strconv.FormatUint(receipt.TransactionGeneration, 10), strconv.FormatUint(receipt.ActionGeneration, 10), strconv.FormatBool(receipt.Succeeded), receipt.ErrorCode, receipt.ErrorMessage, receipt.CommandDigest}); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func transactionKey(receipt Receipt) string {
	return receipt.PlanID + "\x00" + strconv.FormatUint(receipt.TransactionGeneration, 10)
}
