// Package candidates implements the scheduler-independent candidate engine.
package candidates

import (
	"container/heap"
	"fmt"
	"sort"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/constraints"
	"google.golang.org/protobuf/proto"
)

const (
	// DefaultEvidenceBudget bounds the combined candidate and rejection audit
	// surface. The effective budget grows when necessary to retain every
	// selected candidate.
	DefaultEvidenceBudget = 4096

	FallbackNoCandidate      = "NO_CANDIDATE"
	FallbackInvalidSelection = "INVALID_SELECTION"
)

// Config controls the policy shortlist and the independently bounded audit
// surface. TopK is applied after every pair in a unit's feasible surface has
// been evaluated; it never limits pair evaluation.
type Config struct {
	TopK           int
	EvidenceBudget int
}

// DecisionMetadata contains decision facts needed during candidate
// construction and hard-constraint evaluation.
type DecisionMetadata struct {
	DecisionID        string
	RequiresSafePoint bool
	SafePoint         bool
}

// Unit identifies one placement unit. Pending may be nil for a synthetic unit
// derived from an intent. When present, its resource and capability requests
// take precedence over the intent defaults.
type Unit struct {
	ID      string
	Pending *tgsrlv1.PendingUnit
}

// CandidateInput is passed to the injected plan/candidate factory. The engine
// overwrites CandidateId, Score, and ComponentScores on the returned proto so
// those fields always reflect the authoritative pair evaluation.
type CandidateInput struct {
	Snapshot             *tgsrlv1.ClusterSnapshot
	Intent               *tgsrlv1.SchedulingIntent
	Decision             DecisionMetadata
	Unit                 Unit
	Device               *tgsrlv1.Device
	RequestedResources   *tgsrlv1.ResourceVector
	RequiredCapabilities *tgsrlv1.CapabilitySet
	CandidateID          string
	Score                float64
	Components           map[string]float64
}

// CandidateIDFunc constructs the stable identity shared by feasible and
// rejected evidence for a unit/device pair.
type CandidateIDFunc func(unitID, deviceID string) string

// CandidateFactory constructs the policy-visible candidate, including its
// one-binding plan when required by the caller.
type CandidateFactory func(CandidateInput) *tgsrlv1.PlacementCandidate

// SelectFunc receives the score-first TopK shortlist for one unit and may
// apply its policy-specific order within that shortlist. A non-nil result must
// be the same pointer as one of candidates.
type SelectFunc func(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, unitID string, candidates []*tgsrlv1.PlacementCandidate, topK int) (selected *tgsrlv1.PlacementCandidate, fallback string)

// Request contains all immutable inputs needed by Engine. CandidateID and
// BuildCandidate have deterministic minimal defaults, but scheduler callers
// normally inject both so IDs and plans share the outer decision contract.
type Request struct {
	Snapshot       *tgsrlv1.ClusterSnapshot
	Intent         *tgsrlv1.SchedulingIntent
	Decision       DecisionMetadata
	Units          []Unit
	Select         SelectFunc
	CandidateID    CandidateIDFunc
	BuildCandidate CandidateFactory
	Config         Config
}

// Option identifies a selected candidate without requiring callers to inspect
// its injected plan shape. Candidate is the exact object passed to Select.
type Option struct {
	Candidate *tgsrlv1.PlacementCandidate
	UnitID    string
	DeviceID  string
}

// Result is the candidate-engine projection. Candidates and Rejections are a
// bounded audit sample, not a claim that the complete pair surface is present.
// The total fields describe the actually evaluated units, including a failure
// unit when selection stops early.
type Result struct {
	Selected            []Option
	Candidates          []*tgsrlv1.PlacementCandidate
	Rejections          []*tgsrlv1.CandidateRejection
	FailureUnit         string
	Fallback            string
	TotalCandidateCount uint64
	TotalRejectionCount uint64
	EvidenceTruncated   bool
}

// Engine evaluates, ranks, selects, and consumes candidates. It has no mutable
// state and is safe to reuse across decisions.
type Engine struct{}

// Evaluate runs one candidate engine with two traversal strategies sharing the
// same pair evaluator, scorer, comparator, resource ledger, and evidence
// collector. Score-first uses a frontier heap; an injected policy receives the
// stable score-first TopK shortlist after the complete feasible surface has
// been evaluated. Heterogeneous unit requirements rebuild the same frontier.
func (Engine) Evaluate(request Request) (Result, error) {
	if request.Snapshot == nil {
		return Result{}, fmt.Errorf("candidates: snapshot must not be nil")
	}
	if request.Intent == nil {
		return Result{}, fmt.Errorf("candidates: intent must not be nil")
	}

	// The engine never mutates request inputs. Expensive full-graph clones are
	// created only at injected callback boundaries; the built-in score-first
	// path has no external callback and remains allocation-light.
	snapshot := request.Snapshot
	intent := request.Intent
	factoryRequest := request
	var selectorSnapshot *tgsrlv1.ClusterSnapshot
	var selectorIntent *tgsrlv1.SchedulingIntent
	if request.Select != nil {
		selectorSnapshot = proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
		selectorIntent = proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
	}

	config := normalizeConfig(request.Config)
	units, err := normalizeUnits(request.Units)
	if err != nil {
		return Result{}, err
	}
	devices, err := normalizeDevices(snapshot.GetDevices())
	if err != nil {
		return Result{}, err
	}
	idFactory := request.CandidateID
	if idFactory == nil {
		idFactory = defaultCandidateID(snapshot, intent)
	}
	candidateFactory := request.BuildCandidate
	cloneFactoryOutput := candidateFactory != nil
	if candidateFactory == nil {
		candidateFactory = defaultCandidateFactory
	} else {
		// One callback-owned graph is enough to isolate the engine ledger. It is
		// intentionally not cloned per pair, which would turn a 1000x1000
		// score-first decision into millions of protobuf allocations.
		factoryRequest.Snapshot = proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
		factoryRequest.Intent = proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
	}

	states := buildDeviceStates(snapshot, devices)
	retainFeasibleEvidence := request.Select != nil || pairSurfaceFitsBudget(len(units), len(states), config.EvidenceBudget)
	evidenceCapacity := config.EvidenceBudget
	if !retainFeasibleEvidence && len(units) < evidenceCapacity {
		evidenceCapacity = len(units)
	}
	collector := newEvidenceCollector(config.EvidenceBudget, evidenceCapacity)
	result := Result{Selected: make([]Option, 0, len(units))}
	if len(units) == 0 {
		return result, nil
	}

	var frontier deviceFrontier
	var evaluatedRequest *tgsrlv1.ResourceVector
	var evaluatedCapabilities *tgsrlv1.CapabilitySet
	feasibleCount := 0

	for _, unit := range units {
		unitRequest, unitCapabilities := requirementsFor(unit, intent)
		requestChanged := evaluatedRequest != unitRequest && !proto.Equal(evaluatedRequest, unitRequest)
		capabilitiesChanged := evaluatedCapabilities != unitCapabilities && !proto.Equal(evaluatedCapabilities, unitCapabilities)
		if frontier == nil || requestChanged || capabilitiesChanged {
			frontier = rebuildFrontier(states, intent, unitRequest, unitCapabilities, request.Decision)
			feasibleCount = frontier.Len()
			evaluatedRequest = unitRequest
			evaluatedCapabilities = unitCapabilities
		}

		result.TotalCandidateCount = saturatingCountAdd(result.TotalCandidateCount, feasibleCount)
		result.TotalRejectionCount = saturatingCountAdd(result.TotalRejectionCount, len(states)-feasibleCount)

		var (
			materialized map[*deviceState]*tgsrlv1.PlacementCandidate
			choices      []frontierChoice
			choice       frontierChoice
			selected     *tgsrlv1.PlacementCandidate
			fallback     string
			choiceIndex  = -1
		)
		if request.Select == nil && frontier.Len() > 0 {
			state := heap.Pop(&frontier).(*deviceState)
			candidate, buildErr := materializeCandidate(factoryRequest, unit, state, unitRequest, unitCapabilities, idFactory, candidateFactory, cloneFactoryOutput)
			if buildErr != nil {
				return Result{}, buildErr
			}
			choice = frontierChoice{state: state, candidate: candidate}
			selected = candidate
			if retainFeasibleEvidence {
				materialized = map[*deviceState]*tgsrlv1.PlacementCandidate{state: candidate}
			}
		} else if request.Select != nil {
			shortlistSize := config.TopK
			if frontier.Len() < shortlistSize {
				shortlistSize = frontier.Len()
			}
			materialized = make(map[*deviceState]*tgsrlv1.PlacementCandidate, shortlistSize)
			choices = make([]frontierChoice, 0, shortlistSize)
			policyInput := make([]*tgsrlv1.PlacementCandidate, 0, shortlistSize)
			for range shortlistSize {
				state := heap.Pop(&frontier).(*deviceState)
				candidate, buildErr := materializeCandidate(factoryRequest, unit, state, unitRequest, unitCapabilities, idFactory, candidateFactory, cloneFactoryOutput)
				if buildErr != nil {
					return Result{}, buildErr
				}
				materialized[state] = candidate
				choices = append(choices, frontierChoice{state: state, candidate: candidate})
				policyInput = append(policyInput, candidate)
			}
			if len(policyInput) > 0 {
				selected, fallback = request.Select(selectorSnapshot, selectorIntent, unit.ID, policyInput, config.TopK)
				choiceIndex = selectedChoiceIndex(choices, selected)
				if choiceIndex >= 0 {
					choice = choices[choiceIndex]
				}
			}
		}

		if selected == nil && len(choices) == 0 && choice.candidate == nil {
			if retainFeasibleEvidence || feasibleCount < len(states) {
				if collectErr := collectUnitEvidence(collector, factoryRequest, unit, states, materialized, unitRequest, unitCapabilities, idFactory, candidateFactory, cloneFactoryOutput, retainFeasibleEvidence); collectErr != nil {
					return Result{}, collectErr
				}
			}
			result.FailureUnit = unit.ID
			result.Fallback = FallbackNoCandidate
			break
		}

		if request.Select != nil && choiceIndex < 0 {
			if retainFeasibleEvidence || feasibleCount < len(states) {
				if collectErr := collectUnitEvidence(collector, factoryRequest, unit, states, materialized, unitRequest, unitCapabilities, idFactory, candidateFactory, cloneFactoryOutput, retainFeasibleEvidence); collectErr != nil {
					return Result{}, collectErr
				}
			}
			result.FailureUnit = unit.ID
			if selected != nil {
				result.Fallback = FallbackInvalidSelection
			} else if fallback != "" {
				result.Fallback = fallback
			} else {
				result.Fallback = FallbackNoCandidate
			}
			break
		}

		selectedOption := Option{Candidate: choice.candidate, UnitID: unit.ID, DeviceID: choice.state.device.GetDeviceId()}
		collector.addSelected(selectedOption)
		if retainFeasibleEvidence || feasibleCount < len(states) {
			if collectErr := collectUnitEvidence(collector, factoryRequest, unit, states, materialized, unitRequest, unitCapabilities, idFactory, candidateFactory, cloneFactoryOutput, retainFeasibleEvidence); collectErr != nil {
				return Result{}, collectErr
			}
		}
		result.Selected = append(result.Selected, selectedOption)

		if request.Select != nil {
			for index, current := range choices {
				if index != choiceIndex {
					heap.Push(&frontier, current.state)
				}
			}
		}

		consume(choice.state.resources, unitRequest)
		choice.state.evaluation = evaluatePair(intent, choice.state.device, choice.state.resources, unitRequest, unitCapabilities, request.Decision)
		if choice.state.evaluation.feasible {
			heap.Push(&frontier, choice.state)
		} else {
			feasibleCount--
		}
	}

	result.Candidates, result.Rejections = collector.project()
	recorded := uint64(len(result.Candidates) + len(result.Rejections))
	total := saturatingUintAdd(result.TotalCandidateCount, result.TotalRejectionCount)
	result.EvidenceTruncated = recorded < total
	return result, nil
}

func normalizeConfig(config Config) Config {
	if config.TopK <= 0 {
		config.TopK = 1
	}
	if config.EvidenceBudget <= 0 {
		config.EvidenceBudget = DefaultEvidenceBudget
	}
	return config
}

func normalizeUnits(input []Unit) ([]Unit, error) {
	units := append([]Unit(nil), input...)
	seen := make(map[string]struct{}, len(units))
	pending := make([]*tgsrlv1.PendingUnit, len(units))
	byPending := make(map[*tgsrlv1.PendingUnit]Unit, len(units))
	for index := range units {
		if units[index].ID == "" && units[index].Pending != nil {
			units[index].ID = units[index].Pending.GetPendingUnitId()
		}
		if units[index].ID == "" {
			return nil, fmt.Errorf("candidates: unit %d has an empty ID", index)
		}
		if _, exists := seen[units[index].ID]; exists {
			return nil, fmt.Errorf("candidates: duplicate unit ID %q", units[index].ID)
		}
		if units[index].Pending != nil && units[index].Pending.GetPendingUnitId() != units[index].ID {
			return nil, fmt.Errorf("candidates: unit ID %q does not match pending unit ID %q", units[index].ID, units[index].Pending.GetPendingUnitId())
		}
		if units[index].Pending != nil {
			units[index].Pending = proto.Clone(units[index].Pending).(*tgsrlv1.PendingUnit)
		}
		seen[units[index].ID] = struct{}{}
		pending[index] = units[index].Pending
		if pending[index] == nil {
			pending[index] = &tgsrlv1.PendingUnit{PendingUnitId: units[index].ID}
		}
		byPending[pending[index]] = units[index]
	}
	ordered := constraints.StableUnitOrder(pending)
	for index, current := range ordered {
		units[index] = byPending[current]
	}
	return units, nil
}

func normalizeDevices(input []*tgsrlv1.Device) ([]*tgsrlv1.Device, error) {
	devices := append([]*tgsrlv1.Device(nil), input...)
	seen := make(map[string]struct{}, len(devices))
	for index, device := range devices {
		if device == nil {
			return nil, fmt.Errorf("candidates: device %d is nil", index)
		}
		if device.GetDeviceId() == "" {
			return nil, fmt.Errorf("candidates: device %d has an empty ID", index)
		}
		if _, exists := seen[device.GetDeviceId()]; exists {
			return nil, fmt.Errorf("candidates: duplicate device ID %q", device.GetDeviceId())
		}
		seen[device.GetDeviceId()] = struct{}{}
	}
	sort.Slice(devices, func(i, j int) bool { return devices[i].GetDeviceId() < devices[j].GetDeviceId() })
	return devices, nil
}

func requirementsFor(unit Unit, intent *tgsrlv1.SchedulingIntent) (*tgsrlv1.ResourceVector, *tgsrlv1.CapabilitySet) {
	resources := intent.GetResourcesPerUnit()
	capabilities := intent.GetRequiredCapabilities()
	if unit.Pending != nil {
		if unit.Pending.GetRequestedResources() != nil {
			resources = unit.Pending.GetRequestedResources()
		}
		if unit.Pending.GetRequiredCapabilities() != nil {
			capabilities = unit.Pending.GetRequiredCapabilities()
		}
	}
	return resources, capabilities
}

type frontierChoice struct {
	state     *deviceState
	candidate *tgsrlv1.PlacementCandidate
}

func selectedChoiceIndex(choices []frontierChoice, selected *tgsrlv1.PlacementCandidate) int {
	if selected == nil {
		return -1
	}
	for index := range choices {
		if choices[index].candidate == selected {
			return index
		}
	}
	return -1
}

func rebuildFrontier(states []*deviceState, intent *tgsrlv1.SchedulingIntent, request *tgsrlv1.ResourceVector, required *tgsrlv1.CapabilitySet, decision DecisionMetadata) deviceFrontier {
	frontier := make(deviceFrontier, 0, len(states))
	for _, state := range states {
		state.evaluation = evaluatePair(intent, state.device, state.resources, request, required, decision)
		if state.evaluation.feasible {
			frontier = append(frontier, state)
		}
	}
	heap.Init(&frontier)
	return frontier
}

func materializeCandidate(request Request, unit Unit, state *deviceState, resources *tgsrlv1.ResourceVector, capabilities *tgsrlv1.CapabilitySet, idFactory CandidateIDFunc, factory CandidateFactory, cloneOutput bool) (*tgsrlv1.PlacementCandidate, error) {
	id := idFactory(unit.ID, state.device.GetDeviceId())
	if id == "" {
		return nil, fmt.Errorf("candidates: candidate ID is empty for unit %q and device %q", unit.ID, state.device.GetDeviceId())
	}
	components := state.evaluation.score.components()
	if !cloneOutput {
		candidate := defaultCandidateFactory(CandidateInput{
			Unit:               unit,
			Device:             state.device,
			RequestedResources: resources,
		})
		candidate.CandidateId = id
		candidate.Score = state.evaluation.score.total
		candidate.ComponentScores = components
		if err := validateCandidatePlan(candidate, unit.ID, state.device.GetDeviceId()); err != nil {
			return nil, err
		}
		return candidate, nil
	}
	factoryUnit := unit
	if unit.Pending != nil {
		factoryUnit.Pending = proto.Clone(unit.Pending).(*tgsrlv1.PendingUnit)
	}
	produced := factory(CandidateInput{
		Snapshot:             request.Snapshot,
		Intent:               request.Intent,
		Decision:             request.Decision,
		Unit:                 factoryUnit,
		Device:               proto.Clone(state.device).(*tgsrlv1.Device),
		RequestedResources:   cloneOptionalResources(resources),
		RequiredCapabilities: cloneCapabilities(capabilities),
		CandidateID:          id,
		Score:                state.evaluation.score.total,
		Components:           cloneComponents(components),
	})
	if produced == nil {
		return nil, fmt.Errorf("candidates: candidate factory returned nil for unit %q and device %q", unit.ID, state.device.GetDeviceId())
	}
	candidate := produced
	if cloneOutput {
		// Injected factories may reuse a scratch proto. Every evaluated pair
		// must retain a distinct identity for policy and evidence surfaces.
		candidate = proto.Clone(produced).(*tgsrlv1.PlacementCandidate)
	}
	candidate.CandidateId = id
	candidate.Score = state.evaluation.score.total
	candidate.ComponentScores = components
	if err := validateCandidatePlan(candidate, unit.ID, state.device.GetDeviceId()); err != nil {
		return nil, err
	}
	return candidate, nil
}

func validateCandidatePlan(candidate *tgsrlv1.PlacementCandidate, unitID, deviceID string) error {
	if candidate.GetPlan() == nil || len(candidate.GetPlan().GetBindings()) != 1 {
		return fmt.Errorf("candidates: candidate factory must build exactly one binding for unit %q and device %q", unitID, deviceID)
	}
	binding := candidate.GetPlan().GetBindings()[0]
	if binding == nil || binding.GetPendingUnitId() != unitID || len(binding.GetDeviceIds()) != 1 || binding.GetDeviceIds()[0] != deviceID {
		return fmt.Errorf("candidates: candidate factory binding does not match unit %q and device %q", unitID, deviceID)
	}
	return nil
}

func defaultCandidateFactory(input CandidateInput) *tgsrlv1.PlacementCandidate {
	return &tgsrlv1.PlacementCandidate{
		Plan: &tgsrlv1.PlacementPlan{Bindings: []*tgsrlv1.Binding{{
			PendingUnitId: input.Unit.ID,
			DeviceIds:     []string{input.Device.GetDeviceId()},
			Resources:     cloneResources(input.RequestedResources),
		}}},
	}
}

func collectUnitEvidence(collector *evidenceCollector, request Request, unit Unit, states []*deviceState, materialized map[*deviceState]*tgsrlv1.PlacementCandidate, resources *tgsrlv1.ResourceVector, capabilities *tgsrlv1.CapabilitySet, idFactory CandidateIDFunc, factory CandidateFactory, cloneOutput, retainFeasible bool) error {
	if !collector.hasRegularCapacity() {
		return nil
	}
	for _, state := range states {
		if !collector.hasRegularCapacity() {
			break
		}
		if state.evaluation.feasible {
			if !retainFeasible {
				continue
			}
			candidate := materialized[state]
			if candidate == nil {
				var err error
				candidate, err = materializeCandidate(request, unit, state, resources, capabilities, idFactory, factory, cloneOutput)
				if err != nil {
					return err
				}
				materialized[state] = candidate
			}
			collector.addCandidate(Option{Candidate: candidate, UnitID: unit.ID, DeviceID: state.device.GetDeviceId()})
			continue
		}
		id := idFactory(unit.ID, state.device.GetDeviceId())
		if id == "" {
			return fmt.Errorf("candidates: candidate ID is empty for unit %q and device %q", unit.ID, state.device.GetDeviceId())
		}
		collector.addRejection(unit.ID, state.device.GetDeviceId(), &tgsrlv1.CandidateRejection{
			CandidateId: id,
			Reason:      state.evaluation.reason,
			Detail:      state.evaluation.detail,
		})
	}
	return nil
}

func pairSurfaceFitsBudget(unitCount, deviceCount, budget int) bool {
	if unitCount <= 0 || deviceCount <= 0 {
		return true
	}
	return deviceCount <= budget/unitCount
}

func cloneComponents(input map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func saturatingCountAdd(current uint64, increment int) uint64 {
	if increment <= 0 {
		return current
	}
	return saturatingUintAdd(current, uint64(increment))
}

func saturatingUintAdd(left, right uint64) uint64 {
	if ^uint64(0)-left < right {
		return ^uint64(0)
	}
	return left + right
}
