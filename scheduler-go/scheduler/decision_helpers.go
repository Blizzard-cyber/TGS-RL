package scheduler

import (
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type workUnit struct{ id string }

type candidateOption struct {
	deviceID string
	unitID   string
	score    float64
	proto    *tgsrlv1.PlacementCandidate
}

type deviceChoice struct {
	deviceID string
	score    float64
}

type deviceChoiceHeap []deviceChoice

// decisionCandidateLimit bounds the audit payload while retaining every
// candidate for small decisions and the selected candidate for every unit in
// large decisions. Candidate evaluation itself remains exhaustive.
const decisionCandidateLimit = 4096

func (h deviceChoiceHeap) Len() int { return len(h) }

func (h deviceChoiceHeap) Less(i, j int) bool {
	return h[i].score > h[j].score || h[i].score == h[j].score && h[i].deviceID < h[j].deviceID
}

func (h deviceChoiceHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *deviceChoiceHeap) Push(value any) { *h = append(*h, value.(deviceChoice)) }

func (h *deviceChoiceHeap) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

func makeAuditCandidate(candidateID, decisionID string, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, now time.Time, unitID, deviceID string, score float64, components map[string]float64, requiresSafePoint bool) *tgsrlv1.PlacementCandidate {
	binding := makeBinding(decisionID, unitID, intent.GetLabels()["runtime_unit_id"], deviceID, intent.GetResourcesPerUnit())
	plan := makePlan(decisionID, stableID("candidate-plan", candidateID), snapshot, intent, now, []*tgsrlv1.Binding{binding}, requiresSafePoint)
	return &tgsrlv1.PlacementCandidate{CandidateId: candidateID, Plan: plan, Score: score, ComponentScores: components}
}

func selectLargeDecisionCandidates(decisionID string, snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, now time.Time, units []workUnit, devices []*tgsrlv1.Device, resources map[string]*deviceResources, capabilityFailures map[string]string, safePoint, requiresSafePoint bool) ([]candidateOption, []*tgsrlv1.CandidateRejection) {
	choices := make(deviceChoiceHeap, 0, len(devices))
	rejections := make([]*tgsrlv1.CandidateRejection, 0)
	request := intent.GetResourcesPerUnit()
	for _, device := range devices {
		deviceID := device.GetDeviceId()
		if rejection := candidateRejectionWithCapabilityDetail("", device, resources[deviceID], request, safePoint, capabilityFailures[deviceID]); rejection != nil {
			if len(rejections) < decisionCandidateLimit {
				rejection.CandidateId = stableID("candidate", units[0].id, deviceID, strconv.FormatUint(snapshot.GetRevision(), 10), strconv.FormatUint(intent.GetVersion(), 10))
				rejections = append(rejections, rejection)
			}
			continue
		}
		choices = append(choices, deviceChoice{deviceID: deviceID, score: scoreCandidateValue(resources[deviceID], request)})
	}
	heap.Init(&choices)
	selected := make([]candidateOption, 0, len(units))
	for _, unit := range units {
		if len(choices) == 0 {
			return selected, rejections
		}
		choice := heap.Pop(&choices).(deviceChoice)
		components, score := scoreCandidate(resources[choice.deviceID], request)
		candidateID := stableID("candidate", unit.id, choice.deviceID, strconv.FormatUint(snapshot.GetRevision(), 10), strconv.FormatUint(intent.GetVersion(), 10))
		option := candidateOption{deviceID: choice.deviceID, unitID: unit.id, score: score}
		option.proto = makeAuditCandidate(candidateID, decisionID, snapshot, intent, now, unit.id, choice.deviceID, score, components, requiresSafePoint)
		selected = append(selected, option)
		consume(resources[choice.deviceID], request)
		index := sort.Search(len(devices), func(i int) bool { return devices[i].GetDeviceId() >= choice.deviceID })
		if index < len(devices) && devices[index].GetDeviceId() == choice.deviceID && candidateRejectionWithCapabilityDetail("", devices[index], resources[choice.deviceID], request, safePoint, capabilityFailures[choice.deviceID]) == nil {
			heap.Push(&choices, deviceChoice{deviceID: choice.deviceID, score: scoreCandidateValue(resources[choice.deviceID], request)})
		}
	}
	return selected, rejections
}

func (s *Scheduler) fallbackResult(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, now time.Time, record *tgsrlv1.DecisionRecord, reason string) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	planID := stableID("fallback-plan", record.GetDecisionId(), string(s.fallback), reason)
	bindings := []*tgsrlv1.Binding(nil)
	if s.fallback == FallbackStatic {
		bindings = staticBindings(record.GetDecisionId(), snapshot, intent)
	}
	plan := makePlan(record.GetDecisionId(), planID, snapshot, intent, now, bindings, false)
	// A fallback never authorizes a mutation, including when static bindings are
	// included to describe the state being held.
	plan.Actions = nil
	if !now.Before(intent.GetValidUntil().AsTime()) {
		plan.ExpiresAt = timestamppb.New(now)
	}
	record.Fallback = true
	record.FallbackReason = reason
	record.Score = 0
	record.SelectedPlan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	sortDecisionSurface(record)
	return proto.Clone(plan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
}

func sortDecisionSurface(record *tgsrlv1.DecisionRecord) {
	sort.SliceStable(record.Candidates, func(i, j int) bool {
		if record.Candidates[i].GetScore() != record.Candidates[j].GetScore() {
			return record.Candidates[i].GetScore() > record.Candidates[j].GetScore()
		}
		leftDevice, leftUnit := candidateSortKeys(record.Candidates[i])
		rightDevice, rightUnit := candidateSortKeys(record.Candidates[j])
		if leftDevice != rightDevice {
			return leftDevice < rightDevice
		}
		if leftUnit != rightUnit {
			return leftUnit < rightUnit
		}
		return record.Candidates[i].GetCandidateId() < record.Candidates[j].GetCandidateId()
	})
	sort.SliceStable(record.RejectedCandidates, func(i, j int) bool {
		left, right := record.RejectedCandidates[i], record.RejectedCandidates[j]
		if left.GetCandidateId() != right.GetCandidateId() {
			return left.GetCandidateId() < right.GetCandidateId()
		}
		if left.GetReason() != right.GetReason() {
			return left.GetReason() < right.GetReason()
		}
		return left.GetDetail() < right.GetDetail()
	})
}

func candidateSortKeys(candidate *tgsrlv1.PlacementCandidate) (string, string) {
	if candidate == nil || candidate.GetPlan() == nil || len(candidate.GetPlan().GetBindings()) == 0 {
		return "", ""
	}
	binding := candidate.GetPlan().GetBindings()[0]
	deviceID := ""
	if len(binding.GetDeviceIds()) > 0 {
		deviceID = binding.GetDeviceIds()[0]
	}
	return deviceID, binding.GetPendingUnitId()
}

func stableID(prefix string, parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(strconv.Itoa(len(part))))
		_, _ = hash.Write([]byte{':'})
		_, _ = hash.Write([]byte(part))
	}
	return prefix + "-" + hex.EncodeToString(hash.Sum(nil)[:12])
}
