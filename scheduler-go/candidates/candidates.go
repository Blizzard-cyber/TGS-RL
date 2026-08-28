package candidates

import (
	"sort"
	"strconv"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/cache"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/constraints"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EvaluatedCandidate keeps the plan plus scoring metadata before final
// DecisionRecord materialization.
type EvaluatedCandidate struct {
	Candidate  *tgsrlv1.PlacementCandidate
	Rejection  *tgsrlv1.CandidateRejection
	DeviceID   string
	PendingID  string
	Generation uint64
}

// TopK limits candidate enumeration for each tick.
type TopK struct {
	Limit int
}

// Evaluate enumerates and ranks one candidate per feasible unit/device pair.
func (t TopK) Evaluate(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, checks []constraints.Constraint, score func(*tgsrlv1.ClusterSnapshot, *tgsrlv1.PendingUnit, *tgsrlv1.Device) map[string]float64) []EvaluatedCandidate {
	if snapshot == nil || intent == nil {
		return nil
	}
	units := constraints.StableUnitOrder(snapshot.GetPendingUnits())
	out := make([]EvaluatedCandidate, 0, len(units)*len(snapshot.GetDevices()))
	for _, unit := range units {
		if unit.GetExecutionId() != intent.GetExecutionId() || unit.GetStageId() != intent.GetStageId() || unit.GetIntentVersion() != intent.GetVersion() {
			continue
		}
		for _, device := range snapshot.GetDevices() {
			ctx := constraints.Context{Snapshot: snapshot, Intent: intent, Unit: unit, Device: device}
			var rejected error
			for _, check := range checks {
				if check == nil {
					continue
				}
				if err := check.Check(ctx); err != nil {
					rejected = err
					break
				}
			}
			candidateID := cache.StableID("candidate", unit.GetPendingUnitId(), device.GetDeviceId(), strconv.FormatUint(snapshot.GetRevision(), 10), strconv.FormatUint(intent.GetVersion(), 10))
			if rejected != nil {
				reason, _ := rejected.(*constraints.Reason)
				code := tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VALIDITY_RULE
				detail := rejected.Error()
				if reason != nil {
					code = reason.Code
					detail = reason.Detail
				}
				out = append(out, EvaluatedCandidate{
					Rejection: &tgsrlv1.CandidateRejection{CandidateId: candidateID, Reason: code, Detail: detail},
					DeviceID:  device.GetDeviceId(),
					PendingID: unit.GetPendingUnitId(),
				})
				continue
			}
			componentScores := score(snapshot, unit, device)
			total := 0.0
			for _, value := range componentScores {
				total += value
			}
			binding := &tgsrlv1.Binding{
				BindingId:     cache.StableID("binding", unit.GetPendingUnitId(), device.GetDeviceId()),
				PendingUnitId: unit.GetPendingUnitId(),
				DeviceIds:     []string{device.GetDeviceId()},
				Resources:     cloneResources(unit.GetRequestedResources()),
				SandboxId:     cache.StableID("sandbox", unit.GetPendingUnitId(), device.GetDeviceId()),
				Generation:    1,
			}
			planID := cache.StableID("candidate-plan", candidateID)
			plan := &tgsrlv1.PlacementPlan{
				PlanId:           planID,
				ExecutionId:      intent.GetExecutionId(),
				StageId:          intent.GetStageId(),
				IntentVersion:    intent.GetVersion(),
				SnapshotRevision: snapshot.GetRevision(),
				Bindings:         []*tgsrlv1.Binding{cloneBinding(binding)},
				Actions: []*tgsrlv1.Action{{
					ActionId:                 cache.StableID("action", planID, binding.GetBindingId()),
					ActionType:               tgsrlv1.ActionType_ACTION_TYPE_BIND,
					Level:                    tgsrlv1.ActionLevel_ACTION_LEVEL_L1,
					TargetId:                 unit.GetPendingUnitId(),
					Binding:                  cloneBinding(binding),
					Order:                    1,
					PlanId:                   planID,
					SandboxId:                binding.GetSandboxId(),
					ExpectedGeneration:       1,
					ExpectedSnapshotRevision: snapshot.GetRevision(),
					RequiredCapabilities:     cloneCapabilities(intent.GetRequiredCapabilities()),
					Deadline:                 cloneTimestamp(intent.GetValidUntil()),
					IdempotencyKey:           cache.StableID("action-idempotency", intent.GetIdempotencyKey(), unit.GetPendingUnitId()),
					Rollback: &tgsrlv1.Rollback{
						ActionType: tgsrlv1.ActionType_ACTION_TYPE_RELEASE,
						TargetId:   binding.GetBindingId(),
						Reason:     "compensate bind on scheduler evolution plan failure",
					},
				}},
				CreatedAt: timestamppb.New(time.Now().UTC()),
				ExpiresAt: cloneTimestamp(intent.GetValidUntil()),
			}
			out = append(out, EvaluatedCandidate{
				Candidate: &tgsrlv1.PlacementCandidate{
					CandidateId:     candidateID,
					Plan:            plan,
					Score:           total,
					ComponentScores: componentScores,
				},
				DeviceID:   device.GetDeviceId(),
				PendingID:  unit.GetPendingUnitId(),
				Generation: binding.GetGeneration(),
			})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		left, right := out[i], out[j]
		if left.Candidate != nil && right.Candidate != nil {
			if left.Candidate.GetScore() != right.Candidate.GetScore() {
				return left.Candidate.GetScore() > right.Candidate.GetScore()
			}
			return left.Candidate.GetCandidateId() < right.Candidate.GetCandidateId()
		}
		return left.PendingID < right.PendingID
	})
	if t.Limit > 0 {
		accepted := 0
		trimmed := out[:0]
		for _, candidate := range out {
			if candidate.Candidate != nil {
				if accepted >= t.Limit {
					continue
				}
				accepted++
			}
			trimmed = append(trimmed, candidate)
		}
		out = trimmed
	}
	return out
}

func cloneBinding(binding *tgsrlv1.Binding) *tgsrlv1.Binding {
	if binding == nil {
		return nil
	}
	return proto.Clone(binding).(*tgsrlv1.Binding)
}

func cloneResources(resources *tgsrlv1.ResourceVector) *tgsrlv1.ResourceVector {
	if resources == nil {
		return &tgsrlv1.ResourceVector{}
	}
	return proto.Clone(resources).(*tgsrlv1.ResourceVector)
}

func cloneCapabilities(capabilities *tgsrlv1.CapabilitySet) *tgsrlv1.CapabilitySet {
	if capabilities == nil {
		return &tgsrlv1.CapabilitySet{}
	}
	return proto.Clone(capabilities).(*tgsrlv1.CapabilitySet)
}

func cloneTimestamp(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	if ts == nil {
		return nil
	}
	return proto.Clone(ts).(*timestamppb.Timestamp)
}
