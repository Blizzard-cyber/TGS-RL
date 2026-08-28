package scheduler

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/candidates"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type workUnit struct {
	id      string
	pending *tgsrlv1.PendingUnit
}

func (s *Scheduler) fallbackResult(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent, now time.Time, record *tgsrlv1.DecisionRecord, reason string) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	planID := stableID("fallback-plan", record.GetDecisionId(), string(s.fallback), reason)
	bindings := []*tgsrlv1.Binding(nil)
	if s.fallback == FallbackStatic {
		bindings = staticBindings(record.GetDecisionId(), snapshot, intent)
	}
	plan := makePlan(record.GetDecisionId(), planID, snapshot, intent, now, bindings, false, record.GetTickKind())
	// A fallback never authorizes a mutation, including when static bindings are
	// included to describe the state being held.
	plan.Actions = nil
	if !now.Before(intent.GetValidUntil().AsTime()) {
		plan.ExpiresAt = timestamppb.New(now)
	}
	record.Fallback = true
	record.FallbackReason = reason
	record.Score = 0
	if record.GetTotalCandidateCount() == 0 && record.GetTotalRejectedCandidateCount() == 0 {
		record.TotalCandidateCount = uint64(len(record.GetCandidates()))
		record.TotalRejectedCandidateCount = uint64(len(record.GetRejectedCandidates()))
	}
	record.SelectedPlan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	sortDecisionSurface(record)
	return proto.Clone(plan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
}

func sortDecisionSurface(record *tgsrlv1.DecisionRecord) {
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

func recordCandidateScore(selected []candidates.Option) float64 {
	if len(selected) == 0 {
		return 0
	}
	var score float64
	for _, item := range selected {
		score += item.Candidate.GetScore()
	}
	return roundScore(score / float64(len(selected)))
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
