// Package scheduler implements the deterministic, provider-independent scheduling
// decision core. It deliberately depends only on generated protocol DTOs.
package scheduler

import (
	"sort"
	"strconv"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Evaluate deterministically evaluates a cloned snapshot/intent pair and
// returns independent PlacementPlan and DecisionRecord object graphs. It never
// mutates, retains, or aliases either input.
func (s *Scheduler) Evaluate(snapshot *tgsrlv1.ClusterSnapshot, intent *tgsrlv1.SchedulingIntent) (*tgsrlv1.PlacementPlan, *tgsrlv1.DecisionRecord, error) {
	if s == nil {
		return nil, nil, &ValidationError{Field: "scheduler", Reason: "must not be nil"}
	}
	if snapshot == nil {
		return nil, nil, &ValidationError{Field: "snapshot", Reason: "must not be nil"}
	}
	if intent == nil {
		return nil, nil, &ValidationError{Field: "intent", Reason: "must not be nil"}
	}

	// Clone before validation so even getters and future normalization logic are
	// isolated from concurrent caller mutation.
	snapshot = proto.Clone(snapshot).(*tgsrlv1.ClusterSnapshot)
	intent = proto.Clone(intent).(*tgsrlv1.SchedulingIntent)
	now := s.clock.Now().UTC()
	if now.IsZero() {
		return nil, nil, &ValidationError{Field: "clock", Reason: "returned zero time"}
	}
	nowTimestamp := timestamppb.New(now)
	if err := nowTimestamp.CheckValid(); err != nil {
		return nil, nil, &ValidationError{Field: "clock", Reason: err.Error()}
	}
	if err := validateSnapshot(snapshot); err != nil {
		return nil, nil, err
	}
	if err := validateIntent(intent); err != nil {
		return nil, nil, err
	}

	sequence := s.nextSequence()
	decisionID := stableID("decision", snapshot.GetSnapshotId(), strconv.FormatUint(snapshot.GetRevision(), 10), intent.GetExecutionId(), intent.GetStageId(), strconv.FormatUint(intent.GetVersion(), 10), strconv.FormatUint(sequence, 10))
	record := &tgsrlv1.DecisionRecord{
		DecisionId:        decisionID,
		Sequence:          sequence,
		ExecutionId:       intent.GetExecutionId(),
		StageId:           intent.GetStageId(),
		IntentVersion:     intent.GetVersion(),
		SnapshotRevision:  snapshot.GetRevision(),
		DataKind:          s.resolveDataKind(snapshot, intent),
		CodeRevision:      s.codeRevision,
		DecidedAt:         nowTimestamp,
		PolicyVersion:     intent.GetPolicyVersion(),
		DeterministicSeed: intent.GetDeterministicSeed(),
		ConfigRevision:    s.configRevision,
		JobId:             intent.GetJobId(),
		RunId:             intent.GetRunId(),
		TraceId:           intent.GetTraceId(),
		SemanticContext:   cloneSemanticEnvelope(intent.GetSemanticContext()),
		Generation:        intent.GetGeneration(),
		Cursor:            intent.GetCursor(),
	}

	if !now.Before(intent.GetValidUntil().AsTime()) {
		record.RejectedCandidates = []*tgsrlv1.CandidateRejection{{
			CandidateId: stableID("expired", intent.GetExecutionId(), intent.GetStageId(), strconv.FormatUint(intent.GetVersion(), 10)),
			Reason:      tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_EXPIRED,
			Detail:      "intent valid_until is not after evaluation time",
		}}
		return s.fallbackResult(snapshot, intent, now, record, FallbackReasonIntentExpired)
	}

	units, fallbackReason, fallbackDetail := workUnits(snapshot, intent)
	if fallbackReason != "" {
		record.RejectedCandidates = []*tgsrlv1.CandidateRejection{{
			CandidateId: stableID("pending-version", intent.GetExecutionId(), intent.GetStageId()),
			Reason:      tgsrlv1.CandidateRejectionReason_CANDIDATE_REJECTION_REASON_VERSION_CONSTRAINT,
			Detail:      fallbackDetail,
		}}
		return s.fallbackResult(snapshot, intent, now, record, fallbackReason)
	}
	if s.policy != nil && (s.policy.Name() == "noop" || s.policy.Name() == "static") {
		_, reason := s.policy.Choose(snapshot, intent, nil)
		return s.fallbackResult(snapshot, intent, now, record, "POLICY_"+reason)
	}

	devices := append([]*tgsrlv1.Device(nil), snapshot.GetDevices()...)
	sort.Slice(devices, func(i, j int) bool { return devices[i].GetDeviceId() < devices[j].GetDeviceId() })
	resources := buildDeviceResources(snapshot, devices)
	requiresSafePoint := contractRequiresSafePoint(intent.GetExecutionContract())
	safePoint := !requiresSafePoint || snapshotAtSafePoint(snapshot, intent, s.safePointKey)
	auditAllCandidates := len(units) == 0 || len(devices) <= decisionCandidateLimit/len(units) || s.policy != nil && s.policy.Name() != "score_first"
	capabilityFailures := make(map[string]string, len(devices))
	for _, device := range devices {
		capabilityFailures[device.GetDeviceId()] = capabilityMismatch(device.GetCapabilities(), intent.GetRequiredCapabilities())
	}

	selected := make([]candidateOption, 0, len(units))
	if !auditAllCandidates {
		selected, record.RejectedCandidates = selectLargeDecisionCandidates(decisionID, snapshot, intent, now, units, devices, resources, capabilityFailures, safePoint, requiresSafePoint)
		if len(selected) != len(units) {
			sortDecisionSurface(record)
			if len(selected) == 0 && len(units) == 1 {
				if preemptionPlan, fallbackReason := s.preemptionPlan(snapshot, intent, now, record, units[0], safePoint); preemptionPlan != nil {
					guardKey := intent.GetExecutionId() + "/" + intent.GetStageId()
					if guardDecision := s.guard.AllowN(guardKey, 0, len(preemptionPlan.GetActions())); !guardDecision.Allowed {
						return s.fallbackResult(snapshot, intent, now, record, "PROTECTION_"+guardDecision.Reason)
					}
					record.SelectedPlan = proto.Clone(preemptionPlan).(*tgsrlv1.PlacementPlan)
					return proto.Clone(preemptionPlan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
				} else if fallbackReason != "" {
					return s.fallbackResult(snapshot, intent, now, record, fallbackReason)
				}
			}
			reason := FallbackReasonNoCandidate
			if requiresSafePoint && !safePoint {
				reason = FallbackReasonSafePoint
			}
			s.guard.Reject(intent.GetExecutionId() + "/" + intent.GetStageId())
			return s.fallbackResult(snapshot, intent, now, record, reason)
		}
	}
	if auditAllCandidates {
		for _, unit := range units {
			var chosen candidateOption
			hasChoice := false
			options := make([]*candidateOption, 0, len(devices))
			for _, device := range devices {
				candidateID := ""
				if auditAllCandidates {
					candidateID = stableID("candidate", unit.id, device.GetDeviceId(), strconv.FormatUint(snapshot.GetRevision(), 10), strconv.FormatUint(intent.GetVersion(), 10))
				}
				rejection := candidateRejectionWithCapabilityDetail(candidateID, device, resources[device.GetDeviceId()], intent.GetResourcesPerUnit(), safePoint, capabilityFailures[device.GetDeviceId()])
				if rejection != nil {
					if auditAllCandidates || len(record.RejectedCandidates) < decisionCandidateLimit {
						if rejection.GetCandidateId() == "" {
							rejection.CandidateId = stableID("candidate", unit.id, device.GetDeviceId(), strconv.FormatUint(snapshot.GetRevision(), 10), strconv.FormatUint(intent.GetVersion(), 10))
						}
						record.RejectedCandidates = append(record.RejectedCandidates, rejection)
					}
					continue
				}
				var components map[string]float64
				var score float64
				if auditAllCandidates {
					components, score = scoreCandidate(resources[device.GetDeviceId()], intent.GetResourcesPerUnit())
				} else {
					score = scoreCandidateValue(resources[device.GetDeviceId()], intent.GetResourcesPerUnit())
				}
				option := candidateOption{
					deviceID: device.GetDeviceId(),
					unitID:   unit.id,
					score:    score,
				}
				if !hasChoice || option.score > chosen.score || option.score == chosen.score && option.deviceID < chosen.deviceID {
					chosen = option
					hasChoice = true
				}
				if auditAllCandidates {
					option.proto = makeAuditCandidate(candidateID, decisionID, snapshot, intent, now, unit.id, device.GetDeviceId(), score, components, requiresSafePoint)
					record.Candidates = append(record.Candidates, option.proto)
				}
				optionCopy := option
				options = append(options, &optionCopy)
			}
			if !hasChoice {
				sortDecisionSurface(record)
				if len(selected) == 0 && len(units) == 1 {
					if preemptionPlan, fallbackReason := s.preemptionPlan(snapshot, intent, now, record, unit, safePoint); preemptionPlan != nil {
						guardKey := intent.GetExecutionId() + "/" + intent.GetStageId()
						if guardDecision := s.guard.AllowN(guardKey, 0, len(preemptionPlan.GetActions())); !guardDecision.Allowed {
							return s.fallbackResult(snapshot, intent, now, record, "PROTECTION_"+guardDecision.Reason)
						}
						record.SelectedPlan = proto.Clone(preemptionPlan).(*tgsrlv1.PlacementPlan)
						return proto.Clone(preemptionPlan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
					} else if fallbackReason != "" {
						return s.fallbackResult(snapshot, intent, now, record, fallbackReason)
					}
				}
				reason := FallbackReasonNoCandidate
				if requiresSafePoint && !safePoint {
					reason = FallbackReasonSafePoint
				}
				s.guard.Reject(intent.GetExecutionId() + "/" + intent.GetStageId())
				return s.fallbackResult(snapshot, intent, now, record, reason)
			}

			if !auditAllCandidates {
				candidateID := stableID("candidate", unit.id, chosen.deviceID, strconv.FormatUint(snapshot.GetRevision(), 10), strconv.FormatUint(intent.GetVersion(), 10))
				components, score := scoreCandidate(resources[chosen.deviceID], intent.GetResourcesPerUnit())
				chosen.proto = makeAuditCandidate(candidateID, decisionID, snapshot, intent, now, unit.id, chosen.deviceID, score, components, requiresSafePoint)
				record.Candidates = append(record.Candidates, chosen.proto)
			}
			if s.policy != nil && s.policy.Name() != "score_first" {
				if configuredChoice, reason := s.chooseConfiguredCandidate(snapshot, intent, options); configuredChoice != nil {
					chosen = *configuredChoice
				} else if reason != "" {
					return s.fallbackResult(snapshot, intent, now, record, "POLICY_"+reason)
				}
			}
			consume(resources[chosen.deviceID], intent.GetResourcesPerUnit())
			selected = append(selected, chosen)
		}
	}
	sortDecisionSurface(record)
	bindings := make([]*tgsrlv1.Binding, 0, len(selected))
	for _, choice := range selected {
		binding := makeBinding(
			decisionID, choice.unitID, intent.GetLabels()["runtime_unit_id"],
			choice.deviceID, intent.GetResourcesPerUnit(),
		)
		bindings = append(bindings, binding)
	}
	planID := stableID("plan", decisionID, snapshot.GetSnapshotId(), strconv.FormatUint(snapshot.GetRevision(), 10), intent.GetIdempotencyKey())
	plan := makePlan(decisionID, planID, snapshot, intent, now, bindings, requiresSafePoint)
	guardKey := intent.GetExecutionId() + "/" + intent.GetStageId()
	if guardDecision := s.guard.AllowN(guardKey, recordCandidateScore(selected), len(plan.GetActions())); !guardDecision.Allowed {
		return s.fallbackResult(snapshot, intent, now, record, "PROTECTION_"+guardDecision.Reason)
	}
	record.SelectedPlan = proto.Clone(plan).(*tgsrlv1.PlacementPlan)
	for _, choice := range selected {
		record.Score += choice.score
	}
	if len(selected) > 0 {
		record.Score = roundScore(record.Score / float64(len(selected)))
	}
	return proto.Clone(plan).(*tgsrlv1.PlacementPlan), proto.Clone(record).(*tgsrlv1.DecisionRecord), nil
}
