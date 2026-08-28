package status

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const authoritativeRuntimeObservationSource = "runtime-observation"

// MergeAggregatedStatus folds source observations into one aggregate record.
func MergeAggregatedStatus(existing []*tgsrlv1.ComponentStatus, incoming *tgsrlv1.ComponentStatus) *tgsrlv1.ComponentStatus {
	component := incoming.GetComponent()
	observationBySource := map[string]*tgsrlv1.ComponentStatus{}
	for _, statusValue := range existing {
		if statusValue.GetComponent() != component {
			continue
		}
		for source, observation := range parseObservationAnnotations(statusValue) {
			observationBySource[source] = observation
		}
	}
	source := incoming.GetSource()
	if source == "" {
		source = "unknown"
	}
	incoming = cloneComponentStatus(incoming)
	incoming.Source = source
	normalizeRuntimeSignal(incoming)
	if current, ok := observationBySource[source]; !ok || isObservationNewer(incoming, current) {
		observationBySource[source] = incoming
	}
	sources := make([]string, 0, len(observationBySource))
	for source := range observationBySource {
		sources = append(sources, source)
	}
	sort.Strings(sources)
	aggregated := &tgsrlv1.ComponentStatus{
		Component:       component,
		Health:          tgsrlv1.ComponentHealth_COMPONENT_HEALTH_UNKNOWN,
		Source:          "aggregated",
		Revision:        incoming.GetRevision(),
		ObservedAt:      incoming.GetObservedAt(),
		Annotations:     map[string]string{},
		JobId:           incoming.GetJobId(),
		RunId:           incoming.GetRunId(),
		TraceId:         incoming.GetTraceId(),
		DataKind:        incoming.GetDataKind(),
		SemanticContext: incoming.GetSemanticContext(),
	}
	details := make([]string, 0, len(sources))
	var latestRuntimeSignalObservation *tgsrlv1.ComponentStatus
	for _, source := range sources {
		observation := observationBySource[source]
		normalizeRuntimeSignal(observation)
		if healthRank(observation.GetHealth()) > healthRank(aggregated.GetHealth()) {
			aggregated.Health = observation.GetHealth()
		}
		if observation.GetRevision() > aggregated.GetRevision() {
			aggregated.Revision = observation.GetRevision()
		}
		if aggregated.GetObservedAt() == nil || observation.GetObservedAt().AsTime().After(aggregated.GetObservedAt().AsTime()) {
			aggregated.ObservedAt = observation.GetObservedAt()
		}
		detail := strings.TrimSpace(observation.GetDetail())
		if detail == "" {
			detail = observation.GetHealth().String()
		}
		details = append(details, fmt.Sprintf("%s=%s", source, detail))
		aggregated.Annotations["observation."+source+".health"] = observation.GetHealth().String()
		aggregated.Annotations["observation."+source+".detail"] = observation.GetDetail()
		aggregated.Annotations["observation."+source+".revision"] = fmt.Sprintf("%d", observation.GetRevision())
		aggregated.Annotations["observation."+source+".observed_at"] = observation.GetObservedAt().AsTime().UTC().Format(time.RFC3339Nano)
		if observation.GetObservedRuntimeState() != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
			aggregated.Annotations["observation."+source+".runtime_state"] = observation.GetObservedRuntimeState().String()
			aggregated.Annotations["observation."+source+".runtime_converged"] = strconv.FormatBool(observation.GetConverged())
			if source == authoritativeRuntimeObservationSource && (latestRuntimeSignalObservation == nil || isObservationNewer(observation, latestRuntimeSignalObservation)) {
				latestRuntimeSignalObservation = observation
			}
		}
	}
	if latestRuntimeSignalObservation != nil {
		aggregated.ObservedRuntimeState = latestRuntimeSignalObservation.GetObservedRuntimeState()
		aggregated.Converged = latestRuntimeSignalObservation.GetConverged()
		aggregated.Annotations["runtime.source"] = latestRuntimeSignalObservation.GetSource()
		aggregated.Annotations["runtime.state"] = aggregated.GetObservedRuntimeState().String()
		aggregated.Annotations["runtime.converged"] = strconv.FormatBool(aggregated.GetConverged())
	}
	aggregated.Detail = strings.Join(details, "; ")
	aggregated.Annotations["aggregation.strategy"] = "retain-latest-per-source-worst-health"
	aggregated.Annotations["observation.count"] = fmt.Sprintf("%d", len(sources))
	aggregated.Annotations["observation.sources"] = strings.Join(sources, ",")
	return aggregated
}

// UpsertAggregatedStatus replaces or appends the aggregate for one component.
func UpsertAggregatedStatus(existing []*tgsrlv1.ComponentStatus, aggregated *tgsrlv1.ComponentStatus) []*tgsrlv1.ComponentStatus {
	result := make([]*tgsrlv1.ComponentStatus, 0, len(existing)+1)
	replaced := false
	for _, statusValue := range existing {
		if statusValue.GetComponent() == aggregated.GetComponent() {
			result = append(result, cloneComponentStatus(aggregated))
			replaced = true
			continue
		}
		result = append(result, cloneComponentStatus(statusValue))
	}
	if !replaced {
		result = append(result, cloneComponentStatus(aggregated))
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].GetComponent() < result[j].GetComponent() })
	return result
}

func parseObservationAnnotations(statusValue *tgsrlv1.ComponentStatus) map[string]*tgsrlv1.ComponentStatus {
	observations := map[string]*tgsrlv1.ComponentStatus{}
	for key, value := range statusValue.GetAnnotations() {
		if !strings.HasPrefix(key, "observation.") {
			continue
		}
		parts := strings.Split(key, ".")
		if len(parts) != 3 {
			continue
		}
		source := parts[1]
		field := parts[2]
		if observations[source] == nil {
			observations[source] = &tgsrlv1.ComponentStatus{
				Component:       statusValue.GetComponent(),
				Source:          source,
				JobId:           statusValue.GetJobId(),
				RunId:           statusValue.GetRunId(),
				TraceId:         statusValue.GetTraceId(),
				DataKind:        statusValue.GetDataKind(),
				SemanticContext: statusValue.GetSemanticContext(),
			}
		}
		switch field {
		case "health":
			if enumValue, ok := tgsrlv1.ComponentHealth_value[value]; ok {
				observations[source].Health = tgsrlv1.ComponentHealth(enumValue)
			}
		case "detail":
			observations[source].Detail = value
		case "revision":
			var revision uint64
			fmt.Sscanf(value, "%d", &revision)
			observations[source].Revision = revision
		case "observed_at":
			if timestamp, err := time.Parse(time.RFC3339Nano, value); err == nil {
				observations[source].ObservedAt = timestamppb.New(timestamp)
			}
		case "runtime_state":
			if enumValue, ok := tgsrlv1.RuntimeState_value[value]; ok {
				observations[source].ObservedRuntimeState = tgsrlv1.RuntimeState(enumValue)
			}
		case "runtime_converged":
			if parsed, err := strconv.ParseBool(value); err == nil {
				observations[source].Converged = parsed
			}
		}
	}
	return observations
}

func normalizeRuntimeSignal(statusValue *tgsrlv1.ComponentStatus) {
	if statusValue == nil || statusValue.GetComponent() != "runtime" {
		return
	}
	if statusValue.GetObservedRuntimeState() != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		return
	}
	rawState := strings.TrimSpace(statusValue.GetAnnotations()["runtime.state"])
	if rawState == "" {
		return
	}
	enumValue, ok := tgsrlv1.RuntimeState_value[rawState]
	if !ok {
		return
	}
	statusValue.ObservedRuntimeState = tgsrlv1.RuntimeState(enumValue)
	if rawConverged := strings.TrimSpace(statusValue.GetAnnotations()["runtime.converged"]); rawConverged != "" {
		if parsed, err := strconv.ParseBool(rawConverged); err == nil {
			statusValue.Converged = parsed
		}
	}
}

func healthRank(health tgsrlv1.ComponentHealth) int {
	switch health {
	case tgsrlv1.ComponentHealth_COMPONENT_HEALTH_FAILED:
		return 5
	case tgsrlv1.ComponentHealth_COMPONENT_HEALTH_DEGRADED:
		return 4
	case tgsrlv1.ComponentHealth_COMPONENT_HEALTH_PROGRESSING:
		return 3
	case tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY:
		return 2
	default:
		return 1
	}
}

func isObservationNewer(candidate, current *tgsrlv1.ComponentStatus) bool {
	if current == nil {
		return true
	}
	if candidate == nil {
		return false
	}
	if candidate.GetRevision() != current.GetRevision() {
		return candidate.GetRevision() > current.GetRevision()
	}
	candidateObserved := candidate.GetObservedAt().AsTime()
	currentObserved := current.GetObservedAt().AsTime()
	if !candidateObserved.Equal(currentObserved) {
		return candidateObserved.After(currentObserved)
	}
	return false
}

func cloneComponentStatus(component *tgsrlv1.ComponentStatus) *tgsrlv1.ComponentStatus {
	if component == nil {
		return nil
	}
	return proto.Clone(component).(*tgsrlv1.ComponentStatus)
}
