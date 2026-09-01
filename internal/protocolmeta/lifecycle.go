package protocolmeta

import (
	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

// RunRetirementAttribute marks a terminal observation that retires the
// complete Run rather than one Scheduler-selected allocation.
const RunRetirementAttribute = "tgsrl.io/run-retirement"

// SetRunRetirement records whether a terminal observation belongs to a whole
// Run lifecycle command. Scheduler-owned scale-in uses false.
func SetRunRetirement(event *tgsrlv1.SandboxEvent, retire bool) {
	if event == nil {
		return
	}
	if event.SemanticContext == nil {
		event.SemanticContext = &tgsrlv1.SemanticEnvelope{}
	}
	if event.SemanticContext.Attributes == nil {
		event.SemanticContext.Attributes = make(map[string]string)
	}
	if retire {
		event.SemanticContext.Attributes[RunRetirementAttribute] = "true"
	} else {
		event.SemanticContext.Attributes[RunRetirementAttribute] = "false"
	}
}

// RetiresRun reports whether an event is authoritative evidence that the
// complete Run was stopped or terminated by its lifecycle owner.
func RetiresRun(event *tgsrlv1.SandboxEvent) bool {
	return event != nil && EnvelopeRetiresRun(event.GetSemanticContext())
}

// EnvelopeRetiresRun reads the marker after an event has been projected into
// durable Sandbox state.
func EnvelopeRetiresRun(envelope *tgsrlv1.SemanticEnvelope) bool {
	return envelope != nil && envelope.GetAttributes()[RunRetirementAttribute] == "true"
}

// HasRunRetirementDisposition distinguishes an explicit execution-substrate
// terminal observation from a Scheduler-internal release event.
func HasRunRetirementDisposition(envelope *tgsrlv1.SemanticEnvelope) bool {
	if envelope == nil {
		return false
	}
	_, ok := envelope.GetAttributes()[RunRetirementAttribute]
	return ok
}
