// Command proto-roundtrip-go decodes a length-delimited SchedulingIntent and
// PlacementPlan, DecisionRecord, and RuntimeManifest from stdin and writes all
// deterministic payloads back. The
// cross-language test owns fixture construction and semantic assertions.
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	wire, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail(err)
	}
	frames, err := splitFrames(wire, 4)
	if err != nil {
		fail(err)
	}
	intent := &tgsrlv1.SchedulingIntent{}
	if err := proto.Unmarshal(frames[0], intent); err != nil {
		fail(err)
	}
	if intent.GetExecutionId() == "" || intent.GetStageId() == "" || intent.GetVersion() == 0 {
		fail(fmt.Errorf("decoded intent is missing its identity"))
	}
	plan := &tgsrlv1.PlacementPlan{}
	if err := proto.Unmarshal(frames[1], plan); err != nil {
		fail(err)
	}
	if plan.GetPlanId() == "" {
		fail(fmt.Errorf("decoded plan is missing its identity"))
	}
	decision := &tgsrlv1.DecisionRecord{}
	if err := proto.Unmarshal(frames[2], decision); err != nil {
		fail(err)
	}
	if decision.GetDecisionId() == "" || len(decision.GetContractEvaluations()) != 1 {
		fail(fmt.Errorf("decoded decision is missing PR2 evaluation coverage"))
	}
	manifest := &tgsrlv1.RuntimeManifest{}
	if err := proto.Unmarshal(frames[3], manifest); err != nil {
		fail(err)
	}
	if manifest.GetManifestId() != "roundtrip-manifest" || len(manifest.GetComponentVersions()) != 1 {
		fail(fmt.Errorf("decoded manifest is missing PR2 component versions"))
	}
	observation := intent.GetContractObservation()
	contract := intent.GetExecutionContract()
	if observation == nil || len(observation.GetFactObservations()) != 1 || len(observation.GetComponentVersions()) != 1 ||
		contract == nil || len(contract.GetCriticalFactPolicies()) != 1 || len(contract.GetVersionConstraints()) == 0 ||
		decision.GetContractEvaluations()[0].GetObservationDisposition() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD {
		fail(fmt.Errorf("decoded messages are missing PR2 freshness/version fields"))
	}
	factObservation := observation.GetFactObservations()[0]
	componentVersion := observation.GetComponentVersions()[0]
	versionConstraint := contract.GetVersionConstraints()[0]
	factPolicy := contract.GetCriticalFactPolicies()[0]
	capabilityVersions := intent.GetRequiredCapabilities().GetComponentVersions()
	manifestVersion := manifest.GetComponentVersions()[0]
	if len(capabilityVersions) != 1 || capabilityVersions[0].GetKind() != tgsrlv1.ComponentKind_COMPONENT_KIND_EXECUTION_BACKEND ||
		capabilityVersions[0].GetRevision() != 42 ||
		manifestVersion.GetKind() != tgsrlv1.ComponentKind_COMPONENT_KIND_EXECUTION_BACKEND ||
		manifestVersion.GetName() != "execution-backend-primary" || manifestVersion.GetRevision() != 42 ||
		factObservation.GetFact().GetKey() != "sample.policy_lag" || factObservation.GetObservedAt() == nil ||
		factObservation.GetSource() != "roundtrip-runtime" || factObservation.GetRevision() != 41 ||
		componentVersion.GetKind() != tgsrlv1.ComponentKind_COMPONENT_KIND_EXECUTION_BACKEND ||
		componentVersion.GetName() != "execution-backend-primary" || componentVersion.GetVersion() != "1.2.3-rc.1" ||
		componentVersion.GetObservedAt() == nil || componentVersion.GetSource() != "roundtrip-registry" ||
		componentVersion.GetRevision() != 42 || componentVersion.GetAttributes()["region"] != "test" ||
		versionConstraint.GetComponentKind() != tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL ||
		!versionConstraint.GetAllowPrerelease() || versionConstraint.GetSource() != "roundtrip-registry" ||
		versionConstraint.GetRevision() != 42 || versionConstraint.GetObservationPolicy() == nil ||
		versionConstraint.GetObservationPolicy().GetMaximumAge().GetSeconds() != 30 ||
		versionConstraint.GetObservationPolicy().GetMissing() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK ||
		versionConstraint.GetObservationPolicy().GetStale() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD ||
		factPolicy.GetFactPath() != "sample.policy_lag" || factPolicy.GetObservationPolicy() == nil ||
		factPolicy.GetObservationPolicy().GetMaximumAge().GetSeconds() != 10 ||
		factPolicy.GetObservationPolicy().GetMissing() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_DEGRADE ||
		factPolicy.GetObservationPolicy().GetStale() != tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE {
		fail(fmt.Errorf("decoded PR2 field values changed"))
	}
	intentOut, err := proto.MarshalOptions{Deterministic: true}.Marshal(proto.Clone(intent))
	if err != nil {
		fail(err)
	}
	planOut, err := proto.MarshalOptions{Deterministic: true}.Marshal(proto.Clone(plan))
	if err != nil {
		fail(err)
	}
	decisionOut, err := proto.MarshalOptions{Deterministic: true}.Marshal(proto.Clone(decision))
	if err != nil {
		fail(err)
	}
	manifestOut, err := proto.MarshalOptions{Deterministic: true}.Marshal(proto.Clone(manifest))
	if err != nil {
		fail(err)
	}
	out := appendFrame(nil, intentOut)
	out = appendFrame(out, planOut)
	out = appendFrame(out, decisionOut)
	out = appendFrame(out, manifestOut)
	if _, err := os.Stdout.Write(out); err != nil {
		fail(err)
	}
}

func splitFrames(wire []byte, count int) ([][]byte, error) {
	frames := make([][]byte, 0, count)
	for index := 0; index < count; index++ {
		length, read := binary.Uvarint(wire)
		if read <= 0 || length > uint64(len(wire)-read) {
			return nil, fmt.Errorf("invalid frame %d", index)
		}
		end := read + int(length)
		frames = append(frames, wire[read:end])
		wire = wire[end:]
	}
	if len(wire) != 0 {
		return nil, fmt.Errorf("unexpected trailing round-trip bytes")
	}
	return frames, nil
}

func appendFrame(out, payload []byte) []byte {
	var prefix [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(prefix[:], uint64(len(payload)))
	out = append(out, prefix[:length]...)
	return append(out, payload...)
}

func fail(err error) {
	_, _ = fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
