package semantics

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
)

type semanticVersion struct {
	major      uint64
	minor      uint64
	patch      uint64
	prerelease []string
	build      []string
}

func normalizeVersion(value string) (string, error) {
	parsed, err := parseSemanticVersion(value)
	if err != nil {
		return "", err
	}
	result := fmt.Sprintf("%d.%d.%d", parsed.major, parsed.minor, parsed.patch)
	if len(parsed.prerelease) > 0 {
		result += "-" + strings.Join(parsed.prerelease, ".")
	}
	if len(parsed.build) > 0 {
		result += "+" + strings.Join(parsed.build, ".")
	}
	return result, nil
}

// NormalizeSemanticVersion normalizes the version syntax shared by contract
// validation and evaluation. Two-component legacy protocol versions retain
// compatibility and are normalized by appending a zero patch component.
func NormalizeSemanticVersion(value string) (string, error) {
	return normalizeVersion(value)
}

func parseSemanticVersion(value string) (semanticVersion, error) {
	if value == "" || value != strings.TrimSpace(value) {
		return semanticVersion{}, errors.New("version must be non-empty and canonical")
	}
	value = strings.TrimPrefix(strings.TrimPrefix(value, "v"), "V")
	coreAndPre, buildPart, hasBuild := strings.Cut(value, "+")
	if hasBuild && (buildPart == "" || strings.Contains(buildPart, "+")) {
		return semanticVersion{}, errors.New("version build metadata is invalid")
	}
	core, prereleasePart, hasPrerelease := strings.Cut(coreAndPre, "-")
	parts := strings.Split(core, ".")
	if len(parts) != 2 && len(parts) != 3 {
		return semanticVersion{}, errors.New("version must have 2 or 3 numeric components")
	}
	values := [3]uint64{}
	for index, part := range parts {
		if !validNumericIdentifier(part) {
			return semanticVersion{}, errors.New("version components must be canonical uint32 values")
		}
		number, err := strconv.ParseUint(part, 10, 32)
		if err != nil {
			return semanticVersion{}, errors.New("version components must be canonical uint32 values")
		}
		values[index] = number
	}
	parsed := semanticVersion{major: values[0], minor: values[1], patch: values[2]}
	if hasPrerelease {
		parsed.prerelease = strings.Split(prereleasePart, ".")
		if !validVersionIdentifiers(parsed.prerelease, true) {
			return semanticVersion{}, errors.New("version prerelease is invalid")
		}
	}
	if hasBuild {
		parsed.build = strings.Split(buildPart, ".")
		if !validVersionIdentifiers(parsed.build, false) {
			return semanticVersion{}, errors.New("version build metadata is invalid")
		}
	}
	return parsed, nil
}

func validNumericIdentifier(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func validVersionIdentifiers(values []string, prerelease bool) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if value == "" {
			return false
		}
		numeric := true
		for _, character := range value {
			if (character < '0' || character > '9') && (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') && character != '-' {
				return false
			}
			if character < '0' || character > '9' {
				numeric = false
			}
		}
		if prerelease && numeric && len(value) > 1 && value[0] == '0' {
			return false
		}
	}
	return true
}

func compareSemanticVersions(left, right semanticVersion) int {
	for _, pair := range [][2]uint64{{left.major, right.major}, {left.minor, right.minor}, {left.patch, right.patch}} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	if len(left.prerelease) == 0 && len(right.prerelease) == 0 {
		return 0
	}
	if len(left.prerelease) == 0 {
		return 1
	}
	if len(right.prerelease) == 0 {
		return -1
	}
	for index := 0; index < len(left.prerelease) && index < len(right.prerelease); index++ {
		leftPart, rightPart := left.prerelease[index], right.prerelease[index]
		if leftPart == rightPart {
			continue
		}
		leftNumeric, rightNumeric := numericPrerelease(leftPart), numericPrerelease(rightPart)
		switch {
		case leftNumeric && !rightNumeric:
			return -1
		case !leftNumeric && rightNumeric:
			return 1
		case leftNumeric && rightNumeric:
			if len(leftPart) < len(rightPart) || (len(leftPart) == len(rightPart) && leftPart < rightPart) {
				return -1
			}
			return 1
		case leftPart < rightPart:
			return -1
		default:
			return 1
		}
	}
	if len(left.prerelease) < len(right.prerelease) {
		return -1
	}
	if len(left.prerelease) > len(right.prerelease) {
		return 1
	}
	return 0
}

func numericPrerelease(value string) bool {
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return value != ""
}

func protocolCompatible(current, required string) bool {
	actual, actualErr := parseSemanticVersion(current)
	minimum, requiredErr := parseSemanticVersion(required)
	if actualErr != nil || requiredErr != nil || actual.major != minimum.major || compareSemanticVersions(actual, minimum) < 0 {
		return false
	}
	if actual.major == 0 {
		return actual.minor == minimum.minor
	}
	return true
}

// VersionsCompatible reports whether current is within required's compatible
// SemVer window and is not older than required.
func VersionsCompatible(current, required string) bool {
	return protocolCompatible(current, required)
}

const (
	componentPrioritySnapshot    = 1
	componentPriorityObservation = 2
	componentPriorityBuiltIn     = 3
)

type componentIdentity struct {
	kind tgsrlv1.ComponentKind
	name string
}

type componentEntry struct {
	version  *tgsrlv1.ComponentVersion
	priority int
	builtIn  bool
	conflict string
}

type componentRegistry struct {
	entries      map[componentIdentity]componentEntry
	byKind       map[tgsrlv1.ComponentKind][]componentIdentity
	evaluationAt time.Time
	devicesKnown bool
	hasGPU       bool
}

var componentAliases = map[string]tgsrlv1.ComponentKind{
	"protocol":          tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL,
	"scheduler":         tgsrlv1.ComponentKind_COMPONENT_KIND_SCHEDULER,
	"runtime":           tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME,
	"operator":          tgsrlv1.ComponentKind_COMPONENT_KIND_OPERATOR,
	"provider":          tgsrlv1.ComponentKind_COMPONENT_KIND_PROVIDER,
	"framework-adapter": tgsrlv1.ComponentKind_COMPONENT_KIND_FRAMEWORK_ADAPTER,
	"rollout-engine":    tgsrlv1.ComponentKind_COMPONENT_KIND_ROLLOUT_ENGINE,
	"trainer":           tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER,
	"cuda-driver":       tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER,
	"execution-backend": tgsrlv1.ComponentKind_COMPONENT_KIND_EXECUTION_BACKEND,
}

func canonicalComponentAlias(value string) string {
	return strings.ReplaceAll(strings.ToLower(strings.TrimSpace(value)), "_", "-")
}

// ComponentKindForAlias resolves the backward-compatible component spelling.
func ComponentKindForAlias(value string) (tgsrlv1.ComponentKind, bool) {
	kind, ok := componentAliases[canonicalComponentAlias(value)]
	return kind, ok
}

func buildComponentRegistry(input Input, observation *tgsrlv1.ContractObservation) (*componentRegistry, error) {
	registry := &componentRegistry{
		entries:      make(map[componentIdentity]componentEntry),
		byKind:       make(map[tgsrlv1.ComponentKind][]componentIdentity),
		evaluationAt: input.Context.GetEvaluationTime().AsTime().UTC(),
	}
	if err := registry.add(&tgsrlv1.ComponentVersion{
		Kind:    tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL,
		Name:    "protocol",
		Version: ProtocolVersion,
		Source:  "scheduler-protocol",
	}, componentPriorityBuiltIn, true); err != nil {
		return nil, err
	}
	if observation != nil {
		for _, version := range observation.GetComponentVersions() {
			if err := registry.add(version, componentPriorityObservation, false); err != nil {
				return nil, fmt.Errorf("contract observation component version: %w", err)
			}
		}
	}
	if input.Snapshot != nil && len(input.Snapshot.GetDevices()) > 0 {
		registry.devicesKnown = true
		for _, device := range input.Snapshot.GetDevices() {
			if device == nil || device.GetKind() == tgsrlv1.DeviceKind_DEVICE_KIND_UNKNOWN {
				registry.devicesKnown = false
				continue
			}
			if device.GetKind() == tgsrlv1.DeviceKind_DEVICE_KIND_GPU {
				registry.hasGPU = true
			}
			for _, version := range device.GetCapabilities().GetComponentVersions() {
				if version.GetKind() != tgsrlv1.ComponentKind_COMPONENT_KIND_PROVIDER && version.GetKind() != tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER {
					continue
				}
				if err := registry.add(version, componentPrioritySnapshot, false); err != nil {
					return nil, fmt.Errorf("snapshot component version: %w", err)
				}
			}
		}
	}
	for identity := range registry.entries {
		registry.byKind[identity.kind] = append(registry.byKind[identity.kind], identity)
	}
	for kind := range registry.byKind {
		sort.Slice(registry.byKind[kind], func(i, j int) bool {
			return registry.byKind[kind][i].name < registry.byKind[kind][j].name
		})
	}
	return registry, nil
}

func (registry *componentRegistry) add(version *tgsrlv1.ComponentVersion, priority int, builtIn bool) error {
	if version == nil {
		return errors.New("component version is nil")
	}
	if _, known := tgsrlv1.ComponentKind_name[int32(version.GetKind())]; !known || version.GetKind() == tgsrlv1.ComponentKind_COMPONENT_KIND_UNKNOWN {
		return errors.New("component kind must be recognized and non-UNKNOWN")
	}
	if version.GetName() == "" || version.GetName() != strings.TrimSpace(version.GetName()) {
		return errors.New("component name must be non-empty and canonical")
	}
	if version.GetVersion() == "" || version.GetVersion() != strings.TrimSpace(version.GetVersion()) {
		return errors.New("component version must be non-empty and canonical")
	}
	if !builtIn {
		if version.GetObservedAt() == nil {
			return errors.New("component observed_at is required")
		}
		if err := version.GetObservedAt().CheckValid(); err != nil {
			return fmt.Errorf("component observed_at is invalid: %w", err)
		}
		if version.GetSource() == "" || version.GetSource() != strings.TrimSpace(version.GetSource()) {
			return errors.New("component source must be non-empty and canonical")
		}
	}
	identity := componentIdentity{kind: version.GetKind(), name: strings.ToLower(version.GetName())}
	incoming := componentEntry{version: proto.Clone(version).(*tgsrlv1.ComponentVersion), priority: priority, builtIn: builtIn}
	existing, found := registry.entries[identity]
	if !found {
		registry.entries[identity] = incoming
		return nil
	}
	if incoming.priority > existing.priority {
		registry.entries[identity] = incoming
		return nil
	}
	if incoming.priority < existing.priority {
		return nil
	}
	if versionsSemanticallyEqual(existing.version.GetVersion(), incoming.version.GetVersion()) {
		return nil
	}
	existing.conflict = fmt.Sprintf("conflicting component versions %q and %q at the same priority", existing.version.GetVersion(), incoming.version.GetVersion())
	registry.entries[identity] = existing
	return nil
}

func versionsSemanticallyEqual(left, right string) bool {
	if left == right {
		return true
	}
	leftNormalized, leftErr := normalizeVersion(left)
	rightNormalized, rightErr := normalizeVersion(right)
	return leftErr == nil && rightErr == nil && leftNormalized == rightNormalized
}

func evaluateVersionConstraint(input Input, registry *componentRegistry, constraint *tgsrlv1.VersionConstraint) *tgsrlv1.ContractEvaluation {
	clauseID := constraint.GetComponent()
	base := newEvaluation(input, tgsrlv1.ContractClauseKind_CONTRACT_CLAUSE_KIND_VERSION_CONSTRAINT, clauseID, nil, tgsrlv1.ValidityFailureMode_VALIDITY_FAILURE_MODE_REJECT)
	kind, generic, err := constraintComponentKind(constraint)
	if err != nil {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.Detail = err.Error()
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT
		return finalizeEvaluation(input, base)
	}
	base.Evidence = append(base.Evidence,
		semanticField("component.kind", semanticString(kind.String())),
		semanticField("component.version.required", semanticString(constraint.GetVersion())),
		semanticField("component.version.operator", semanticString(constraint.GetOperator().String())),
	)
	if kind == tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER && registry.devicesKnown && !registry.hasGPU {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_NOT_APPLICABLE
		base.ObservationDisposition = tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_NOT_APPLICABLE
		base.Detail = "CUDA driver is not applicable to a device set with no GPU"
		base.Evidence = append(base.Evidence, semanticField("snapshot.device_set", semanticString("cpu-only")))
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
		return finalizeEvaluation(input, base)
	}
	_, entry, found, ambiguity := registry.resolve(kind, constraint.GetComponent(), generic)
	if ambiguity != "" {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.Detail = ambiguity
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT
		return finalizeEvaluation(input, base)
	}
	if !found {
		base.MissingKeys = []string{componentMissingKey(kind, constraint.GetComponent())}
		base.Detail = "applicable component version is missing"
		applyObservationDisposition(base, versionObservationPolicy(constraint).GetMissing())
		return finalizeEvaluation(input, base)
	}
	base.Observations = append(base.Observations, semanticField("component.version.current", semanticString(entry.version.GetVersion())))
	base.Evidence = append(base.Evidence, componentEvidence(entry.version)...)
	if conflict := entry.conflict; conflict != "" {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_INDETERMINATE
		base.Detail = conflict
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT
		return finalizeEvaluation(input, base)
	}
	if !entry.builtIn {
		state, evidence, detail := componentObservationState(registry.evaluationAt, entry.version, versionObservationPolicy(constraint))
		base.Evidence = append(base.Evidence, evidence...)
		if state == observationStale {
			base.Detail = detail
			applyObservationDisposition(base, versionObservationPolicy(constraint).GetStale())
			return finalizeEvaluation(input, base)
		}
	}
	compatible, detail := evaluateVersionMatch(entry.version.GetVersion(), constraint)
	if compatible {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_SATISFIED
		base.Detail = detail
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_ALLOW
	} else {
		base.Status = tgsrlv1.ContractEvaluationStatus_CONTRACT_EVALUATION_STATUS_VIOLATED
		base.Detail = detail
		base.RecommendedAction = tgsrlv1.ContractDecisionAction_CONTRACT_DECISION_ACTION_REJECT
	}
	return finalizeEvaluation(input, base)
}

func constraintComponentKind(constraint *tgsrlv1.VersionConstraint) (tgsrlv1.ComponentKind, bool, error) {
	if constraint == nil || constraint.GetComponent() == "" || constraint.GetComponent() != strings.TrimSpace(constraint.GetComponent()) {
		return tgsrlv1.ComponentKind_COMPONENT_KIND_UNKNOWN, false, errors.New("component constraint is malformed")
	}
	aliasKind, alias := ComponentKindForAlias(constraint.GetComponent())
	kind := constraint.GetComponentKind()
	if kind == tgsrlv1.ComponentKind_COMPONENT_KIND_UNKNOWN {
		if !alias {
			return kind, false, errors.New("legacy component must use a recognized alias")
		}
		return aliasKind, true, nil
	}
	if _, known := tgsrlv1.ComponentKind_name[int32(kind)]; !known {
		return kind, false, errors.New("component kind is not recognized")
	}
	if alias && aliasKind != kind {
		return kind, false, errors.New("component alias conflicts with component kind")
	}
	return kind, alias, nil
}

func (registry *componentRegistry) resolve(kind tgsrlv1.ComponentKind, component string, generic bool) (componentIdentity, componentEntry, bool, string) {
	if !generic {
		identity := componentIdentity{kind: kind, name: strings.ToLower(component)}
		entry, ok := registry.entries[identity]
		return identity, entry, ok, ""
	}
	candidates := registry.byKind[kind]
	if len(candidates) == 0 {
		return componentIdentity{}, componentEntry{}, false, ""
	}
	if len(candidates) > 1 {
		return componentIdentity{}, componentEntry{}, false, "component alias matches multiple observed components"
	}
	identity := candidates[0]
	return identity, registry.entries[identity], true, ""
}

func versionObservationPolicy(constraint *tgsrlv1.VersionConstraint) *tgsrlv1.ObservationPolicy {
	if constraint.GetObservationPolicy() != nil {
		return constraint.GetObservationPolicy()
	}
	return &tgsrlv1.ObservationPolicy{
		Missing: tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
		Stale:   tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
	}
}

func componentObservationState(evaluationAt time.Time, version *tgsrlv1.ComponentVersion, policy *tgsrlv1.ObservationPolicy) (observationState, []*tgsrlv1.SemanticField, string) {
	if version.GetObservedAt() == nil {
		return observationStale, nil, "component observation time is missing"
	}
	observedAt := version.GetObservedAt().AsTime().UTC()
	age := evaluationAt.Sub(observedAt)
	evidence := []*tgsrlv1.SemanticField{
		semanticField("component.observed_at", semanticString(observedAt.Format(time.RFC3339Nano))),
		semanticField("component.age_ms", semanticInt(age.Milliseconds())),
	}
	if age < 0 {
		return observationStale, evidence, "component observation time is after evaluation time"
	}
	if maximumAge := policy.GetMaximumAge(); maximumAge != nil {
		evidence = append(evidence, semanticField("component.maximum_age_ms", semanticInt(maximumAge.AsDuration().Milliseconds())))
		if age > maximumAge.AsDuration() {
			return observationStale, evidence, "component observation exceeded maximum age"
		}
	}
	return observationFresh, evidence, "component observation is fresh"
}

func componentEvidence(version *tgsrlv1.ComponentVersion) []*tgsrlv1.SemanticField {
	return []*tgsrlv1.SemanticField{
		semanticField("component.name", semanticString(version.GetName())),
		semanticField("component.source", semanticString(version.GetSource())),
		semanticField("component.revision", semanticUint(version.GetRevision())),
	}
}

func componentMissingKey(kind tgsrlv1.ComponentKind, component string) string {
	return "component_version." + strings.ToLower(strings.TrimPrefix(kind.String(), "COMPONENT_KIND_")) + "." + strings.ToLower(component)
}

func evaluateVersionMatch(current string, constraint *tgsrlv1.VersionConstraint) (bool, string) {
	if constraint.GetOperator() == tgsrlv1.VersionOperator_VERSION_OPERATOR_EXACT {
		exact := current == constraint.GetVersion()
		actual, actualErr := parseSemanticVersion(current)
		_, requiredErr := parseSemanticVersion(constraint.GetVersion())
		if !exact && actualErr == nil && requiredErr == nil {
			normalizedActual, _ := normalizeVersion(current)
			normalizedRequired, _ := normalizeVersion(constraint.GetVersion())
			exact = normalizedActual == normalizedRequired
		}
		if exact && actualErr == nil && len(actual.prerelease) > 0 && !constraint.GetAllowPrerelease() {
			return false, "prerelease component version is not allowed"
		}
		if exact {
			return true, "component version exactly matched"
		}
		return false, "component version did not exactly match"
	}
	actual, actualErr := normalizeVersion(current)
	required, requiredErr := normalizeVersion(constraint.GetVersion())
	if actualErr != nil || requiredErr != nil {
		return false, "component or required version is not valid semantic version syntax"
	}
	actualParsed, _ := parseSemanticVersion(actual)
	requiredParsed, _ := parseSemanticVersion(required)
	if (len(actualParsed.prerelease) > 0 || len(requiredParsed.prerelease) > 0) && !constraint.GetAllowPrerelease() {
		return false, "prerelease component version is not allowed"
	}
	switch constraint.GetOperator() {
	case tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER:
		if compareSemanticVersions(actualParsed, requiredParsed) >= 0 {
			return true, "component semantic version satisfies the minimum"
		}
		return false, "component semantic version is below the minimum"
	case tgsrlv1.VersionOperator_VERSION_OPERATOR_COMPATIBLE:
		if protocolCompatible(actual, required) {
			return true, "component semantic version is compatible"
		}
		return false, "component semantic version is incompatible"
	default:
		return false, "version operator is unknown"
	}
}
