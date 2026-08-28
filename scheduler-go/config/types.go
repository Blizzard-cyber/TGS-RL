package config

import (
	"regexp"
	"time"
)

var (
	imageDigestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	floatingTokenPattern   = regexp.MustCompile(`(?i)(?:^|[^a-z])(latest|stable|main|master|head|edge)(?:[^a-z]|$)`)
	unpinnedVersionPattern = regexp.MustCompile(`[*xX]|>=|<=|~=|!=|\^`)
	sensitiveTokenPattern  = regexp.MustCompile(`(?i)(secret|token|password|credential|authorization|cookie|session|key)`)
)

const (
	DefaultManifestPath = "compatibility/manifests/cpu-mock.yaml"
	DefaultEnvPrefix    = "TGSRL_CONFIG_"
)

var expectedSchemas = map[string]string{
	"manifest": "tgsrl.io/compatibility-manifest/v1alpha1", "bom": "tgsrl.io/bom/v1alpha1", "profile": "tgsrl.io/compatibility-profile/v1alpha1", "capabilities": "tgsrl.io/capability-set/v1alpha1", "policy": "tgsrl.io/policy-bundle/v1alpha1", "scenario": "tgsrl.io/synthetic-scenario/v1alpha1",
}

type ConfigError struct {
	Code, Message, Source, Field string
	Context                      map[string]any
}

func (e *ConfigError) Error() string { return e.Message }
func (e *ConfigError) CodeOrDefault() string {
	if e.Code == "" {
		return "config_error"
	}
	return e.Code
}
func (e *ConfigError) Diagnostic() map[string]any {
	d := map[string]any{"code": e.CodeOrDefault(), "message": e.Message}
	if e.Source != "" {
		d["source"] = e.Source
	}
	if e.Field != "" {
		d["field"] = e.Field
	}
	if len(e.Context) > 0 {
		c := make(map[string]any, len(e.Context))
		for k, v := range e.Context {
			c[k] = redactValue(k, v)
		}
		d["context"] = c
	}
	return d
}
func configError(message, code, source, field string, context map[string]any) *ConfigError {
	return &ConfigError{Code: code, Message: message, Source: source, Field: field, Context: context}
}

type LoadOptions struct {
	Root, ManifestPath, EnvPrefix string
	Env                           map[string]string
}
type ReferenceSet struct{ BOM, Profile, Capabilities, Policy, RepresentativeScenario, PatchLedger string }
type ComponentRef struct {
	Name    string
	Version *string
}
type ResourceProviderRef struct{ Name, Source string }
type KubernetesRef struct {
	Enabled bool
	Version *string
}
type Combination struct {
	Framework, ExecutionBackend, Trainer, RolloutEngine ComponentRef
	ResourceProvider                                    ResourceProviderRef
	Kubernetes                                          KubernetesRef
	GPUManagementProfile                                string
}
type DeclaredMatrix struct {
	Algorithms, RolloutModes, DataKinds []string
	EvidenceStatus                      string
	Evidence                            []string
}
type CompatibilityManifest struct {
	Path, SchemaVersion, ManifestID string
	Revision                        int
	Status, Scope                   string
	References                      ReferenceSet
	Combination                     Combination
	DeclaredMatrix                  DeclaredMatrix
}
type PinningPolicy struct {
	FloatingVersionsAllowed, ImmutableImageDigestRequiredForRelease bool
	UnknownCommitOrDigestValue                                      *string
	Note                                                            string
}
type DependencyRecord struct {
	Name, Version, Source        string
	SourceCommit, ArtifactDigest *string
}
type DevelopmentImage struct{ Name, Image, ImageDigest string }
type RuntimeBOM struct {
	Path, SchemaVersion, BOMID     string
	Revision                       int
	Status, Scope                  string
	PinningPolicy                  PinningPolicy
	Toolchain, RuntimeDependencies []DependencyRecord
	DevelopmentImages              []DevelopmentImage
}
type ProviderConfig struct {
	Kind, Source     string
	LiveHardware     bool
	AuthoritativeFor string
}
type RuntimeScope struct{ RequiredHostResources, DataKinds, Algorithms, RolloutModes, LifecycleActions []string }
type GPUManagementProfile struct {
	Selected                         string
	AllowedValues                    []string
	MutuallyExclusive                bool
	MaximumActiveProfiles            int
	AdmissionRejectsMultipleProfiles bool
	EnabledProfiles                  []string
}
type CompatibilityProfile struct {
	Path, SchemaVersion, ProfileID string
	Revision                       int
	Status, Description            string
	Provider                       ProviderConfig
	RuntimeScope                   RuntimeScope
	GPUManagementProfile           GPUManagementProfile
}
type CapabilityClaims struct{ MockOnly, LiveGPU, HardwareLatency, Performance bool }
type CapabilitySetConfig struct {
	Path, SchemaVersion, CapabilityID          string
	Revision                                   int
	Source, Semantics, VerificationStatus      string
	MeasuredAt                                 *time.Time
	RuntimeLoader                              bool
	Names                                      []string
	Attributes                                 map[string]string
	Limits                                     map[string]float64
	Algorithms, RolloutModes, SupportedActions []string
	Claims                                     CapabilityClaims
}
type SelectionPolicy struct {
	Strategy    string
	Tiebreakers []string
	TopK        int
}
type FallbackPolicy struct {
	Order                         []string
	PermitBinpack, ReasonRequired bool
}
type ActionPolicy struct {
	UnsupportedAction                                                           string
	RequireIdempotencyKey, RequireExplicitRollback, RequireGenerationFenceForL4 bool
}
type ConstraintPolicy struct {
	RequireCapacity              bool
	RequireCapability            bool
	RequireSafePoint             bool
	RequireCurrentIntent         bool
	RequireSnapshotRevisionMatch bool
	RejectUnknownCapability      bool
}
type ProtectionPolicy struct {
	Enabled             bool
	Cooldown            time.Duration
	Hysteresis          float64
	MaxActionsPerWindow int
	Window              time.Duration
	BreakerThreshold    int
	BreakerResetAfter   time.Duration
}

type PreemptionPolicy struct {
	Enabled          bool
	Strategy         string
	RequireSafePoint bool
}
type DeterminismPolicy struct {
	ExplicitSeedRequired            bool
	VirtualClockRequiredForReplay   bool
	DeterministicProtoSerialization bool
	WallClockInCanonicalOutput      bool
}
type SafetyPolicy struct {
	RewardValueAllowed         bool
	StaleIntentAllowed         bool
	StaleSnapshotCommitAllowed bool
	PartialFailureIsSuccess    bool
}

type PolicyBundle struct {
	Path          string
	SchemaVersion string
	PolicyID      string
	PolicyVersion string
	Status        string
	RuntimeLoader bool
	Description   string
	Selection     SelectionPolicy
	Constraints   ConstraintPolicy
	Fallback      FallbackPolicy
	Actions       ActionPolicy
	Protection    ProtectionPolicy
	Preemption    PreemptionPolicy
	Determinism   DeterminismPolicy
	Safety        SafetyPolicy
}

type ScenarioReferences struct {
	CompatibilityManifest string
	ResourceProfile       string
	Capabilities          string
	Policy                string
}

type ScenarioWorkload struct {
	Algorithm   string
	RolloutMode string
	ExecutionID string
	StageID     string
	UnitCount   int
}

type Topology struct {
	ProviderSource string
}

type SyntheticScenarioConfig struct {
	Path             string
	SchemaVersion    string
	ScenarioID       string
	ScenarioRevision int
	Status           string
	RuntimeLoader    bool
	DataKind         string
	Seed             int
	References       ScenarioReferences
	Workload         ScenarioWorkload
	Topology         Topology
}

type AppliedOverrides struct {
	ManifestPath     string
	BOMPath          string
	ProfilePath      string
	CapabilitiesPath string
	PolicyPath       string
	ScenarioPath     string
	Env              map[string]string
}

type Bundle struct {
	Root         string
	Manifest     CompatibilityManifest
	BOM          RuntimeBOM
	Profile      CompatibilityProfile
	Capabilities CapabilitySetConfig
	Policy       PolicyBundle
	Scenario     SyntheticScenarioConfig
	Overrides    AppliedOverrides
}

type RuntimeIntervals struct {
	Fast   time.Duration
	Medium time.Duration
	Slow   time.Duration
}

type StartupConfig struct {
	Root               string
	ManifestPath       string
	ProviderKind       string
	ProviderSource     string
	Capabilities       CapabilitySetConfig
	FallbackMode       string
	SelectionStrategy  string
	TopK               int
	RuntimeIntervals   RuntimeIntervals
	Policy             PolicyBundle
	AppliedEnvOverride map[string]string
}

type SchedulerPolicyConfig struct {
	PolicyID      string
	PolicyVersion string
	Strategy      string
	Protection    ProtectionPolicy
	Preemption    PreemptionPolicy
}

type preparedLine struct {
	Number int
	Indent int
	Text   string
	Raw    string
}

type yamlSubsetParser struct {
	path  string
	lines []preparedLine
	index int
}
