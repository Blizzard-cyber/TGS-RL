package scheduler

import (
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func pr2ObservationPolicy(age time.Duration) *tgsrlv1.ObservationPolicy {
	return &tgsrlv1.ObservationPolicy{
		MaximumAge: durationpb.New(age),
		Missing:    tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_BLOCK,
		Stale:      tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_HOLD,
	}
}

func refreshPR2ContractID(t *testing.T, contract *tgsrlv1.ExecutionContract) {
	t.Helper()
	contract.ContractId = ""
	id, err := CanonicalContractID(contract)
	if err != nil {
		t.Fatalf("CanonicalContractID() error = %v", err)
	}
	contract.ContractId = id
}

func requirePR2ValidationError(t *testing.T, intent *tgsrlv1.SchedulingIntent, field string) {
	t.Helper()
	refreshPR2ContractID(t, intent.GetExecutionContract())
	err := ValidateIntent(intent)
	if err == nil || !IsValidationError(err) || !strings.Contains(err.Error(), field) {
		t.Fatalf("ValidateIntent() error = %v, want validation error containing %q", err, field)
	}
}

func TestPR2CriticalFactPolicyValidation(t *testing.T) {
	valid := &tgsrlv1.CriticalFactPolicy{
		FactPath:          "sample.policy_lag",
		ObservationPolicy: pr2ObservationPolicy(time.Second),
	}
	tests := []struct {
		name   string
		mutate func(*tgsrlv1.ExecutionContract)
		field  string
	}{
		{name: "nil entry", mutate: func(c *tgsrlv1.ExecutionContract) { c.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{nil} }, field: "critical_fact_policies[0]"},
		{name: "noncanonical path", mutate: func(c *tgsrlv1.ExecutionContract) { c.CriticalFactPolicies[0].FactPath = " Sample.PolicyLag" }, field: "fact_path"},
		{name: "duplicate path", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.CriticalFactPolicies = append(c.CriticalFactPolicies, proto.Clone(c.CriticalFactPolicies[0]).(*tgsrlv1.CriticalFactPolicy))
		}, field: "fact_path"},
		{name: "missing policy", mutate: func(c *tgsrlv1.ExecutionContract) { c.CriticalFactPolicies[0].ObservationPolicy = nil }, field: "observation_policy"},
		{name: "zero maximum age", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.CriticalFactPolicies[0].ObservationPolicy.MaximumAge = durationpb.New(0)
		}, field: "maximum_age"},
		{name: "negative maximum age", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.CriticalFactPolicies[0].ObservationPolicy.MaximumAge = durationpb.New(-time.Nanosecond)
		}, field: "maximum_age"},
		{name: "invalid maximum age", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.CriticalFactPolicies[0].ObservationPolicy.MaximumAge = &durationpb.Duration{Seconds: 1, Nanos: -1}
		}, field: "maximum_age"},
		{name: "unknown missing disposition", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.CriticalFactPolicies[0].ObservationPolicy.Missing = tgsrlv1.ObservationDisposition_OBSERVATION_DISPOSITION_UNKNOWN
		}, field: "observation_policy.missing"},
		{name: "unrecognized stale disposition", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.CriticalFactPolicies[0].ObservationPolicy.Stale = tgsrlv1.ObservationDisposition(99)
		}, field: "observation_policy.stale"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, intent := validFixture()
			intent.ExecutionContract.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{proto.Clone(valid).(*tgsrlv1.CriticalFactPolicy)}
			test.mutate(intent.ExecutionContract)
			requirePR2ValidationError(t, intent, test.field)
		})
	}

	_, intent := validFixture()
	withoutAge := proto.Clone(valid).(*tgsrlv1.CriticalFactPolicy)
	withoutAge.ObservationPolicy.MaximumAge = nil
	intent.ExecutionContract.CriticalFactPolicies = []*tgsrlv1.CriticalFactPolicy{withoutAge}
	refreshPR2ContractID(t, intent.ExecutionContract)
	if err := ValidateIntent(intent); err != nil {
		t.Fatalf("ValidateIntent(nil maximum_age) error = %v", err)
	}
}

func TestPR2VersionConstraintAliasesAndShape(t *testing.T) {
	aliases := []struct {
		alias string
		kind  tgsrlv1.ComponentKind
	}{
		{"protocol", tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL},
		{"scheduler", tgsrlv1.ComponentKind_COMPONENT_KIND_SCHEDULER},
		{"runtime", tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME},
		{"operator", tgsrlv1.ComponentKind_COMPONENT_KIND_OPERATOR},
		{"provider", tgsrlv1.ComponentKind_COMPONENT_KIND_PROVIDER},
		{"framework_adapter", tgsrlv1.ComponentKind_COMPONENT_KIND_FRAMEWORK_ADAPTER},
		{"rollout_engine", tgsrlv1.ComponentKind_COMPONENT_KIND_ROLLOUT_ENGINE},
		{"trainer", tgsrlv1.ComponentKind_COMPONENT_KIND_TRAINER},
		{"cuda_driver", tgsrlv1.ComponentKind_COMPONENT_KIND_CUDA_DRIVER},
		{"execution_backend", tgsrlv1.ComponentKind_COMPONENT_KIND_EXECUTION_BACKEND},
	}
	for _, test := range aliases {
		t.Run(test.alias, func(t *testing.T) {
			identity, legacy, err := versionConstraintIdentity(&tgsrlv1.VersionConstraint{Component: test.alias})
			if err != nil || !legacy || identity.kind != test.kind || identity.name != test.alias {
				t.Fatalf("versionConstraintIdentity(%q) = (%+v, %v, %v)", test.alias, identity, legacy, err)
			}
			hyphenated := strings.ReplaceAll(test.alias, "_", "-")
			identity, legacy, err = versionConstraintIdentity(&tgsrlv1.VersionConstraint{Component: hyphenated})
			if err != nil || !legacy || identity.kind != test.kind || identity.name != test.alias {
				t.Fatalf("versionConstraintIdentity(%q) = (%+v, %v, %v)", hyphenated, identity, legacy, err)
			}
		})
	}
	for _, test := range aliases {
		t.Run("typed-"+test.alias, func(t *testing.T) {
			_, intent := validFixture()
			constraint := &tgsrlv1.VersionConstraint{
				Component:         test.alias,
				ComponentKind:     test.kind,
				Operator:          tgsrlv1.VersionOperator_VERSION_OPERATOR_EXACT,
				Version:           "build-1",
				ObservationPolicy: pr2ObservationPolicy(time.Second),
			}
			if test.kind == tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL {
				intent.ExecutionContract.VersionConstraints = []*tgsrlv1.VersionConstraint{constraint}
			} else {
				intent.ExecutionContract.VersionConstraints = append(intent.ExecutionContract.VersionConstraints, constraint)
			}
			refreshPR2ContractID(t, intent.ExecutionContract)
			if err := ValidateIntent(intent); err != nil {
				t.Fatalf("ValidateIntent(typed %q) error = %v", test.alias, err)
			}
		})
	}

	_, intent := validFixture()
	intent.ExecutionContract.VersionConstraints[0].Version = "9.9.9"
	refreshPR2ContractID(t, intent.ExecutionContract)
	if err := ValidateIntent(intent); err != nil {
		t.Fatalf("ValidateIntent(incompatible protocol shape) error = %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*tgsrlv1.ExecutionContract)
		field  string
	}{
		{name: "unknown legacy alias", mutate: func(c *tgsrlv1.ExecutionContract) { c.VersionConstraints[0].Component = "ray" }, field: "component"},
		{name: "unknown enum", mutate: func(c *tgsrlv1.ExecutionContract) { c.VersionConstraints[0].ComponentKind = tgsrlv1.ComponentKind(99) }, field: "component"},
		{name: "alias kind conflict", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.VersionConstraints[0].ComponentKind = tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME
		}, field: "component"},
		{name: "typed policy required", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.VersionConstraints[0].ComponentKind = tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL
		}, field: "observation_policy"},
		{name: "source canonical", mutate: func(c *tgsrlv1.ExecutionContract) { c.VersionConstraints[0].Source = " registry " }, field: "source"},
		{name: "semver strict", mutate: func(c *tgsrlv1.ExecutionContract) {
			c.VersionConstraints[0].Operator = tgsrlv1.VersionOperator_VERSION_OPERATOR_SEMVER
			c.VersionConstraints[0].Version = "driver-build"
		}, field: "version"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, intent := validFixture()
			test.mutate(intent.ExecutionContract)
			requirePR2ValidationError(t, intent, test.field)
		})
	}

	_, exactIntent := validFixture()
	exact := exactIntent.ExecutionContract.VersionConstraints[0]
	exact.Operator = tgsrlv1.VersionOperator_VERSION_OPERATOR_EXACT
	exact.Version = "driver-build-2026.08.29"
	refreshPR2ContractID(t, exactIntent.ExecutionContract)
	if err := ValidateIntent(exactIntent); err != nil {
		t.Fatalf("ValidateIntent(opaque EXACT version) error = %v", err)
	}

	_, backendIntent := validFixture()
	backendIntent.ExecutionContract.VersionConstraints = append(backendIntent.ExecutionContract.VersionConstraints, &tgsrlv1.VersionConstraint{
		Component:         "ray",
		ComponentKind:     tgsrlv1.ComponentKind_COMPONENT_KIND_EXECUTION_BACKEND,
		Operator:          tgsrlv1.VersionOperator_VERSION_OPERATOR_EXACT,
		Version:           "ray-build-1",
		ObservationPolicy: pr2ObservationPolicy(time.Second),
	})
	refreshPR2ContractID(t, backendIntent.ExecutionContract)
	if err := ValidateIntent(backendIntent); err != nil {
		t.Fatalf("ValidateIntent(execution backend constraint) error = %v", err)
	}
}

func TestPR2VersionConstraintIdentityDeduplication(t *testing.T) {
	_, intent := validFixture()
	protocol := intent.ExecutionContract.VersionConstraints[0]
	protocol.ComponentKind = tgsrlv1.ComponentKind_COMPONENT_KIND_PROTOCOL
	protocol.ObservationPolicy = pr2ObservationPolicy(time.Minute)
	duplicate := proto.Clone(protocol).(*tgsrlv1.VersionConstraint)
	duplicate.Component = "PROTOCOL"
	intent.ExecutionContract.VersionConstraints = append(intent.ExecutionContract.VersionConstraints, duplicate)
	requirePR2ValidationError(t, intent, "version_constraints[1].component")
}

func TestPR2ComponentVersionValidation(t *testing.T) {
	valid := func() *tgsrlv1.ComponentVersion {
		return &tgsrlv1.ComponentVersion{
			Kind:       tgsrlv1.ComponentKind_COMPONENT_KIND_RUNTIME,
			Name:       "runtime-primary",
			Version:    "opaque-build-7",
			ObservedAt: timestamppb.New(time.Unix(100, 0)),
			Source:     "runtime-registry",
			Revision:   1,
			Attributes: map[string]string{"region": "test"},
		}
	}
	tests := []struct {
		name   string
		mutate func(*tgsrlv1.ComponentVersion)
		field  string
	}{
		{name: "unknown kind", mutate: func(v *tgsrlv1.ComponentVersion) { v.Kind = tgsrlv1.ComponentKind_COMPONENT_KIND_UNKNOWN }, field: "kind"},
		{name: "unrecognized kind", mutate: func(v *tgsrlv1.ComponentVersion) { v.Kind = tgsrlv1.ComponentKind(99) }, field: "kind"},
		{name: "noncanonical name", mutate: func(v *tgsrlv1.ComponentVersion) { v.Name = " runtime " }, field: "name"},
		{name: "alias kind conflict", mutate: func(v *tgsrlv1.ComponentVersion) { v.Name = "protocol" }, field: "name"},
		{name: "empty version", mutate: func(v *tgsrlv1.ComponentVersion) { v.Version = "" }, field: "version"},
		{name: "noncanonical version", mutate: func(v *tgsrlv1.ComponentVersion) { v.Version = " build-1 " }, field: "version"},
		{name: "missing timestamp", mutate: func(v *tgsrlv1.ComponentVersion) { v.ObservedAt = nil }, field: "observed_at"},
		{name: "invalid timestamp", mutate: func(v *tgsrlv1.ComponentVersion) { v.ObservedAt = &timestamppb.Timestamp{Seconds: 253402300800} }, field: "observed_at"},
		{name: "noncanonical source", mutate: func(v *tgsrlv1.ComponentVersion) { v.Source = " source " }, field: "source"},
		{name: "empty attribute key", mutate: func(v *tgsrlv1.ComponentVersion) { v.Attributes = map[string]string{"": "x"} }, field: "attributes"},
		{name: "noncanonical attribute value", mutate: func(v *tgsrlv1.ComponentVersion) { v.Attributes = map[string]string{"region": " test "} }, field: "attributes"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, intent := validFixture()
			version := valid()
			test.mutate(version)
			intent.RequiredCapabilities.ComponentVersions = []*tgsrlv1.ComponentVersion{version}
			requirePR2ValidationError(t, intent, test.field)
		})
	}

	t.Run("valid unknown revision", func(t *testing.T) {
		_, intent := validFixture()
		version := valid()
		version.Revision = 0
		intent.RequiredCapabilities.ComponentVersions = []*tgsrlv1.ComponentVersion{version}
		refreshPR2ContractID(t, intent.ExecutionContract)
		if err := ValidateIntent(intent); err != nil {
			t.Fatalf("ValidateIntent(component version revision zero) error = %v", err)
		}
	})

	t.Run("duplicate capability identity", func(t *testing.T) {
		_, intent := validFixture()
		first := valid()
		second := proto.Clone(first).(*tgsrlv1.ComponentVersion)
		second.Name = "RUNTIME-PRIMARY"
		intent.RequiredCapabilities.ComponentVersions = []*tgsrlv1.ComponentVersion{first, second}
		requirePR2ValidationError(t, intent, "component_versions[1]")
	})

	t.Run("duplicate effective observation identity", func(t *testing.T) {
		_, intent := validFixture()
		first := valid()
		second := proto.Clone(first).(*tgsrlv1.ComponentVersion)
		second.Name = "RUNTIME-PRIMARY"
		intent.ContractObservation = validPR2ContractObservation()
		intent.ContractObservation.ComponentVersions = []*tgsrlv1.ComponentVersion{first, second}
		requirePR2ValidationError(t, intent, "contract_observation.component_versions[1]")
	})

	t.Run("snapshot capability component version", func(t *testing.T) {
		snapshot, _ := validFixture()
		version := valid()
		version.ObservedAt = nil
		snapshot.Devices[0].Capabilities.ComponentVersions = []*tgsrlv1.ComponentVersion{version}
		if err := validateSnapshot(snapshot); err == nil || !strings.Contains(err.Error(), "component_versions[0].observed_at") {
			t.Fatalf("validateSnapshot() error = %v, want component version timestamp error", err)
		}
	})
}

func TestPR2ObservedFactValidation(t *testing.T) {
	valid := func() *tgsrlv1.ObservedFact {
		return &tgsrlv1.ObservedFact{
			Fact: &tgsrlv1.SemanticField{
				Key:   "sample.policy_lag",
				Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: 1}},
			},
			ObservedAt: timestamppb.New(time.Unix(100, 0)),
			Source:     "runtime",
			Revision:   1,
		}
	}
	tests := []struct {
		name   string
		mutate func(*tgsrlv1.ObservedFact)
		field  string
	}{
		{name: "nil fact", mutate: func(f *tgsrlv1.ObservedFact) { f.Fact = nil }, field: "fact"},
		{name: "noncanonical key", mutate: func(f *tgsrlv1.ObservedFact) { f.Fact.Key = "sample.PolicyLag" }, field: "fact.key"},
		{name: "missing typed value", mutate: func(f *tgsrlv1.ObservedFact) { f.Fact.Value = &tgsrlv1.SemanticValue{} }, field: "fact.value"},
		{name: "missing timestamp", mutate: func(f *tgsrlv1.ObservedFact) { f.ObservedAt = nil }, field: "observed_at"},
		{name: "source canonical", mutate: func(f *tgsrlv1.ObservedFact) { f.Source = " runtime" }, field: "source"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, intent := validFixture()
			fact := valid()
			test.mutate(fact)
			intent.ContractObservation = validPR2ContractObservation()
			intent.ContractObservation.FactObservations = []*tgsrlv1.ObservedFact{fact}
			requirePR2ValidationError(t, intent, test.field)
		})
	}

	t.Run("valid unknown revision", func(t *testing.T) {
		_, intent := validFixture()
		fact := valid()
		fact.Revision = 0
		intent.ContractObservation = validPR2ContractObservation()
		intent.ContractObservation.FactObservations = []*tgsrlv1.ObservedFact{fact}
		refreshPR2ContractID(t, intent.ExecutionContract)
		if err := ValidateIntent(intent); err != nil {
			t.Fatalf("ValidateIntent(observed fact revision zero) error = %v", err)
		}
	})

	t.Run("duplicate path", func(t *testing.T) {
		_, intent := validFixture()
		first := valid()
		intent.ContractObservation = validPR2ContractObservation()
		intent.ContractObservation.FactObservations = []*tgsrlv1.ObservedFact{first, proto.Clone(first).(*tgsrlv1.ObservedFact)}
		requirePR2ValidationError(t, intent, "fact_observations[1].fact.key")
	})
}

func TestPR2ContractObservationEnvelopeValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tgsrlv1.ContractObservation)
		field  string
	}{
		{name: "invalid timestamp", mutate: func(o *tgsrlv1.ContractObservation) { o.ObservedAt = &timestamppb.Timestamp{Seconds: 253402300800} }, field: "contract_observation.observed_at"},
		{name: "noncanonical source", mutate: func(o *tgsrlv1.ContractObservation) { o.Source = " runtime " }, field: "contract_observation.source"},
		{name: "noncanonical event id", mutate: func(o *tgsrlv1.ContractObservation) { o.EventId = " event " }, field: "contract_observation.event_id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, intent := validFixture()
			intent.ContractObservation = validPR2ContractObservation()
			test.mutate(intent.ContractObservation)
			requirePR2ValidationError(t, intent, test.field)
		})
	}

	_, intent := validFixture()
	intent.ContractObservation = &tgsrlv1.ContractObservation{
		SafePoint: proto.Bool(true),
		TypedFacts: []*tgsrlv1.SemanticField{{
			Key:   "sample.policy_lag",
			Value: &tgsrlv1.SemanticValue{Kind: &tgsrlv1.SemanticValue_Uint64Value{Uint64Value: 1}},
		}},
	}
	refreshPR2ContractID(t, intent.ExecutionContract)
	if err := ValidateIntent(intent); err != nil {
		t.Fatalf("ValidateIntent(legacy sparse observation) error = %v", err)
	}
}

func validPR2ContractObservation() *tgsrlv1.ContractObservation {
	return &tgsrlv1.ContractObservation{
		ObservedAt: timestamppb.New(time.Unix(100, 0)),
		Source:     "runtime",
	}
}
