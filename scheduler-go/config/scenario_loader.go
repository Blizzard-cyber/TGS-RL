package config

func LoadScenario(path string) (*SyntheticScenarioConfig, error) {
	raw, err := loadYAMLSubset(path)
	if err != nil {
		return nil, err
	}
	data, err := mapping(raw, "scenario")
	if err != nil {
		return nil, err
	}
	references, err := mapping(data["references"], "scenario.references")
	if err != nil {
		return nil, err
	}
	workload, err := mapping(data["workload"], "scenario.workload")
	if err != nil {
		return nil, err
	}
	topology, err := mapping(data["topology"], "scenario.topology")
	if err != nil {
		return nil, err
	}
	schemaVersion, err := mustString(data["schema_version"], "scenario.schema_version")
	if err != nil {
		return nil, err
	}
	scenarioID, err := mustString(data["scenario_id"], "scenario.scenario_id")
	if err != nil {
		return nil, err
	}
	scenarioRevision, err := mustInt(data["scenario_revision"], "scenario.scenario_revision")
	if err != nil {
		return nil, err
	}
	status, err := mustString(data["status"], "scenario.status")
	if err != nil {
		return nil, err
	}
	runtimeLoader, err := mustBool(data["runtime_loader"], "scenario.runtime_loader")
	if err != nil {
		return nil, err
	}
	dataKind, err := mustString(data["data_kind"], "scenario.data_kind")
	if err != nil {
		return nil, err
	}
	seed, err := mustInt(data["seed"], "scenario.seed")
	if err != nil {
		return nil, err
	}
	compatibilityManifest, err := mustString(references["compatibility_manifest"], "scenario.references.compatibility_manifest")
	if err != nil {
		return nil, err
	}
	resourceProfile, err := mustString(references["resource_profile"], "scenario.references.resource_profile")
	if err != nil {
		return nil, err
	}
	capabilitiesRef, err := mustString(references["capabilities"], "scenario.references.capabilities")
	if err != nil {
		return nil, err
	}
	policyRef, err := mustString(references["policy"], "scenario.references.policy")
	if err != nil {
		return nil, err
	}
	algorithm, err := mustString(workload["algorithm"], "scenario.workload.algorithm")
	if err != nil {
		return nil, err
	}
	rolloutMode, err := mustString(workload["rollout_mode"], "scenario.workload.rollout_mode")
	if err != nil {
		return nil, err
	}
	executionID, err := mustString(workload["execution_id"], "scenario.workload.execution_id")
	if err != nil {
		return nil, err
	}
	stageID, err := mustString(workload["stage_id"], "scenario.workload.stage_id")
	if err != nil {
		return nil, err
	}
	unitCount, err := mustInt(workload["unit_count"], "scenario.workload.unit_count")
	if err != nil {
		return nil, err
	}
	providerSource, err := mustString(topology["provider_source"], "scenario.topology.provider_source")
	if err != nil {
		return nil, err
	}
	scenario := &SyntheticScenarioConfig{
		Path:             absPath(path),
		SchemaVersion:    schemaVersion,
		ScenarioID:       scenarioID,
		ScenarioRevision: scenarioRevision,
		Status:           status,
		RuntimeLoader:    runtimeLoader,
		DataKind:         dataKind,
		Seed:             seed,
		References: ScenarioReferences{
			CompatibilityManifest: compatibilityManifest,
			ResourceProfile:       resourceProfile,
			Capabilities:          capabilitiesRef,
			Policy:                policyRef,
		},
		Workload: ScenarioWorkload{
			Algorithm:   algorithm,
			RolloutMode: rolloutMode,
			ExecutionID: executionID,
			StageID:     stageID,
			UnitCount:   unitCount,
		},
		Topology: Topology{
			ProviderSource: providerSource,
		},
	}
	if err := expectSchema(scenario.SchemaVersion, "scenario", scenario.Path); err != nil {
		return nil, err
	}
	return scenario, nil
}
