package config

func LoadManifest(path string) (*CompatibilityManifest, error) {
	raw, err := loadYAMLSubset(path)
	if err != nil {
		return nil, err
	}
	data, err := mapping(raw, "manifest")
	if err != nil {
		return nil, err
	}
	references, err := mapping(data["references"], "manifest.references")
	if err != nil {
		return nil, err
	}
	combination, err := mapping(data["combination"], "manifest.combination")
	if err != nil {
		return nil, err
	}
	matrix, err := mapping(data["declared_matrix"], "manifest.declared_matrix")
	if err != nil {
		return nil, err
	}
	framework, err := componentRef(combination["framework"], "manifest.combination.framework")
	if err != nil {
		return nil, err
	}
	executionBackend, err := componentRef(combination["execution_backend"], "manifest.combination.execution_backend")
	if err != nil {
		return nil, err
	}
	trainer, err := componentRef(combination["trainer"], "manifest.combination.trainer")
	if err != nil {
		return nil, err
	}
	rolloutEngine, err := componentRef(combination["rollout_engine"], "manifest.combination.rollout_engine")
	if err != nil {
		return nil, err
	}
	resourceProvider, err := resourceProviderRef(combination["resource_provider"], "manifest.combination.resource_provider")
	if err != nil {
		return nil, err
	}
	kubernetes, err := kubernetesRef(combination["kubernetes"], "manifest.combination.kubernetes")
	if err != nil {
		return nil, err
	}
	algorithms, err := stringSlice(matrix["algorithms"], "manifest.declared_matrix.algorithms", false)
	if err != nil {
		return nil, err
	}
	rolloutModes, err := stringSlice(matrix["rollout_modes"], "manifest.declared_matrix.rollout_modes", false)
	if err != nil {
		return nil, err
	}
	dataKinds, err := stringSlice(matrix["data_kinds"], "manifest.declared_matrix.data_kinds", false)
	if err != nil {
		return nil, err
	}
	evidence, err := stringSlice(matrix["evidence"], "manifest.declared_matrix.evidence", false)
	if err != nil {
		return nil, err
	}
	schemaVersion, err := mustString(data["schema_version"], "manifest.schema_version")
	if err != nil {
		return nil, err
	}
	manifestID, err := mustString(data["manifest_id"], "manifest.manifest_id")
	if err != nil {
		return nil, err
	}
	revision, err := mustInt(data["revision"], "manifest.revision")
	if err != nil {
		return nil, err
	}
	status, err := mustString(data["status"], "manifest.status")
	if err != nil {
		return nil, err
	}
	scope, err := mustString(data["scope"], "manifest.scope")
	if err != nil {
		return nil, err
	}
	bomRef, err := mustString(references["bom"], "manifest.references.bom")
	if err != nil {
		return nil, err
	}
	profileRef, err := mustString(references["profile"], "manifest.references.profile")
	if err != nil {
		return nil, err
	}
	capabilitiesRef, err := mustString(references["capabilities"], "manifest.references.capabilities")
	if err != nil {
		return nil, err
	}
	policyRef, err := mustString(references["policy"], "manifest.references.policy")
	if err != nil {
		return nil, err
	}
	representativeScenario, err := mustString(references["representative_scenario"], "manifest.references.representative_scenario")
	if err != nil {
		return nil, err
	}
	patchLedger, err := mustString(references["patch_ledger"], "manifest.references.patch_ledger")
	if err != nil {
		return nil, err
	}
	gpuManagementProfile, err := mustString(combination["gpu_management_profile"], "manifest.combination.gpu_management_profile")
	if err != nil {
		return nil, err
	}
	evidenceStatus, err := mustString(matrix["evidence_status"], "manifest.declared_matrix.evidence_status")
	if err != nil {
		return nil, err
	}
	manifest := &CompatibilityManifest{
		Path:          absPath(path),
		SchemaVersion: schemaVersion,
		ManifestID:    manifestID,
		Revision:      revision,
		Status:        status,
		Scope:         scope,
		References: ReferenceSet{
			BOM:                    bomRef,
			Profile:                profileRef,
			Capabilities:           capabilitiesRef,
			Policy:                 policyRef,
			RepresentativeScenario: representativeScenario,
			PatchLedger:            patchLedger,
		},
		Combination: Combination{
			Framework:            *framework,
			ExecutionBackend:     *executionBackend,
			Trainer:              *trainer,
			RolloutEngine:        *rolloutEngine,
			ResourceProvider:     *resourceProvider,
			Kubernetes:           *kubernetes,
			GPUManagementProfile: gpuManagementProfile,
		},
		DeclaredMatrix: DeclaredMatrix{
			Algorithms:     algorithms,
			RolloutModes:   rolloutModes,
			DataKinds:      dataKinds,
			EvidenceStatus: evidenceStatus,
			Evidence:       evidence,
		},
	}
	if err := expectSchema(manifest.SchemaVersion, "manifest", manifest.Path); err != nil {
		return nil, err
	}
	return manifest, nil
}
