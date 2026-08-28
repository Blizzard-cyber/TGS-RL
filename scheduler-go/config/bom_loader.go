package config

func LoadBOM(path string) (*RuntimeBOM, error) {
	raw, err := loadYAMLSubset(path)
	if err != nil {
		return nil, err
	}
	data, err := mapping(raw, "bom")
	if err != nil {
		return nil, err
	}
	pinning, err := mapping(data["pinning_policy"], "bom.pinning_policy")
	if err != nil {
		return nil, err
	}
	toolchain, err := dependencyRecords(data["toolchain"], "bom.toolchain")
	if err != nil {
		return nil, err
	}
	runtimeDependencies, err := dependencyRecords(data["runtime_dependencies"], "bom.runtime_dependencies")
	if err != nil {
		return nil, err
	}
	developmentImages, err := developmentImages(data["development_images"], "bom.development_images")
	if err != nil {
		return nil, err
	}
	schemaVersion, err := mustString(data["schema_version"], "bom.schema_version")
	if err != nil {
		return nil, err
	}
	bomID, err := mustString(data["bom_id"], "bom.bom_id")
	if err != nil {
		return nil, err
	}
	revision, err := mustInt(data["revision"], "bom.revision")
	if err != nil {
		return nil, err
	}
	status, err := mustString(data["status"], "bom.status")
	if err != nil {
		return nil, err
	}
	scope, err := mustString(data["scope"], "bom.scope")
	if err != nil {
		return nil, err
	}
	floatingVersionsAllowed, err := mustBool(pinning["floating_versions_allowed"], "bom.pinning_policy.floating_versions_allowed")
	if err != nil {
		return nil, err
	}
	immutableImageDigestRequiredForRelease, err := mustBool(pinning["immutable_image_digest_required_for_release"], "bom.pinning_policy.immutable_image_digest_required_for_release")
	if err != nil {
		return nil, err
	}
	unknownCommitOrDigestValue, err := optionalString(pinning["unknown_commit_or_digest_value"], "bom.pinning_policy.unknown_commit_or_digest_value")
	if err != nil {
		return nil, err
	}
	note, err := mustString(pinning["note"], "bom.pinning_policy.note")
	if err != nil {
		return nil, err
	}
	bom := &RuntimeBOM{
		Path:          absPath(path),
		SchemaVersion: schemaVersion,
		BOMID:         bomID,
		Revision:      revision,
		Status:        status,
		Scope:         scope,
		PinningPolicy: PinningPolicy{
			FloatingVersionsAllowed:                floatingVersionsAllowed,
			ImmutableImageDigestRequiredForRelease: immutableImageDigestRequiredForRelease,
			UnknownCommitOrDigestValue:             unknownCommitOrDigestValue,
			Note:                                   note,
		},
		Toolchain:           toolchain,
		RuntimeDependencies: runtimeDependencies,
		DevelopmentImages:   developmentImages,
	}
	if err := expectSchema(bom.SchemaVersion, "bom", bom.Path); err != nil {
		return nil, err
	}
	if err := validateBOM(bom); err != nil {
		return nil, err
	}
	return bom, nil
}
