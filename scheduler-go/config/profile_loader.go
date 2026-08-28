package config

func LoadProfile(path string) (*CompatibilityProfile, error) {
	raw, err := loadYAMLSubset(path)
	if err != nil {
		return nil, err
	}
	data, err := mapping(raw, "profile")
	if err != nil {
		return nil, err
	}
	provider, err := mapping(data["provider"], "profile.provider")
	if err != nil {
		return nil, err
	}
	runtimeScope, err := mapping(data["runtime_scope"], "profile.runtime_scope")
	if err != nil {
		return nil, err
	}
	gpu, err := mapping(data["gpu_management_profile"], "profile.gpu_management_profile")
	if err != nil {
		return nil, err
	}
	requiredHostResources, err := stringSlice(runtimeScope["required_host_resources"], "profile.runtime_scope.required_host_resources", false)
	if err != nil {
		return nil, err
	}
	dataKinds, err := stringSlice(runtimeScope["data_kinds"], "profile.runtime_scope.data_kinds", false)
	if err != nil {
		return nil, err
	}
	algorithms, err := stringSlice(runtimeScope["algorithms"], "profile.runtime_scope.algorithms", false)
	if err != nil {
		return nil, err
	}
	rolloutModes, err := stringSlice(runtimeScope["rollout_modes"], "profile.runtime_scope.rollout_modes", false)
	if err != nil {
		return nil, err
	}
	lifecycleActions, err := stringSlice(runtimeScope["lifecycle_actions"], "profile.runtime_scope.lifecycle_actions", false)
	if err != nil {
		return nil, err
	}
	allowedValues, err := stringSlice(gpu["allowed_values"], "profile.gpu_management_profile.allowed_values", false)
	if err != nil {
		return nil, err
	}
	enabledProfiles, err := stringSlice(gpu["enabled_profiles"], "profile.gpu_management_profile.enabled_profiles", true)
	if err != nil {
		return nil, err
	}
	schemaVersion, err := mustString(data["schema_version"], "profile.schema_version")
	if err != nil {
		return nil, err
	}
	profileID, err := mustString(data["profile_id"], "profile.profile_id")
	if err != nil {
		return nil, err
	}
	revision, err := mustInt(data["revision"], "profile.revision")
	if err != nil {
		return nil, err
	}
	status, err := mustString(data["status"], "profile.status")
	if err != nil {
		return nil, err
	}
	description, err := mustString(data["description"], "profile.description")
	if err != nil {
		return nil, err
	}
	providerKind, err := mustString(provider["kind"], "profile.provider.kind")
	if err != nil {
		return nil, err
	}
	providerSource, err := mustString(provider["source"], "profile.provider.source")
	if err != nil {
		return nil, err
	}
	liveHardware, err := mustBool(provider["live_hardware"], "profile.provider.live_hardware")
	if err != nil {
		return nil, err
	}
	authoritativeFor, err := mustString(provider["authoritative_for"], "profile.provider.authoritative_for")
	if err != nil {
		return nil, err
	}
	selected, err := mustString(gpu["selected"], "profile.gpu_management_profile.selected")
	if err != nil {
		return nil, err
	}
	mutuallyExclusive, err := mustBool(gpu["mutually_exclusive"], "profile.gpu_management_profile.mutually_exclusive")
	if err != nil {
		return nil, err
	}
	maximumActiveProfiles, err := mustInt(gpu["maximum_active_profiles"], "profile.gpu_management_profile.maximum_active_profiles")
	if err != nil {
		return nil, err
	}
	admissionRejectsMultipleProfiles, err := mustBool(gpu["admission_rejects_multiple_profiles"], "profile.gpu_management_profile.admission_rejects_multiple_profiles")
	if err != nil {
		return nil, err
	}
	profile := &CompatibilityProfile{
		Path:          absPath(path),
		SchemaVersion: schemaVersion,
		ProfileID:     profileID,
		Revision:      revision,
		Status:        status,
		Description:   description,
		Provider: ProviderConfig{
			Kind:             providerKind,
			Source:           providerSource,
			LiveHardware:     liveHardware,
			AuthoritativeFor: authoritativeFor,
		},
		RuntimeScope: RuntimeScope{
			RequiredHostResources: requiredHostResources,
			DataKinds:             dataKinds,
			Algorithms:            algorithms,
			RolloutModes:          rolloutModes,
			LifecycleActions:      lifecycleActions,
		},
		GPUManagementProfile: GPUManagementProfile{
			Selected:                         selected,
			AllowedValues:                    allowedValues,
			MutuallyExclusive:                mutuallyExclusive,
			MaximumActiveProfiles:            maximumActiveProfiles,
			AdmissionRejectsMultipleProfiles: admissionRejectsMultipleProfiles,
			EnabledProfiles:                  enabledProfiles,
		},
	}
	if err := expectSchema(profile.SchemaVersion, "profile", profile.Path); err != nil {
		return nil, err
	}
	if err := validateProfile(profile); err != nil {
		return nil, err
	}
	return profile, nil
}
