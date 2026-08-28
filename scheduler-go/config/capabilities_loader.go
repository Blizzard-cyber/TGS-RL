package config

func LoadCapabilities(path string) (*CapabilitySetConfig, error) {
	raw, err := loadYAMLSubset(path)
	if err != nil {
		return nil, err
	}
	data, err := mapping(raw, "capabilities")
	if err != nil {
		return nil, err
	}
	claims, err := mapping(data["claims"], "capabilities.claims")
	if err != nil {
		return nil, err
	}
	attributesNode, err := mapping(data["attributes"], "capabilities.attributes")
	if err != nil {
		return nil, err
	}
	attributes := make(map[string]string, len(attributesNode))
	for key, value := range attributesNode {
		text, valueErr := mustString(value, "capabilities.attributes."+key)
		if valueErr != nil {
			return nil, valueErr
		}
		attributes[key] = text
	}
	limitsNode, err := mapping(data["limits"], "capabilities.limits")
	if err != nil {
		return nil, err
	}
	limits := make(map[string]float64, len(limitsNode))
	for key, value := range limitsNode {
		number, valueErr := mustFloat(value, "capabilities.limits."+key)
		if valueErr != nil {
			return nil, valueErr
		}
		limits[key] = number
	}
	names, err := stringSlice(data["names"], "capabilities.names", false)
	if err != nil {
		return nil, err
	}
	algorithms, err := stringSlice(data["algorithms"], "capabilities.algorithms", false)
	if err != nil {
		return nil, err
	}
	rolloutModes, err := stringSlice(data["rollout_modes"], "capabilities.rollout_modes", false)
	if err != nil {
		return nil, err
	}
	supportedActions, err := stringSlice(data["supported_actions"], "capabilities.supported_actions", false)
	if err != nil {
		return nil, err
	}
	schemaVersion, err := mustString(data["schema_version"], "capabilities.schema_version")
	if err != nil {
		return nil, err
	}
	capabilityID, err := mustString(data["capability_id"], "capabilities.capability_id")
	if err != nil {
		return nil, err
	}
	revision, err := mustInt(data["revision"], "capabilities.revision")
	if err != nil {
		return nil, err
	}
	source, err := mustString(data["source"], "capabilities.source")
	if err != nil {
		return nil, err
	}
	measuredAt, err := optionalTimestamp(data["measured_at"], "capabilities.measured_at")
	if err != nil {
		return nil, err
	}
	semantics, err := mustString(data["semantics"], "capabilities.semantics")
	if err != nil {
		return nil, err
	}
	verificationStatus, err := mustString(data["verification_status"], "capabilities.verification_status")
	if err != nil {
		return nil, err
	}
	runtimeLoader, err := mustBool(data["runtime_loader"], "capabilities.runtime_loader")
	if err != nil {
		return nil, err
	}
	mockOnly, err := mustBool(claims["mock_only"], "capabilities.claims.mock_only")
	if err != nil {
		return nil, err
	}
	liveGPU, err := mustBool(claims["live_gpu"], "capabilities.claims.live_gpu")
	if err != nil {
		return nil, err
	}
	hardwareLatency, err := mustBool(claims["hardware_latency"], "capabilities.claims.hardware_latency")
	if err != nil {
		return nil, err
	}
	performance, err := mustBool(claims["performance"], "capabilities.claims.performance")
	if err != nil {
		return nil, err
	}
	capabilities := &CapabilitySetConfig{
		Path:               absPath(path),
		SchemaVersion:      schemaVersion,
		CapabilityID:       capabilityID,
		Revision:           revision,
		Source:             source,
		MeasuredAt:         measuredAt,
		Semantics:          semantics,
		VerificationStatus: verificationStatus,
		RuntimeLoader:      runtimeLoader,
		Names:              names,
		Attributes:         attributes,
		Limits:             limits,
		Algorithms:         algorithms,
		RolloutModes:       rolloutModes,
		SupportedActions:   supportedActions,
		Claims: CapabilityClaims{
			MockOnly:        mockOnly,
			LiveGPU:         liveGPU,
			HardwareLatency: hardwareLatency,
			Performance:     performance,
		},
	}
	if err := expectSchema(capabilities.SchemaVersion, "capabilities", capabilities.Path); err != nil {
		return nil, err
	}
	return capabilities, nil
}
