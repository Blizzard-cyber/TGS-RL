package config

import "path/filepath"

func LoadBundleWithOptions(options LoadOptions) (*Bundle, error) {
	repoRoot := options.Root
	if repoRoot == "" {
		repoRoot = "."
	}
	absRoot, err := filepath.Abs(repoRoot)
	if err != nil {
		return nil, err
	}
	manifestPath := options.ManifestPath
	if manifestPath == "" {
		manifestPath = DefaultManifestPath
	}
	envPrefix := options.EnvPrefix
	if envPrefix == "" {
		envPrefix = DefaultEnvPrefix
	}
	envValues := cloneEnv(options.Env)
	overrides := readEnvOverrides(envPrefix, envValues)
	if overrides.ManifestPath != "" {
		manifestPath = overrides.ManifestPath
	}
	manifestFile, err := resolveExistingPath(absRoot, filepath.Join(absRoot, manifestPath), manifestPath)
	if err != nil {
		return nil, err
	}
	manifest, err := LoadManifest(manifestFile)
	if err != nil {
		return nil, err
	}
	bomPath := manifest.References.BOM
	if overrides.BOMPath != "" {
		bomPath = overrides.BOMPath
	}
	profilePath := manifest.References.Profile
	if overrides.ProfilePath != "" {
		profilePath = overrides.ProfilePath
	}
	capabilitiesPath := manifest.References.Capabilities
	if overrides.CapabilitiesPath != "" {
		capabilitiesPath = overrides.CapabilitiesPath
	}
	policyPath := manifest.References.Policy
	if overrides.PolicyPath != "" {
		policyPath = overrides.PolicyPath
	}
	scenarioPath := manifest.References.RepresentativeScenario
	if overrides.ScenarioPath != "" {
		scenarioPath = overrides.ScenarioPath
	}
	bomFile, err := resolveExistingPath(absRoot, manifest.Path, bomPath)
	if err != nil {
		return nil, err
	}
	bom, err := LoadBOM(bomFile)
	if err != nil {
		return nil, err
	}
	profileFile, err := resolveExistingPath(absRoot, manifest.Path, profilePath)
	if err != nil {
		return nil, err
	}
	profile, err := LoadProfile(profileFile)
	if err != nil {
		return nil, err
	}
	capabilitiesFile, err := resolveExistingPath(absRoot, manifest.Path, capabilitiesPath)
	if err != nil {
		return nil, err
	}
	capabilities, err := LoadCapabilities(capabilitiesFile)
	if err != nil {
		return nil, err
	}
	policyFile, err := resolveExistingPath(absRoot, manifest.Path, policyPath)
	if err != nil {
		return nil, err
	}
	policy, err := LoadPolicy(policyFile)
	if err != nil {
		return nil, err
	}
	scenarioFile, err := resolveExistingPath(absRoot, manifest.Path, scenarioPath)
	if err != nil {
		return nil, err
	}
	scenario, err := LoadScenario(scenarioFile)
	if err != nil {
		return nil, err
	}
	bundle := &Bundle{
		Root:         absRoot,
		Manifest:     *manifest,
		BOM:          *bom,
		Profile:      *profile,
		Capabilities: *capabilities,
		Policy:       *policy,
		Scenario:     *scenario,
		Overrides:    overrides,
	}
	if err := validateBundle(bundle); err != nil {
		return nil, err
	}
	return bundle, nil
}
