package config

import (
	"fmt"
	"strconv"
)

func LoadBundle(root string) (*Bundle, error) {
	return LoadBundleWithOptions(LoadOptions{Root: root})
}

func LoadStartupConfig(options LoadOptions) (*StartupConfig, error) {
	bundle, err := LoadBundleWithOptions(options)
	if err != nil {
		return nil, err
	}
	return bundle.StartupConfig()
}

func (b *Bundle) StartupConfig() (*StartupConfig, error) {
	if b == nil {
		return nil, configError("bundle is required", "config_error", "", "bundle", nil)
	}
	fallbackMode, err := deriveFallbackMode(b.Policy)
	if err != nil {
		return nil, err
	}
	intervals, err := deriveRuntimeIntervals(b.Overrides.Env)
	if err != nil {
		return nil, err
	}
	providerKind := b.Profile.Provider.Kind
	if override := b.Overrides.Env[DefaultEnvPrefix+"PROVIDER_KIND"]; override != "" {
		providerKind = override
	}
	strategy := b.Policy.Selection.Strategy
	if override := b.Overrides.Env[DefaultEnvPrefix+"STRATEGY"]; override != "" {
		strategy = override
	}
	topK := b.Policy.Selection.TopK
	if override := b.Overrides.Env[DefaultEnvPrefix+"TOP_K"]; override != "" {
		parsedTopK, parseErr := strconv.Atoi(override)
		if parseErr != nil || parsedTopK < 1 {
			return nil, configError(
				fmt.Sprintf("startup top_k override must be a positive integer, got %q", override),
				"schema_validation",
				b.Policy.Path,
				DefaultEnvPrefix+"TOP_K",
				nil,
			)
		}
		topK = parsedTopK
	}
	if topK < 1 {
		return nil, configError("startup top_k must be at least 1", "schema_validation", b.Policy.Path, "top_k", nil)
	}
	if !isSupportedStrategy(strategy) {
		return nil, configError(
			fmt.Sprintf("unsupported selection strategy %q", strategy),
			"schema_validation",
			b.Policy.Path,
			"policy.selection.strategy",
			nil,
		)
	}
	return &StartupConfig{
		Root:               b.Root,
		ManifestPath:       b.Manifest.Path,
		ProviderKind:       providerKind,
		ProviderSource:     b.Profile.Provider.Source,
		Capabilities:       b.Capabilities,
		FallbackMode:       fallbackMode,
		SelectionStrategy:  strategy,
		TopK:               topK,
		RuntimeIntervals:   intervals,
		Policy:             b.Policy,
		AppliedEnvOverride: cloneEnv(b.Overrides.Env),
	}, nil
}
