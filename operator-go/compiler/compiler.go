package compiler

import "github.com/Blizzard-cyber/TGS-RL/operator-go/api"

const (
	GPUProfileNone               = "none"
	GPUProfileNVIDIADevicePlugin = "nvidia-device-plugin"
	GPUProfileKubernetesDRA      = "kubernetes-dra"
	GPUProfileVolcanoHAMI        = "volcano-hami"
)

type Compiler struct {
	runtimeConfig RuntimeConfig
}

func New() *Compiler {
	return &Compiler{}
}

func NewWithRuntimeConfig(config RuntimeConfig) (*Compiler, error) {
	validated, err := ValidateRuntimeConfig(config)
	if err != nil {
		return nil, err
	}
	return &Compiler{runtimeConfig: validated}, nil
}

func (c *Compiler) Compile(input CompileInput) (*api.Bundle, error) {
	normalized, err := normalize(input, c.runtimeConfig)
	if err != nil {
		return nil, err
	}
	bundle, err := buildBundle(normalized)
	if err != nil {
		return nil, err
	}
	if err := validateBundle(bundle); err != nil {
		return nil, err
	}
	fingerprint, err := bundleFingerprint(bundle)
	if err != nil {
		return nil, err
	}
	bundle.Fingerprint = fingerprint
	return bundle, nil
}
