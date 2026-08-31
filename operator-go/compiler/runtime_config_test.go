package compiler

import (
	"strings"
	"testing"
)

func TestValidateRuntimeClassName(t *testing.T) {
	maxLengthName := strings.Join([]string{
		strings.Repeat("a", 63),
		strings.Repeat("b", 63),
		strings.Repeat("c", 63),
		strings.Repeat("d", 61),
	}, ".")
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "single label", value: "kata-gpu"},
		{name: "DNS subdomain", value: "kata.gpu.example.com"},
		{name: "numeric", value: "123"},
		{name: "maximum length", value: maxLengthName},
		{name: "empty", value: "", wantErr: true},
		{name: "leading whitespace", value: " kata", wantErr: true},
		{name: "embedded whitespace", value: "kata gpu", wantErr: true},
		{name: "slash", value: "example.com/kata", wantErr: true},
		{name: "uppercase", value: "Kata", wantErr: true},
		{name: "underscore", value: "kata_gpu", wantErr: true},
		{name: "empty DNS label", value: "kata..gpu", wantErr: true},
		{name: "label too long", value: strings.Repeat("a", 64) + ".example", wantErr: true},
		{name: "subdomain too long", value: maxLengthName + "a", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRuntimeClassName(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateRuntimeClassName(%q) error = %v, wantErr %v", test.value, err, test.wantErr)
			}
		})
	}
}

func TestValidateRuntimeClassHandler(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "runc", value: "runc"},
		{name: "hyphenated", value: "kata-qemu-2"},
		{name: "maximum length", value: strings.Repeat("a", 63)},
		{name: "empty", value: "", wantErr: true},
		{name: "leading whitespace", value: " runc", wantErr: true},
		{name: "embedded whitespace", value: "kata qemu", wantErr: true},
		{name: "slash", value: "example.com/runc", wantErr: true},
		{name: "dot", value: "kata.qemu", wantErr: true},
		{name: "uppercase", value: "RunC", wantErr: true},
		{name: "underscore", value: "kata_qemu", wantErr: true},
		{name: "too long", value: strings.Repeat("a", 64), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateRuntimeClassHandler(test.value)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateRuntimeClassHandler(%q) error = %v, wantErr %v", test.value, err, test.wantErr)
			}
		})
	}
}

func TestValidateRuntimeConfigRejectsInvalidRuntimeClass(t *testing.T) {
	tests := []struct {
		name   string
		config RuntimeClassConfig
	}{
		{name: "invalid reference name", config: RuntimeClassConfig{Name: "bad/name"}},
		{name: "name whitespace", config: RuntimeClassConfig{Name: " kata"}},
		{name: "invalid handler without create", config: RuntimeClassConfig{Name: "kata", Handler: "bad/handler"}},
		{name: "handler whitespace", config: RuntimeClassConfig{Name: "kata", Handler: " runc", Create: true}},
		{name: "creation missing handler", config: RuntimeClassConfig{Name: "kata", Create: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ValidateRuntimeConfig(RuntimeConfig{RuntimeClass: test.config}); err == nil {
				t.Fatalf("ValidateRuntimeConfig(%+v) succeeded, want error", test.config)
			}
		})
	}
}

func TestNewWithRuntimeConfigRejectsInvalidRuntimeClass(t *testing.T) {
	tests := []RuntimeClassConfig{
		{Name: "bad/name"},
		{Name: "kata", Handler: "bad/handler", Create: true},
		{Name: "kata", Handler: strings.Repeat("a", 64), Create: true},
	}
	for _, runtimeClass := range tests {
		if _, err := NewWithRuntimeConfig(RuntimeConfig{RuntimeClass: runtimeClass}); err == nil {
			t.Fatalf("NewWithRuntimeConfig(%+v) succeeded, want error", runtimeClass)
		}
	}
}

func TestValidateRuntimeConfigRequiresCompleteBootstrapContract(t *testing.T) {
	valid := WorkerBootstrapConfig{
		Enabled:            true,
		InstallerImage:     "registry.example.test/bootstrap@sha256:" + strings.Repeat("a", 64),
		RegistryURL:        "https://scheduler.example.test:50091",
		RegistrySigningKey: []byte(strings.Repeat("k", 32)),
	}
	if _, err := ValidateRuntimeConfig(RuntimeConfig{Bootstrap: valid}); err != nil {
		t.Fatalf("valid bootstrap config error = %v", err)
	}
	for name, mutate := range map[string]func(*WorkerBootstrapConfig){
		"mutable image":     func(value *WorkerBootstrapConfig) { value.InstallerImage = "bootstrap:latest" },
		"missing URL":       func(value *WorkerBootstrapConfig) { value.RegistryURL = "" },
		"invalid URL":       func(value *WorkerBootstrapConfig) { value.RegistryURL = "scheduler:50091" },
		"short signing key": func(value *WorkerBootstrapConfig) { value.RegistrySigningKey = []byte("short") },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			mutate(&candidate)
			if _, err := ValidateRuntimeConfig(RuntimeConfig{Bootstrap: candidate}); err == nil {
				t.Fatal("expected invalid bootstrap config")
			}
		})
	}
}
