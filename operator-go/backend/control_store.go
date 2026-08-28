package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type controlLedger struct {
	Revision uint64                   `json:"revision"`
	Records  map[string]controlRecord `json:"records"`
}

func loadControlLedger(path string) (controlLedger, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return controlLedger{}, nil
		}
		return controlLedger{}, err
	}
	var ledger controlLedger
	if err := json.Unmarshal(payload, &ledger); err != nil {
		return controlLedger{}, fmt.Errorf("decode backend control state: %w", err)
	}
	return ledger, nil
}

// SetControlStatePath enables durable lifecycle idempotency for the
// single-process backend. Kubernetes object preconditions still fence the
// actual external mutations.
func (b *KubernetesBackend) SetControlStatePath(path string) error {
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	b.controlStatePath = path
	if path == "" {
		return nil
	}
	ledger, err := loadControlLedger(path)
	if err != nil {
		return err
	}
	b.revision = ledger.Revision
	if ledger.Records == nil {
		ledger.Records = make(map[string]controlRecord)
	}
	b.controls = ledger.Records
	return nil
}

// RestoreControlMetadata rehydrates observable bundle annotations from the
// durable idempotency ledger after backend objects become available.
func (b *KubernetesBackend) RestoreControlMetadata(ctx context.Context) error {
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	latestByBundle := make(map[string]ControlMetadata)
	for _, record := range b.controls {
		if record.Pending || record.Result == nil || !record.Result.Accepted || len(record.BundleKeys) == 0 {
			continue
		}
		for _, key := range record.BundleKeys {
			if current, found := latestByBundle[key]; !found || record.Result.BackendRevision > current.BackendRevision {
				latestByBundle[key] = metadataForControl(record.Request, record.Result.BackendRevision, true)
			}
		}
	}
	bundleKeys := make([]string, 0, len(latestByBundle))
	for key := range latestByBundle {
		bundleKeys = append(bundleKeys, key)
	}
	sort.Strings(bundleKeys)
	for _, key := range bundleKeys {
		_, found, err := b.client.Get(ctx, b.adapter.BundleObject(key))
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if err := b.setBundleControlMetadata(ctx, []string{key}, latestByBundle[key]); err != nil {
			return err
		}
	}
	return nil
}

func (b *KubernetesBackend) persistControlStateLocked() error {
	if b.controlStatePath == "" {
		return nil
	}
	ledger, err := loadControlLedger(b.controlStatePath)
	if err != nil {
		return err
	}
	if ledger.Records == nil {
		ledger.Records = make(map[string]controlRecord)
	}
	for key, record := range b.controls {
		ledger.Records[key] = record
	}
	if b.revision > ledger.Revision {
		ledger.Revision = b.revision
	}
	payload, err := json.Marshal(ledger)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(b.controlStatePath), 0o755); err != nil {
		return err
	}
	tmp := b.controlStatePath + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, b.controlStatePath); err != nil {
		return fmt.Errorf("rename backend control state: %w", err)
	}
	directory, err := os.Open(filepath.Dir(b.controlStatePath))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (b *FakeBackend) SetControlStatePath(path string) error {
	return b.inner.SetControlStatePath(path)
}

func (b *FakeBackend) RestoreControlMetadata(ctx context.Context) error {
	return b.inner.RestoreControlMetadata(ctx)
}
