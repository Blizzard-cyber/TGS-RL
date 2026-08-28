package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

const (
	controlRequestIDAnnotation   = "tgsrl.io/control-request-id"
	controlIdempotencyAnnotation = "tgsrl.io/control-idempotency-key"
	controlActionAnnotation      = "tgsrl.io/control-action"
	controlRevisionAnnotation    = "tgsrl.io/control-backend-revision"
	controlCommittedAnnotation   = "tgsrl.io/control-committed"
)

// ControlMetadata is persisted on each materialized JobRunBundle so the
// observation path can recover lifecycle causality without an in-memory side
// channel from ControlService.
type ControlMetadata struct {
	RequestID       string
	IdempotencyKey  string
	Action          tgsrlv1.JobCommandType
	BackendRevision uint64
	Committed       bool
}

func metadataForControl(request ControlRequest, revision uint64, committed bool) ControlMetadata {
	return ControlMetadata{RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, Action: request.Action, BackendRevision: revision, Committed: committed}
}

func (b *KubernetesBackend) writeControlMetadata(ctx context.Context, bundleKeys []string, metadata ControlMetadata) error {
	updatedObjects := make([]ClientObject, 0, len(bundleKeys))
	originalObjects := make([]ClientObject, 0, len(bundleKeys))
	for _, key := range bundleKeys {
		object, found, err := b.client.Get(ctx, b.adapter.BundleObject(key))
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: bundle %q", ErrRuntimeNotFound, key)
		}
		updated, err := encodeControlMetadata(*object, metadata)
		if err != nil {
			return err
		}
		originalObjects = append(originalObjects, *object)
		updatedObjects = append(updatedObjects, updated)
	}
	for index, updated := range updatedObjects {
		if _, _, err := b.client.Upsert(ctx, updated); err != nil {
			for rollbackIndex := index - 1; rollbackIndex >= 0; rollbackIndex-- {
				_, _, _ = b.client.Upsert(context.WithoutCancel(ctx), originalObjects[rollbackIndex])
			}
			return err
		}
	}
	return nil
}

func (b *KubernetesBackend) setBundleControlMetadata(ctx context.Context, bundleKeys []string, metadata ControlMetadata) error {
	if memory, ok := b.client.(*MemoryClient); ok {
		return memory.SetBundleControlMetadata(ctx, bundleKeys, metadata)
	}
	return b.writeControlMetadata(ctx, bundleKeys, metadata)
}

func encodeControlMetadata(object ClientObject, metadata ControlMetadata) (ClientObject, error) {
	existing, found, err := DecodeControlMetadata(object.Payload)
	if err == nil && found && (existing.BackendRevision > metadata.BackendRevision || existing.BackendRevision == metadata.BackendRevision && existing.Committed && !metadata.Committed) {
		return object, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(object.Payload, &payload); err != nil {
		return ClientObject{}, fmt.Errorf("decode bundle control metadata: %w", err)
	}
	meta, _ := payload["metadata"].(map[string]any)
	if meta == nil {
		meta = make(map[string]any)
		payload["metadata"] = meta
	}
	annotations, _ := meta["annotations"].(map[string]any)
	if annotations == nil {
		annotations = make(map[string]any)
		meta["annotations"] = annotations
	}
	annotations[controlRequestIDAnnotation] = metadata.RequestID
	annotations[controlIdempotencyAnnotation] = metadata.IdempotencyKey
	annotations[controlActionAnnotation] = metadata.Action.String()
	annotations[controlRevisionAnnotation] = strconv.FormatUint(metadata.BackendRevision, 10)
	annotations[controlCommittedAnnotation] = strconv.FormatBool(metadata.Committed)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return ClientObject{}, err
	}
	object.Payload = encoded
	return object, nil
}

// DecodeControlMetadata reads metadata from a raw JobRunBundle API object.
func DecodeControlMetadata(payload []byte) (ControlMetadata, bool, error) {
	var object struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(payload, &object); err != nil {
		return ControlMetadata{}, false, err
	}
	values := object.Metadata.Annotations
	if values == nil || strings.TrimSpace(values[controlIdempotencyAnnotation]) == "" {
		return ControlMetadata{}, false, nil
	}
	actionValue, ok := tgsrlv1.JobCommandType_value[values[controlActionAnnotation]]
	if !ok {
		return ControlMetadata{}, false, fmt.Errorf("unknown control action %q", values[controlActionAnnotation])
	}
	revision, err := strconv.ParseUint(values[controlRevisionAnnotation], 10, 64)
	if err != nil {
		return ControlMetadata{}, false, fmt.Errorf("invalid control backend revision: %w", err)
	}
	committed, err := strconv.ParseBool(values[controlCommittedAnnotation])
	if err != nil {
		return ControlMetadata{}, false, fmt.Errorf("invalid control committed flag: %w", err)
	}
	return ControlMetadata{RequestID: values[controlRequestIDAnnotation], IdempotencyKey: values[controlIdempotencyAnnotation], Action: tgsrlv1.JobCommandType(actionValue), BackendRevision: revision, Committed: committed}, true, nil
}

func (b *KubernetesBackend) controlMetadataForBundle(ctx context.Context, key string) (ControlMetadata, bool, error) {
	object, found, err := b.client.Get(ctx, b.adapter.BundleObject(key))
	if err != nil || !found {
		return ControlMetadata{}, false, err
	}
	objectMetadata, objectFound, objectErr := DecodeControlMetadata(object.Payload)
	ledgerMetadata, ledgerFound, ledgerErr := b.controlMetadataFromLedger(key)
	if ledgerErr != nil {
		return ControlMetadata{}, false, ledgerErr
	}
	if ledgerFound {
		// The accepted ledger is the durable commit point. Stale or pending
		// annotations must not hide it after a partial metadata write. A newer
		// committed annotation is retained for compatibility with objects
		// written by a newer ledger revision that is not loaded locally.
		if objectErr != nil || !objectFound || !objectMetadata.Committed || ledgerMetadata.BackendRevision >= objectMetadata.BackendRevision {
			return ledgerMetadata, true, nil
		}
	}
	return objectMetadata, objectFound, objectErr
}

func (b *KubernetesBackend) controlMetadataFromLedger(bundleKey string) (ControlMetadata, bool, error) {
	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	return b.controlMetadataFromLedgerLocked(bundleKey)
}

func (b *KubernetesBackend) controlMetadataFromLedgerLocked(bundleKey string) (ControlMetadata, bool, error) {
	var selected ControlMetadata
	for _, record := range b.controls {
		if record.Pending || record.Result == nil || !record.Result.Accepted {
			continue
		}
		matched := false
		for _, key := range record.BundleKeys {
			if key == bundleKey {
				matched = true
				break
			}
		}
		if !matched || record.Result.BackendRevision <= selected.BackendRevision {
			continue
		}
		selected = metadataForControl(record.Request, record.Result.BackendRevision, true)
	}
	return selected, selected.BackendRevision != 0, nil
}

func (b *KubernetesBackend) repairReplayedControlMetadataLocked(ctx context.Context, record controlRecord) error {
	if record.Result == nil || !record.Result.Accepted || len(record.BundleKeys) == 0 {
		return nil
	}
	expected := metadataForControl(record.Request, record.Result.BackendRevision, true)
	repairKeys := make([]string, 0, len(record.BundleKeys))
	for _, key := range record.BundleKeys {
		latest, found, err := b.controlMetadataFromLedgerLocked(key)
		if err != nil {
			return err
		}
		if !found || latest != expected {
			// A later accepted request owns this bundle now. Replaying an older
			// idempotency key must never move its metadata backwards.
			continue
		}
		object, objectFound, err := b.client.Get(ctx, b.adapter.BundleObject(key))
		if err != nil {
			return err
		}
		if !objectFound {
			continue
		}
		metadata, metadataFound, decodeErr := DecodeControlMetadata(object.Payload)
		if decodeErr == nil && metadataFound && metadata.BackendRevision > expected.BackendRevision {
			continue
		}
		if decodeErr == nil && metadataFound && metadata == expected {
			continue
		}
		repairKeys = append(repairKeys, key)
	}
	if len(repairKeys) == 0 {
		return nil
	}
	return b.setBundleControlMetadata(ctx, repairKeys, expected)
}
