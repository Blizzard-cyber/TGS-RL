package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
)

type ClientObject = bundleadapter.Object

type Client interface {
	Upsert(ctx context.Context, object ClientObject) (created bool, previous *ClientObject, err error)
	Get(ctx context.Context, object ClientObject) (*ClientObject, bool, error)
	ListBundles(ctx context.Context) ([]ClientObject, error)
}

type RuntimeClassClient interface {
	EnsureRuntimeClass(ctx context.Context, object ClientObject) (created bool, previous *ClientObject, err error)
}

type ControlClient interface {
	ControlJob(ctx context.Context, object ClientObject, action tgsrlv1.JobCommandType, expectedVersion string) (*bundleadapter.JobControlReadback, error)
}

type KubernetesBackend struct {
	client           Client
	adapter          bundleadapter.Adapter
	mutationMu       sync.Mutex
	controlMu        sync.Mutex
	revision         uint64
	controls         map[string]controlRecord
	controlStatePath string
}

type controlRecord struct {
	Digest         string                  `json:"digest"`
	Pending        bool                    `json:"pending,omitempty"`
	Request        ControlRequest          `json:"request"`
	BundleKeys     []string                `json:"bundleKeys,omitempty"`
	TargetProgress []controlTargetProgress `json:"targetProgress,omitempty"`
	Result         *ControlResult          `json:"result"`
}

type controlProgressState string

const (
	controlProgressPending   controlProgressState = "pending"
	controlProgressInFlight  controlProgressState = "in_flight"
	controlProgressAmbiguous controlProgressState = "ambiguous"
	controlProgressCompleted controlProgressState = "completed"
)

// controlTargetProgress is one durable execution-substrate mutation. A bundle
// may contain several logical runtime targets, but they share one Kubernetes
// Job and therefore one mutation/progress entry.
type controlTargetProgress struct {
	BundleKey string                           `json:"bundleKey"`
	Mutation  bundleadapter.JobControlMutation `json:"mutation"`
	State     controlProgressState             `json:"state"`
}

func NewKubernetes(client Client) (*KubernetesBackend, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	}
	return &KubernetesBackend{
		client:   client,
		adapter:  bundleadapter.NewKubernetes(0),
		controls: make(map[string]controlRecord),
	}, nil
}

func (b *KubernetesBackend) Apply(ctx context.Context, bundle *api.Bundle) (*ApplyResult, error) {
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	if bundle == nil {
		return nil, fmt.Errorf("bundle is required")
	}
	clone, err := api.CloneBundle(bundle)
	if err != nil {
		return nil, err
	}
	previous, ok, err := b.Get(ctx, clone.Key)
	if err != nil {
		return nil, err
	}
	if ok {
		if clone.Generation < previous.Generation {
			return nil, ErrGenerationConflict
		}
		if clone.Generation == previous.Generation && clone.Fingerprint != previous.Fingerprint {
			return nil, ErrFingerprintDrift
		}
		if clone.Generation == previous.Generation && clone.Fingerprint == previous.Fingerprint {
			if err := b.repairApplyLocked(ctx, clone); err != nil {
				return nil, err
			}
			previous.ControllerStatus.Idempotent = true
			out, err := api.CloneBundle(previous)
			if err != nil {
				return nil, err
			}
			return &ApplyResult{Bundle: out, Idempotent: true}, nil
		}
	}

	objects, err := b.adapter.Materialize(clone)
	if err != nil {
		return nil, err
	}
	desiredObjects, markerObject := splitCompletionMarker(objects)
	for _, object := range desiredObjects {
		if err := b.upsertDesiredObject(ctx, object); err != nil {
			return nil, err
		}
	}
	if markerObject != nil {
		if _, _, err := b.client.Upsert(ctx, *markerObject); err != nil {
			return nil, err
		}
	}
	// Backend observations, including fake mode, must come from backend state.
	// A freshly materialized fake Job starts active after admission; Kubernetes
	// ignores this status field and reports authoritative API status instead.
	if memory, ok := b.client.(*MemoryClient); ok {
		memory.setInitialJobState(clone)
	}

	clone.ControllerStatus.AppliedGeneration = clone.Generation
	clone.ControllerStatus.BundleFingerprint = clone.Fingerprint
	clone.ControllerStatus.Phase = "applied"
	if !ok {
		clone.ControllerStatus.Reason = "created"
	} else {
		clone.ControllerStatus.Reason = "updated"
	}
	out, err := api.CloneBundle(clone)
	if err != nil {
		return nil, err
	}
	return &ApplyResult{
		Bundle:  out,
		Created: !ok,
		Updated: ok,
	}, nil
}

func splitCompletionMarker(objects []ClientObject) ([]ClientObject, *ClientObject) {
	desired := make([]ClientObject, 0, len(objects))
	var marker *ClientObject
	for _, object := range objects {
		object := object
		if object.Kind == "JobRunBundle" {
			marker = &object
			continue
		}
		desired = append(desired, object)
	}
	return desired, marker
}

func (b *KubernetesBackend) upsertDesiredObject(ctx context.Context, object ClientObject) error {
	if object.Kind == "RuntimeClass" {
		if runtimeClasses, ok := b.client.(RuntimeClassClient); ok {
			_, _, err := runtimeClasses.EnsureRuntimeClass(ctx, object)
			return err
		}
	}
	_, _, err := b.client.Upsert(ctx, object)
	return err
}

func (b *KubernetesBackend) repairApplyLocked(ctx context.Context, bundle *api.Bundle) error {
	objects, err := b.adapter.Materialize(bundle)
	if err != nil {
		return err
	}
	desiredObjects, markerObject := splitCompletionMarker(objects)
	for _, object := range desiredObjects {
		// Reconcile every desired object. A marker written by an older
		// marker-first implementation does not prove that later object updates
		// completed; an object may exist with stale generation or spec.
		if err := b.upsertDesiredObject(ctx, object); err != nil {
			return err
		}
	}
	if markerObject != nil {
		if _, found, err := b.client.Get(ctx, *markerObject); err != nil {
			return err
		} else if !found {
			if _, _, err := b.client.Upsert(ctx, *markerObject); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *KubernetesBackend) Get(ctx context.Context, key string) (*api.Bundle, bool, error) {
	object, ok, err := b.client.Get(ctx, b.adapter.BundleObject(key))
	if err != nil || !ok {
		return nil, ok, err
	}
	bundle, err := b.adapter.DecodeBundle(*object)
	if err != nil {
		return nil, false, err
	}
	return bundle, true, nil
}

func (b *KubernetesBackend) List(ctx context.Context) ([]*api.Bundle, error) {
	objects, err := b.client.ListBundles(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	bundles := make([]*api.Bundle, 0, len(objects))
	for _, object := range objects {
		bundle, err := b.adapter.DecodeBundle(object)
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, bundle)
	}
	return bundles, nil
}

// Snapshot returns a point-in-time backend observation. It is used by the
// long-lived ObservationManager; lifecycle RPCs never publish observations.
func (b *KubernetesBackend) Snapshot(ctx context.Context, bundle *api.Bundle) (*ObservationSnapshot, bool, error) {
	reader, ok := b.client.(bundleadapter.Reader)
	if !ok {
		return nil, false, fmt.Errorf("backend client does not support observation readback")
	}
	adapter := bundleadapter.NewKubernetes(0)
	snapshot, terminal, err := snapshotFromAdapter(adapter.ObserveOnce(ctx, reader, bundle))
	if err != nil || snapshot == nil {
		return snapshot, terminal, err
	}
	metadata, found, err := b.controlMetadataForBundle(ctx, bundle.Key)
	if err != nil {
		return nil, terminal, err
	}
	if found {
		applyControlMetadata(snapshot, metadata)
	}
	return snapshot, terminal, nil
}

func (b *KubernetesBackend) Control(ctx context.Context, request ControlRequest) (*ControlResult, error) {
	if err := validateControlRequest(request); err != nil {
		return nil, err
	}
	if !request.Deadline.IsZero() && !time.Now().Before(request.Deadline) {
		return nil, context.DeadlineExceeded
	}
	digest := controlDigest(request)
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	b.controlMu.Lock()
	defer b.controlMu.Unlock()
	if b.controls == nil {
		b.controls = make(map[string]controlRecord)
	}
	record, replay := b.controls[request.IdempotencyKey]
	if replay {
		if record.Digest != digest {
			return nil, ErrIdempotencyConflict
		}
		if !record.Pending && record.Result != nil {
			if err := b.repairReplayedControlMetadataLocked(ctx, record); err != nil {
				return nil, fmt.Errorf("repair committed control metadata: %w", err)
			}
			result := cloneControlResult(record.Result)
			result.Idempotent = true
			return result, nil
		}
	}
	resuming := replay && record.Pending

	// Backend implementations must honor cancellation while waiting for the
	// per-process mutation lock. The fast path above is intentionally serialized
	// with mutation and idempotency recording.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var matches []bundleControlMatch
	bundleKeys := append([]string(nil), record.BundleKeys...)
	if !replay || len(record.TargetProgress) == 0 {
		var err error
		matches, err = b.findBundlesForControl(ctx, request)
		if err != nil {
			return nil, err
		}
		bundleKeys = make([]string, 0, len(matches))
		for _, match := range matches {
			bundleKeys = append(bundleKeys, match.bundle.Key)
		}
	}
	controlClient, ok := b.client.(ControlClient)
	if !ok {
		return nil, fmt.Errorf("backend client does not support lifecycle control")
	}
	controlRevision := b.revision + 1
	if replay && record.Result != nil && record.Result.BackendRevision != 0 {
		controlRevision = record.Result.BackendRevision
	} else {
		progress, prepareErr := b.prepareControlProgress(ctx, request.Action, matches)
		if prepareErr != nil {
			return nil, prepareErr
		}
		b.revision = controlRevision
		record = controlRecord{Digest: digest, Pending: true, Request: request, BundleKeys: append([]string(nil), bundleKeys...), TargetProgress: progress, Result: &ControlResult{BackendRevision: controlRevision}}
		b.controls[request.IdempotencyKey] = record
		if err := b.persistControlStateLocked(); err != nil {
			delete(b.controls, request.IdempotencyKey)
			b.revision--
			return nil, fmt.Errorf("persist pending backend control: %w", err)
		}
	}
	if !resuming {
		if err := b.setBundleControlMetadata(ctx, bundleKeys, metadataForControl(request, controlRevision, false)); err != nil {
			return nil, fmt.Errorf("persist pending control metadata: %w", err)
		}
	}
	if len(record.TargetProgress) == 0 {
		// Compatibility for pending ledgers written before per-target progress.
		progress, prepareErr := b.prepareControlProgress(ctx, request.Action, matches)
		if prepareErr != nil {
			return nil, prepareErr
		}
		if replay {
			// Legacy pending records did not say which mutations reached the
			// substrate, so every still-present target must be reconciled first.
			for index := range progress {
				if progress[index].State == controlProgressPending {
					progress[index].State = controlProgressAmbiguous
				}
			}
		}
		record.TargetProgress = progress
		b.controls[request.IdempotencyKey] = record
		if err := b.persistControlStateLocked(); err != nil {
			return nil, fmt.Errorf("persist backend control progress: %w", err)
		}
	}
	for index := range record.TargetProgress {
		if err := b.executeControlProgress(ctx, controlClient, request.IdempotencyKey, &record, index); err != nil {
			return nil, err
		}
	}
	result := &ControlResult{Accepted: true, BackendRevision: controlRevision, Detail: detailForAction(request.Action)}
	record.Pending = false
	record.Result = cloneControlResult(result)
	b.controls[request.IdempotencyKey] = record
	if err := b.persistControlStateLocked(); err != nil {
		record.Pending = true
		record.Result = &ControlResult{BackendRevision: controlRevision}
		b.controls[request.IdempotencyKey] = record
		return nil, fmt.Errorf("persist backend control state: %w", err)
	}
	if err := b.setBundleControlMetadata(ctx, bundleKeys, metadataForControl(request, controlRevision, true)); err != nil {
		return nil, fmt.Errorf("persist committed control metadata: %w", err)
	}
	return result, nil
}

func (b *KubernetesBackend) prepareControlProgress(ctx context.Context, action tgsrlv1.JobCommandType, matches []bundleControlMatch) ([]controlTargetProgress, error) {
	progress := make([]controlTargetProgress, 0, len(matches))
	for _, match := range matches {
		jobObject := metaObject(match.bundle.Job.TypeMeta.APIVersion, match.bundle.Job.TypeMeta.Kind, match.bundle.Job.ObjectMeta)
		expectedVersion := ""
		if !isMemoryControlClient(b.client) {
			current, found, err := b.client.Get(ctx, jobObject)
			if err != nil {
				return nil, err
			}
			if !found {
				if isDeleteControl(action) {
					progress = append(progress, controlTargetProgress{BundleKey: match.bundle.Key, Mutation: bundleadapter.JobControlMutation{Object: jobObject, Action: action}, State: controlProgressCompleted})
					continue
				}
				return nil, ErrRuntimeNotFound
			}
			expectedVersion = current.Version
		}
		progress = append(progress, controlTargetProgress{BundleKey: match.bundle.Key, Mutation: bundleadapter.JobControlMutation{Object: jobObject, Action: action, ExpectedVersion: expectedVersion}, State: controlProgressPending})
	}
	return progress, nil
}

func isMemoryControlClient(client Client) bool {
	switch value := client.(type) {
	case *MemoryClient:
		return true
	case interface{ memoryControlClient() *MemoryClient }:
		return value.memoryControlClient() != nil
	default:
		return false
	}
}

func (b *KubernetesBackend) executeControlProgress(ctx context.Context, client ControlClient, key string, record *controlRecord, index int) error {
	progress := &record.TargetProgress[index]
	if progress.State == controlProgressCompleted {
		return nil
	}
	if progress.State == controlProgressInFlight || progress.State == controlProgressAmbiguous {
		completed, safeToRetry, err := b.reconcileControlProgress(ctx, *progress)
		if err != nil {
			progress.State = controlProgressAmbiguous
			b.controls[key] = *record
			_ = b.persistControlStateLocked()
			return fmt.Errorf("%w for bundle %q: %v", ErrControlOutcomeAmbiguous, progress.BundleKey, err)
		}
		if completed {
			progress.State = controlProgressCompleted
			b.controls[key] = *record
			if err := b.persistControlStateLocked(); err != nil {
				return fmt.Errorf("%w for bundle %q: persist reconciled progress: %v", ErrControlOutcomeAmbiguous, progress.BundleKey, err)
			}
			return nil
		}
		if !safeToRetry {
			progress.State = controlProgressAmbiguous
			b.controls[key] = *record
			if err := b.persistControlStateLocked(); err != nil {
				return fmt.Errorf("%w for bundle %q: persist ambiguous progress: %v", ErrControlOutcomeAmbiguous, progress.BundleKey, err)
			}
			return fmt.Errorf("%w for bundle %q", ErrControlOutcomeAmbiguous, progress.BundleKey)
		}
	}
	progress.State = controlProgressInFlight
	b.controls[key] = *record
	if err := b.persistControlStateLocked(); err != nil {
		return fmt.Errorf("persist in-flight control progress: %w", err)
	}
	readback, err := client.ControlJob(ctx, progress.Mutation.Object, progress.Mutation.Action, progress.Mutation.ExpectedVersion)
	if err != nil {
		progress.State = controlProgressAmbiguous
		b.controls[key] = *record
		if persistErr := b.persistControlStateLocked(); persistErr != nil {
			return fmt.Errorf("%w for bundle %q: control failed: %v; persist ambiguous outcome: %v", ErrControlOutcomeAmbiguous, progress.BundleKey, err, persistErr)
		}
		return fmt.Errorf("%w for bundle %q: %v", ErrControlOutcomeAmbiguous, progress.BundleKey, err)
	}
	if _, _, _, err := observedStateForReadback(progress.Mutation.Action, readback); err != nil {
		progress.State = controlProgressAmbiguous
		b.controls[key] = *record
		_ = b.persistControlStateLocked()
		return fmt.Errorf("%w for bundle %q: %v", ErrControlOutcomeAmbiguous, progress.BundleKey, err)
	}
	progress.State = controlProgressCompleted
	b.controls[key] = *record
	if err := b.persistControlStateLocked(); err != nil {
		return fmt.Errorf("%w for bundle %q: persist completed progress: %v", ErrControlOutcomeAmbiguous, progress.BundleKey, err)
	}
	return nil
}

func (b *KubernetesBackend) reconcileControlProgress(ctx context.Context, progress controlTargetProgress) (completed, safeToRetry bool, err error) {
	current, found, err := b.client.Get(ctx, progress.Mutation.Object)
	if err != nil {
		return false, false, err
	}
	if !found {
		if isDeleteControl(progress.Mutation.Action) {
			return true, false, nil
		}
		return false, false, ErrRuntimeNotFound
	}
	readback, err := readbackFromObject(*current)
	if err != nil {
		return false, false, err
	}
	if controlReadbackMatches(progress.Mutation.Action, readback) {
		return true, false, nil
	}
	if progress.Mutation.ExpectedVersion == "" || current.Version == progress.Mutation.ExpectedVersion {
		return false, true, nil
	}
	return false, false, nil
}

func readbackFromObject(object ClientObject) (*bundleadapter.JobControlReadback, error) {
	var job struct {
		Metadata struct {
			DeletionTimestamp string `json:"deletionTimestamp"`
		} `json:"metadata"`
		Spec struct {
			Suspend bool `json:"suspend"`
		} `json:"spec"`
		Status struct {
			Active    uint32 `json:"active"`
			Succeeded uint32 `json:"succeeded"`
			Failed    uint32 `json:"failed"`
			Paused    bool   `json:"paused"`
			Deleted   bool   `json:"deleted"`
		} `json:"status"`
	}
	if err := json.Unmarshal(object.Payload, &job); err != nil {
		return nil, fmt.Errorf("decode controlled job readback: %w", err)
	}
	return &bundleadapter.JobControlReadback{Paused: job.Spec.Suspend || job.Status.Paused, Deleted: job.Status.Deleted, Deleting: job.Metadata.DeletionTimestamp != "", Active: job.Status.Active, Succeeded: job.Status.Succeeded, Failed: job.Status.Failed, ObservedAt: time.Now().UTC()}, nil
}

func controlReadbackMatches(action tgsrlv1.JobCommandType, readback *bundleadapter.JobControlReadback) bool {
	_, _, accepted, err := observedStateForReadback(action, readback)
	return err == nil && (accepted || isDeleteControl(action) && readback != nil && readback.Deleting)
}

func isDeleteControl(action tgsrlv1.JobCommandType) bool {
	return action == tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP || action == tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE
}

type bundleControlMatch struct {
	bundle   *api.Bundle
	targets  []ControlTarget
	bindings map[string]api.RuntimeTarget
}

func (b *KubernetesBackend) findBundlesForControl(ctx context.Context, request ControlRequest) ([]bundleControlMatch, error) {
	objects, err := b.client.ListBundles(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	bundles := make([]*api.Bundle, 0, len(objects))
	for _, object := range objects {
		bundle, err := b.adapter.DecodeBundle(object)
		if err != nil {
			return nil, err
		}
		if bundle.SourceRunID == request.RunID && bundle.SourceJobID == request.JobID {
			bundles = append(bundles, bundle)
		}
	}
	if len(bundles) == 0 {
		return nil, ErrRuntimeNotFound
	}
	type locatedTarget struct {
		bundleIndex int
		metadata    api.RuntimeTarget
	}
	located := make(map[string]locatedTarget)
	for bundleIndex, bundle := range bundles {
		for _, target := range bundle.RuntimeTargets {
			key := target.RuntimeUnitID + "\x00" + target.SandboxID
			if _, duplicate := located[key]; duplicate {
				return nil, fmt.Errorf("%w: runtime target %q/%q appears in multiple bundles", ErrInvalidControl, target.RuntimeUnitID, target.SandboxID)
			}
			located[key] = locatedTarget{bundleIndex: bundleIndex, metadata: target}
		}
	}
	matchesByBundle := make(map[int]*bundleControlMatch)
	for _, requested := range request.Targets {
		key := requested.RuntimeUnitID + "\x00" + requested.SandboxID
		entry, ok := located[key]
		if !ok {
			return nil, ErrRuntimeNotFound
		}
		if entry.metadata.Generation != requested.ExpectedGeneration {
			return nil, ErrGenerationConflict
		}
		match := matchesByBundle[entry.bundleIndex]
		if match == nil {
			match = &bundleControlMatch{bundle: bundles[entry.bundleIndex], bindings: make(map[string]api.RuntimeTarget)}
			matchesByBundle[entry.bundleIndex] = match
		}
		match.targets = append(match.targets, requested)
		match.bindings[key] = entry.metadata
	}
	indices := make([]int, 0, len(matchesByBundle))
	for index := range matchesByBundle {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	matches := make([]bundleControlMatch, 0, len(indices))
	for _, index := range indices {
		matches = append(matches, *matchesByBundle[index])
	}
	return matches, nil
}

func validateControlRequest(request ControlRequest) error {
	if request.RunID == "" || request.JobID == "" {
		return fmt.Errorf("%w: job_id and run_id are required", ErrInvalidControl)
	}
	if request.IdempotencyKey == "" {
		return fmt.Errorf("%w: idempotency_key is required", ErrInvalidControl)
	}
	switch request.Action {
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME,
		tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE:
	default:
		return fmt.Errorf("%w: unsupported backend lifecycle action %s", ErrInvalidControl, request.Action)
	}
	if len(request.Targets) == 0 {
		return fmt.Errorf("%w: at least one runtime target is required", ErrInvalidControl)
	}
	seen := make(map[string]struct{}, len(request.Targets))
	for _, target := range request.Targets {
		if target.RuntimeUnitID == "" || target.SandboxID == "" || target.ExpectedGeneration == 0 {
			return fmt.Errorf("%w: runtime_unit_id, sandbox_id, and expected_generation are required for every target", ErrInvalidControl)
		}
		key := target.RuntimeUnitID + "\x00" + target.SandboxID
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%w: duplicate runtime target %q/%q", ErrInvalidControl, target.RuntimeUnitID, target.SandboxID)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func controlDigest(request ControlRequest) string {
	parts := []string{request.Action.String(), request.JobID, request.RunID, request.TraceID, request.Reason}
	for _, target := range request.Targets {
		parts = append(parts, fmt.Sprintf("%s/%s/%d", target.RuntimeUnitID, target.SandboxID, target.ExpectedGeneration))
	}
	sort.Strings(parts[5:])
	return strings.Join(parts, "\x00")
}

func observedStateForReadback(action tgsrlv1.JobCommandType, readback *bundleadapter.JobControlReadback) (tgsrlv1.RuntimeState, tgsrlv1.SandboxEventType, bool, error) {
	if readback == nil {
		return 0, 0, false, fmt.Errorf("backend control returned no readback")
	}
	switch action {
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE:
		if !readback.Paused {
			return 0, 0, false, fmt.Errorf("backend did not observe paused job")
		}
		return tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_PAUSED, true, nil
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:
		if readback.Paused || readback.Deleted {
			return 0, 0, false, fmt.Errorf("backend did not observe resumed job")
		}
		return tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_RUNNING, true, nil
	default:
		if readback.Deleting && !readback.Deleted {
			return 0, 0, false, nil
		}
		if !readback.Deleted && !readback.Deleting && readback.Succeeded == 0 {
			return 0, 0, false, fmt.Errorf("backend did not accept job deletion")
		}
		return tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED, tgsrlv1.SandboxEventType_SANDBOX_EVENT_TYPE_TERMINATED, true, nil
	}
}

func detailForAction(action tgsrlv1.JobCommandType) string {
	return "backend observed " + strings.ToLower(strings.TrimPrefix(action.String(), "JOB_COMMAND_TYPE_"))
}

func cloneControlResult(result *ControlResult) *ControlResult {
	if result == nil {
		return nil
	}
	clone := *result
	return &clone
}

func metaObject(apiVersion, kind string, metadata api.ObjectMeta) ClientObject {
	return ClientObject{APIVersion: apiVersion, Kind: kind, Key: metadata.Namespace + "/" + metadata.Name, Name: metadata.Name, Namespace: metadata.Namespace, Generation: uint64(metadata.Generation)}
}

func (b *KubernetesBackend) fakeSnapshots(ctx context.Context, bundle *api.Bundle) ([]*ObservationSnapshot, error) {
	object, ok, err := b.client.Get(ctx, metaObject(bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind, bundle.Job.ObjectMeta))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrRuntimeNotFound
	}
	var job api.Job
	if err := json.Unmarshal(object.Payload, &job); err != nil {
		return nil, err
	}
	claimAllocated := bundle.ResourceClaim == nil || bundle.ResourceClaim.Spec.Count > 0
	metadata, hasMetadata, err := b.controlMetadataForBundle(ctx, bundle.Key)
	if err != nil {
		return nil, err
	}
	observed := &ObservationSnapshot{ObservedGeneration: bundle.Generation, WorkloadAdmitted: true, ResourceClaimsAllocated: claimAllocated, JobActive: job.Status.Active, JobSucceeded: job.Status.Succeeded, JobFailed: job.Status.Failed, JobPaused: job.Status.Paused, JobDeleted: job.Status.Deleted, Reason: "fake backend observed job state", ObservedAt: time.Now().UTC()}
	if hasMetadata {
		applyControlMetadata(observed, metadata)
	}
	return []*ObservationSnapshot{{ObservedGeneration: bundle.Generation, WorkloadAdmitted: true, ResourceClaimsAllocated: claimAllocated, Reason: "fake backend observed workload admission", ObservedAt: time.Now().UTC()}, observed}, nil
}

func applyControlMetadata(snapshot *ObservationSnapshot, metadata ControlMetadata) {
	if snapshot == nil {
		return
	}
	snapshot.ControlRequestID = metadata.RequestID
	snapshot.ControlIdempotencyKey = metadata.IdempotencyKey
	snapshot.ControlAction = metadata.Action
	snapshot.ControlBackendRevision = metadata.BackendRevision
	snapshot.ControlCommitted = metadata.Committed
}

func snapshotFromAdapter(snapshot *bundleadapter.Snapshot, terminal bool, err error) (*ObservationSnapshot, bool, error) {
	if err != nil || snapshot == nil {
		return nil, terminal, err
	}
	return &ObservationSnapshot{ObservedGeneration: snapshot.ObservedGeneration, WorkloadAdmitted: snapshot.WorkloadAdmitted, ResourceClaimsAllocated: snapshot.ResourceClaimsAllocated, JobActive: snapshot.JobActive, JobSucceeded: snapshot.JobSucceeded, JobFailed: snapshot.JobFailed, JobPaused: snapshot.JobPaused, JobDeleted: snapshot.JobDeleted, Reason: snapshot.Reason, ObservedAt: snapshot.ObservedAt, ControlRequestID: snapshot.ControlRequestID, ControlIdempotencyKey: snapshot.ControlIdempotencyKey, ControlAction: snapshot.ControlAction, ControlBackendRevision: snapshot.ControlBackendRevision, ControlCommitted: snapshot.ControlCommitted}, terminal, nil
}
