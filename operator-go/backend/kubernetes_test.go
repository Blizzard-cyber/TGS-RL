package backend

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
)

type committedMetadataFailureClient struct {
	*MemoryClient
	failBundleKey string
	failWrites    int
	controlCalls  int
}

func (c *committedMetadataFailureClient) memoryControlClient() *MemoryClient {
	return c.MemoryClient
}

type partialControlClient struct {
	*MemoryClient
	failObjectKey string
	failCount     int
	calls         map[string]int
}

func (c *partialControlClient) memoryControlClient() *MemoryClient {
	return c.MemoryClient
}

func (c *partialControlClient) ControlJob(ctx context.Context, object ClientObject, action tgsrlv1.JobCommandType, expectedVersion string) (*bundleadapter.JobControlReadback, error) {
	if c.calls == nil {
		c.calls = make(map[string]int)
	}
	c.calls[object.Key]++
	if object.Key == c.failObjectKey && c.failCount > 0 {
		c.failCount--
		return nil, errors.New("injected second control failure")
	}
	return c.MemoryClient.ControlJob(ctx, object, action, expectedVersion)
}

type deleteControlClient struct {
	*MemoryClient
	failObjectKey string
	failCount     int
	calls         map[string]int
}

type partialApplyClient struct {
	*MemoryClient
	failObjectKey string
	failCount     int
	calls         map[string]int
}

func (c *deleteControlClient) memoryControlClient() *MemoryClient {
	return c.MemoryClient
}

func (c *partialApplyClient) memoryControlClient() *MemoryClient {
	return c.MemoryClient
}

func (c *partialApplyClient) Upsert(ctx context.Context, object ClientObject) (bool, *ClientObject, error) {
	if c.calls == nil {
		c.calls = make(map[string]int)
	}
	c.calls[object.Key]++
	if object.Key == c.failObjectKey && c.failCount > 0 {
		c.failCount--
		return false, nil, errors.New("injected apply failure")
	}
	return c.MemoryClient.Upsert(ctx, object)
}

func (c *partialApplyClient) EnsureRuntimeClass(ctx context.Context, object ClientObject) (bool, *ClientObject, error) {
	if c.calls == nil {
		c.calls = make(map[string]int)
	}
	c.calls[object.Key]++
	if object.Key == c.failObjectKey && c.failCount > 0 {
		c.failCount--
		return false, nil, errors.New("injected apply failure")
	}
	return c.MemoryClient.EnsureRuntimeClass(ctx, object)
}

func (c *deleteControlClient) ControlJob(ctx context.Context, object ClientObject, action tgsrlv1.JobCommandType, expectedVersion string) (*bundleadapter.JobControlReadback, error) {
	if c.calls == nil {
		c.calls = make(map[string]int)
	}
	c.calls[object.Key]++
	if object.Key == c.failObjectKey && c.failCount > 0 {
		c.failCount--
		return nil, errors.New("injected second delete failure")
	}
	readback, err := c.MemoryClient.ControlJob(ctx, object, action, expectedVersion)
	if err == nil && isDeleteControl(action) {
		c.MemoryClient.mu.Lock()
		delete(c.MemoryClient.objects, object.Key)
		c.MemoryClient.mu.Unlock()
	}
	return readback, err
}

func twoBundleControlFixture(t *testing.T, backend *KubernetesBackend) (*api.Bundle, *api.Bundle, ControlRequest) {
	t.Helper()
	first := testBundle()
	first.Key, first.Fingerprint, first.SourceRunID, first.SourceJobID = "ns/bundle-a", "fp-a", "run-1", "job-1"
	first.Job.ObjectMeta.Name = "job-a"
	first.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", Generation: 1}}
	second, err := api.CloneBundle(first)
	if err != nil {
		t.Fatal(err)
	}
	second.Key, second.Fingerprint, second.Job.ObjectMeta.Name = "ns/bundle-b", "fp-b", "job-b"
	second.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-b", SandboxID: "sandbox-b", Generation: 1}}
	for _, bundle := range []*api.Bundle{first, second} {
		if _, err := backend.Apply(context.Background(), bundle); err != nil {
			t.Fatal(err)
		}
	}
	request := ControlRequest{
		Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, JobID: "job-1", RunID: "run-1",
		RequestID: "request-both", IdempotencyKey: "control-both",
		Targets: []ControlTarget{{RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", ExpectedGeneration: 1}, {RuntimeUnitID: "unit-b", SandboxID: "sandbox-b", ExpectedGeneration: 1}},
	}
	return first, second, request
}

func (c *committedMetadataFailureClient) Upsert(ctx context.Context, object ClientObject) (bool, *ClientObject, error) {
	metadata, found, err := DecodeControlMetadata(object.Payload)
	if object.Key == c.failBundleKey && err == nil && found && metadata.Committed && c.failWrites > 0 {
		c.failWrites--
		return false, nil, errors.New("injected committed metadata failure")
	}
	return c.MemoryClient.Upsert(ctx, object)
}

func (c *committedMetadataFailureClient) ControlJob(ctx context.Context, object ClientObject, action tgsrlv1.JobCommandType, expectedVersion string) (*bundleadapter.JobControlReadback, error) {
	c.controlCalls++
	return c.MemoryClient.ControlJob(ctx, object, action, expectedVersion)
}

func TestKubernetesBackendApplyAndReplay(t *testing.T) {
	client := NewMemoryClient()
	b, err := NewKubernetes(client)
	if err != nil {
		t.Fatalf("new backend failed: %v", err)
	}

	first, err := b.Apply(context.Background(), testBundle())
	if err != nil {
		t.Fatalf("first apply failed: %v", err)
	}
	if !first.Created {
		t.Fatalf("expected created result")
	}

	second, err := b.Apply(context.Background(), testBundle())
	if err != nil {
		t.Fatalf("second apply failed: %v", err)
	}
	if !second.Idempotent {
		t.Fatalf("expected idempotent replay")
	}
}

func TestKubernetesBackendRepairsMissingObjectsBeforeIdempotentReplay(t *testing.T) {
	for _, failedKey := range []string{"/runtime-a", "ns/claim-a", "ns/workload-a", "ns/job-a"} {
		t.Run(failedKey, func(t *testing.T) {
			client := &partialApplyClient{MemoryClient: NewMemoryClient(), failObjectKey: failedKey, failCount: 1}
			b, err := NewKubernetes(client)
			if err != nil {
				t.Fatal(err)
			}
			bundle := testBundle()
			if _, err := b.Apply(context.Background(), bundle); err == nil {
				t.Fatalf("first apply should fail for %s", failedKey)
			}
			if _, found, err := client.Get(context.Background(), b.adapter.BundleObject(bundle.Key)); err != nil {
				t.Fatal(err)
			} else if found {
				t.Fatalf("completion marker was written before all objects succeeded for %s", failedKey)
			}
			result, err := b.Apply(context.Background(), bundle)
			if err != nil {
				t.Fatalf("repair apply failed for %s: %v", failedKey, err)
			}
			if result.Idempotent || !result.Created {
				t.Fatalf("repair result for %s = %+v, want created repair", failedKey, result)
			}
			replayed, err := b.Apply(context.Background(), bundle)
			if err != nil {
				t.Fatalf("idempotent replay failed for %s: %v", failedKey, err)
			}
			if !replayed.Idempotent {
				t.Fatalf("replay result for %s = %+v, want idempotent", failedKey, replayed)
			}
			for _, object := range []ClientObject{
				metaObject(bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind, bundle.Job.ObjectMeta),
				metaObject(bundle.Workload.TypeMeta.APIVersion, bundle.Workload.TypeMeta.Kind, bundle.Workload.ObjectMeta),
				metaObject(bundle.ResourceClaim.TypeMeta.APIVersion, bundle.ResourceClaim.TypeMeta.Kind, bundle.ResourceClaim.ObjectMeta),
				metaObject(bundle.RuntimeClass.TypeMeta.APIVersion, bundle.RuntimeClass.TypeMeta.Kind, bundle.RuntimeClass.ObjectMeta),
				b.adapter.BundleObject(bundle.Key),
			} {
				if _, found, err := client.Get(context.Background(), object); err != nil {
					t.Fatal(err)
				} else if !found {
					t.Fatalf("object %s missing after repair for %s", object.Key, failedKey)
				}
			}
		})
	}
}

func TestKubernetesBackendReplacementFailurePreservesPreviousGeneration(t *testing.T) {
	client := &partialApplyClient{MemoryClient: NewMemoryClient()}
	backend, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	previous := testBundle()
	if _, err := backend.Apply(context.Background(), previous); err != nil {
		t.Fatal(err)
	}

	replacement, err := api.CloneBundle(previous)
	if err != nil {
		t.Fatal(err)
	}
	replacement.Generation = 2
	replacement.Fingerprint = "fp-2"
	replacement.Workload.ObjectMeta.Name = "workload-a-g2"
	replacement.Workload.ObjectMeta.Generation = 2
	replacement.Job.ObjectMeta.Name = "job-a-g2"
	replacement.Job.ObjectMeta.Generation = 2
	replacement.ResourceClaim.ObjectMeta.Name = "claim-a-g2"
	replacement.ResourceClaim.ObjectMeta.Generation = 2
	client.failObjectKey = "ns/workload-a-g2"
	client.failCount = 1

	if _, err := backend.Apply(context.Background(), replacement); err == nil {
		t.Fatal("replacement apply should fail")
	}
	stored, found, err := backend.Get(context.Background(), previous.Key)
	if err != nil || !found {
		t.Fatalf("get previous bundle after failed replacement: found=%v err=%v", found, err)
	}
	if stored.Generation != previous.Generation || stored.Fingerprint != previous.Fingerprint {
		t.Fatalf("stored bundle = generation %d fingerprint %q, want previous generation %d fingerprint %q", stored.Generation, stored.Fingerprint, previous.Generation, previous.Fingerprint)
	}
	previousObjects, _ := splitCompletionMarker(mustMaterialize(t, backend, previous))
	for _, object := range previousObjects {
		if object.Kind == "RuntimeClass" {
			continue
		}
		if _, found, err := client.Get(context.Background(), object); err != nil {
			t.Fatal(err)
		} else if !found {
			t.Fatalf("previous-generation object %s was removed before replacement was ready", object.Key)
		}
	}
}

func TestKubernetesBackendRepairsStaleMarkerReplayByRecreatingMissingObject(t *testing.T) {
	client := NewMemoryClient()
	b, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	delete(client.objects, metaObject(bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind, bundle.Job.ObjectMeta).Key)
	client.mu.Unlock()

	replayed, err := b.Apply(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Idempotent {
		t.Fatalf("replayed = %+v, want idempotent after repair", replayed)
	}
	if _, found, err := client.Get(context.Background(), metaObject(bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind, bundle.Job.ObjectMeta)); err != nil {
		t.Fatal(err)
	} else if !found {
		t.Fatal("missing job was not recreated during idempotent repair")
	}
}

func TestKubernetesBackendRepairsExistingStaleObjectBeforeIdempotentReplay(t *testing.T) {
	client := NewMemoryClient()
	b, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	desiredObjects, _ := splitCompletionMarker(mustMaterialize(t, b, bundle))
	var desiredJob ClientObject
	for _, object := range desiredObjects {
		if object.Kind == "Job" {
			desiredJob = object
			break
		}
	}
	if desiredJob.Key == "" {
		t.Fatal("materialized bundle has no Job")
	}
	stale := desiredJob
	stale.Payload = []byte(`{"apiVersion":"batch/v1","kind":"Job","metadata":{"name":"job-a","namespace":"ns"},"spec":{"parallelism":99}}`)
	client.mu.Lock()
	client.objects[stale.Key] = stale
	client.mu.Unlock()

	replayed, err := b.Apply(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Idempotent {
		t.Fatalf("replayed = %+v, want idempotent after repair", replayed)
	}
	repaired, found, err := client.Get(context.Background(), desiredJob)
	if err != nil || !found {
		t.Fatalf("Get(repaired Job) found=%v err=%v", found, err)
	}
	var repairedJob api.Job
	if err := json.Unmarshal(repaired.Payload, &repairedJob); err != nil {
		t.Fatalf("decode repaired Job: %v", err)
	}
	if repairedJob.Spec.Parallelism != bundle.Job.Spec.Parallelism || repairedJob.ObjectMeta.Name != bundle.Job.ObjectMeta.Name {
		t.Fatalf("repaired Job remained stale: %+v", repairedJob.Spec)
	}
}

func TestKubernetesBackendGetReturnsStoredBundleMarkerMetadata(t *testing.T) {
	client := NewMemoryClient()
	b, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	bundle.GPUProfile = "nvidia-device-plugin"
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}

	object, found, err := client.Get(context.Background(), b.adapter.BundleObject(bundle.Key))
	if err != nil || !found {
		t.Fatalf("get raw bundle marker: found=%v err=%v", found, err)
	}
	marker, metadata, err := updateStoredBundleMetadata(*object, func(meta *api.ObjectMeta) {
		if meta.Annotations == nil {
			meta.Annotations = make(map[string]string)
		}
		meta.Annotations["custom"] = "value"
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Upsert(context.Background(), marker); err != nil {
		t.Fatal(err)
	}

	got, ok, err := b.Get(context.Background(), bundle.Key)
	if err != nil || !ok {
		t.Fatalf("Get() ok=%v err=%v", ok, err)
	}
	if got.GPUProfile != bundle.GPUProfile {
		t.Fatalf("gpu profile = %q, want %q", got.GPUProfile, bundle.GPUProfile)
	}
	raw, found, err := client.Get(context.Background(), b.adapter.BundleObject(bundle.Key))
	if err != nil || !found {
		t.Fatalf("re-read raw bundle marker: found=%v err=%v", found, err)
	}
	_, roundTrippedMeta, err := decodeStoredBundleObject(*raw)
	if err != nil {
		t.Fatal(err)
	}
	if roundTrippedMeta.Annotations["custom"] != metadata.Annotations["custom"] || roundTrippedMeta.Annotations[bundleSelectedGPUProfile] != bundle.GPUProfile {
		t.Fatalf("bundle marker metadata lost: %+v", roundTrippedMeta.Annotations)
	}
}

func TestKubernetesBackendGetReconcilesDeletingBundleAndRemovesFinalizer(t *testing.T) {
	client := NewMemoryClient()
	b, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}

	object, found, err := client.Get(context.Background(), b.adapter.BundleObject(bundle.Key))
	if err != nil || !found {
		t.Fatalf("get raw bundle marker: found=%v err=%v", found, err)
	}
	marker, _, err := updateStoredBundleMetadata(*object, func(meta *api.ObjectMeta) {
		meta.DeletionTimestamp = "2026-08-29T10:00:00Z"
		if !containsString(meta.Finalizers, bundleFinalizer) {
			meta.Finalizers = append(meta.Finalizers, bundleFinalizer)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Upsert(context.Background(), marker); err != nil {
		t.Fatal(err)
	}

	got, ok, err := b.Get(context.Background(), bundle.Key)
	if err != nil {
		t.Fatal(err)
	}
	if ok || got != nil {
		t.Fatalf("Get() = %+v, ok=%v; want bundle removed after delete reconciliation", got, ok)
	}
	for _, object := range []ClientObject{
		metaObject(bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind, bundle.Job.ObjectMeta),
		metaObject(bundle.Workload.TypeMeta.APIVersion, bundle.Workload.TypeMeta.Kind, bundle.Workload.ObjectMeta),
		metaObject(bundle.ResourceClaim.TypeMeta.APIVersion, bundle.ResourceClaim.TypeMeta.Kind, bundle.ResourceClaim.ObjectMeta),
		metaObject(bundle.RuntimeClass.TypeMeta.APIVersion, bundle.RuntimeClass.TypeMeta.Kind, bundle.RuntimeClass.ObjectMeta),
		b.adapter.BundleObject(bundle.Key),
	} {
		if _, found, err := client.Get(context.Background(), object); err != nil {
			t.Fatal(err)
		} else if found {
			t.Fatalf("object %s still exists after deleting-bundle reconciliation", object.Key)
		}
	}
}

func TestKubernetesBackendListOmitsDeletingBundleAfterCleanup(t *testing.T) {
	client := NewMemoryClient()
	b, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	keep := testBundle()
	keep.Key = "ns/bundle-keep"
	keep.Fingerprint = "fp-keep"
	keep.Job.ObjectMeta.Name = "job-keep"
	keep.Workload.ObjectMeta.Name = "workload-keep"
	keep.ResourceClaim.ObjectMeta.Name = "claim-keep"
	keep.RuntimeClass.ObjectMeta.Name = "runtime-keep"
	if _, err := b.Apply(context.Background(), keep); err != nil {
		t.Fatal(err)
	}
	deleting, err := api.CloneBundle(keep)
	if err != nil {
		t.Fatal(err)
	}
	deleting.Key = "ns/bundle-delete"
	deleting.Fingerprint = "fp-delete"
	deleting.Job.ObjectMeta.Name = "job-delete"
	deleting.Workload.ObjectMeta.Name = "workload-delete"
	deleting.ResourceClaim.ObjectMeta.Name = "claim-delete"
	deleting.RuntimeClass.ObjectMeta.Name = "runtime-delete"
	if _, err := b.Apply(context.Background(), deleting); err != nil {
		t.Fatal(err)
	}

	object, found, err := client.Get(context.Background(), b.adapter.BundleObject(deleting.Key))
	if err != nil || !found {
		t.Fatalf("get deleting marker: found=%v err=%v", found, err)
	}
	marker, _, err := updateStoredBundleMetadata(*object, func(meta *api.ObjectMeta) {
		meta.DeletionTimestamp = "2026-08-29T10:00:00Z"
		if !containsString(meta.Finalizers, bundleFinalizer) {
			meta.Finalizers = append(meta.Finalizers, bundleFinalizer)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Upsert(context.Background(), marker); err != nil {
		t.Fatal(err)
	}

	bundles, err := b.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(bundles) != 1 || bundles[0].Key != keep.Key {
		t.Fatalf("List() = %+v, want only surviving bundle %q", bundles, keep.Key)
	}
}

func mustMaterialize(t *testing.T, backend *KubernetesBackend, bundle *api.Bundle) []ClientObject {
	t.Helper()
	objects, err := backend.adapter.Materialize(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return objects
}

func TestKubernetesBackendRejectsConflicts(t *testing.T) {
	client := NewMemoryClient()
	b, err := NewKubernetes(client)
	if err != nil {
		t.Fatalf("new backend failed: %v", err)
	}
	initial := testBundle()
	initial.Generation = 2
	initial.Fingerprint = "fp-1"
	if _, err := b.Apply(context.Background(), initial); err != nil {
		t.Fatalf("initial apply failed: %v", err)
	}

	regression := testBundle()
	regression.Generation = 1
	regression.Fingerprint = "fp-1"
	if _, err := b.Apply(context.Background(), regression); err != ErrGenerationConflict {
		t.Fatalf("expected ErrGenerationConflict, got %v", err)
	}

	drift := testBundle()
	drift.Generation = 2
	drift.Fingerprint = "fp-2"
	if _, err := b.Apply(context.Background(), drift); err != ErrFingerprintDrift {
		t.Fatalf("expected ErrFingerprintDrift, got %v", err)
	}
}

func TestFakeBackendControlUsesSharedStateAndGenerationFence(t *testing.T) {
	b := NewFake()
	bundle := testBundle()
	bundle.SourceRunID = "run-1"
	bundle.SourceJobID = "job-1"
	bundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", BindingID: "binding-1", Generation: 1}}
	if _, err := b.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	request := ControlRequest{Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, JobID: "job-1", RunID: "run-1", IdempotencyKey: "pause-1", Targets: []ControlTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", ExpectedGeneration: 1}}}
	result, err := b.Control(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted || result.Idempotent || result.BackendRevision != 1 {
		t.Fatalf("control result = %+v", result)
	}
	snapshots, err := b.Snapshots(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 || !snapshots[1].JobPaused || snapshots[1].JobActive != 0 {
		t.Fatalf("shared snapshots = %+v", snapshots)
	}
	replayed, err := b.Control(context.Background(), request)
	if err != nil || !replayed.Idempotent || replayed.BackendRevision != 1 {
		t.Fatalf("replay = %+v, err = %v", replayed, err)
	}
	request.IdempotencyKey = "resume-stale"
	request.Action = tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME
	request.Targets[0].ExpectedGeneration = 2
	stale, err := b.Control(context.Background(), request)
	if err != ErrGenerationConflict || stale != nil {
		t.Fatalf("stale result = %+v, err = %v", stale, err)
	}
}

func TestFakeBackendControlAggregatesTargetsAcrossBundles(t *testing.T) {
	b := NewFake()
	first := testBundle()
	first.Key = "ns/bundle-stage-1"
	first.Fingerprint = "fp-stage-1"
	first.SourceRunID, first.SourceJobID = "run-1", "job-1"
	first.Job.ObjectMeta.Name = "job-stage-1"
	first.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", BindingID: "binding-1", Generation: 2}}
	second, err := api.CloneBundle(first)
	if err != nil {
		t.Fatal(err)
	}
	second.Key = "ns/bundle-stage-2"
	second.Fingerprint = "fp-stage-2"
	second.Job.ObjectMeta.Name = "job-stage-2"
	second.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-2", SandboxID: "sandbox-2", BindingID: "binding-2", Generation: 2}}
	if _, err := b.Apply(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Apply(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	result, err := b.Control(context.Background(), ControlRequest{Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, JobID: "job-1", RunID: "run-1", RequestID: "request-both", IdempotencyKey: "pause-both", Targets: []ControlTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", ExpectedGeneration: 2}, {RuntimeUnitID: "unit-2", SandboxID: "sandbox-2", ExpectedGeneration: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Accepted {
		t.Fatalf("result = %+v", result)
	}
	for _, bundle := range []*api.Bundle{first, second} {
		snapshots, err := b.Snapshots(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		if len(snapshots) != 2 || !snapshots[1].JobPaused {
			t.Fatalf("bundle %s snapshots = %+v", bundle.Key, snapshots)
		}
		if !snapshots[1].ControlCommitted || snapshots[1].ControlIdempotencyKey != "pause-both" || snapshots[1].ControlRequestID != "request-both" || snapshots[1].ControlBackendRevision != result.BackendRevision {
			t.Fatalf("bundle %s causality = %+v", bundle.Key, snapshots[1])
		}
	}
}

func TestFakeBackendControlValidatesAllBundlesBeforeMutation(t *testing.T) {
	b := NewFake()
	first := testBundle()
	first.Key, first.Fingerprint, first.SourceRunID, first.SourceJobID = "ns/bundle-a", "fp-a", "run-1", "job-1"
	first.Job.ObjectMeta.Name = "job-a"
	first.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", Generation: 2}}
	second, _ := api.CloneBundle(first)
	second.Key, second.Fingerprint, second.Job.ObjectMeta.Name = "ns/bundle-b", "fp-b", "job-b"
	second.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-b", SandboxID: "sandbox-b", Generation: 2}}
	if _, err := b.Apply(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Apply(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	_, err := b.Control(context.Background(), ControlRequest{Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, JobID: "job-1", RunID: "run-1", IdempotencyKey: "invalid-all", Targets: []ControlTarget{{RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", ExpectedGeneration: 2}, {RuntimeUnitID: "unit-b", SandboxID: "sandbox-b", ExpectedGeneration: 1}}})
	if err != ErrGenerationConflict {
		t.Fatalf("error = %v, want generation conflict", err)
	}
	snapshots, err := b.Snapshots(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if snapshots[1].JobPaused {
		t.Fatal("first bundle mutated before all targets passed validation")
	}
}

func TestKubernetesBackendPersistsPartialProgressAndRetriesOnlyFailedBundle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	client := &partialControlClient{MemoryClient: NewMemoryClient(), failObjectKey: "ns/job-b", failCount: 1}
	backend, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	_, _, request := twoBundleControlFixture(t, backend)
	result, err := backend.Control(context.Background(), request)
	if result != nil || !errors.Is(err, ErrControlOutcomeAmbiguous) {
		t.Fatalf("first control = %+v, err = %v; want ambiguous partial result", result, err)
	}
	record := backend.controls[request.IdempotencyKey]
	if len(record.TargetProgress) != 2 || record.TargetProgress[0].State != controlProgressCompleted || record.TargetProgress[1].State != controlProgressAmbiguous {
		t.Fatalf("partial progress = %+v", record.TargetProgress)
	}
	result, err = backend.Control(context.Background(), request)
	if err != nil || result == nil || !result.Accepted {
		t.Fatalf("retry = %+v, err = %v", result, err)
	}
	if client.calls["ns/job-a"] != 1 || client.calls["ns/job-b"] != 2 {
		t.Fatalf("control calls = %+v; completed first job must not replay", client.calls)
	}
}

func TestKubernetesBackendResumesPartialProgressAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	client := &partialControlClient{MemoryClient: NewMemoryClient(), failObjectKey: "ns/job-b", failCount: 1}
	first, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	_, _, request := twoBundleControlFixture(t, first)
	if _, err := first.Control(context.Background(), request); !errors.Is(err, ErrControlOutcomeAmbiguous) {
		t.Fatalf("first control error = %v", err)
	}
	restarted, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Control(context.Background(), request)
	if err != nil || result == nil || !result.Accepted {
		t.Fatalf("restart retry = %+v, err = %v", result, err)
	}
	if client.calls["ns/job-a"] != 1 || client.calls["ns/job-b"] != 2 {
		t.Fatalf("control calls after restart = %+v", client.calls)
	}
}

func TestKubernetesBackendDoesNotRepeatCompletedIrreversibleDeleteAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	client := &deleteControlClient{MemoryClient: NewMemoryClient()}
	first, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	firstBundle, _, request := twoBundleControlFixture(t, first)
	request.Action = tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE
	request.IdempotencyKey = "terminate-both"
	wantMetadata := metadataForControl(request, 1, true)

	// Model a crash after the first delete reached Kubernetes but before the
	// completed marker was durable: the ledger retains in_flight while the Job
	// is already absent. Delete cannot be rolled back or safely repeated against
	// a replacement object.
	record := controlRecord{Digest: controlDigest(request), Pending: true, Request: request, BundleKeys: []string{"ns/bundle-a", "ns/bundle-b"}, Result: &ControlResult{BackendRevision: 1}, TargetProgress: []controlTargetProgress{
		{BundleKey: "ns/bundle-a", Mutation: bundleadapter.JobControlMutation{Object: ClientObject{APIVersion: "batch/v1", Kind: "Job", Key: "ns/job-a", Name: "job-a", Namespace: "ns"}, Action: request.Action}, State: controlProgressInFlight},
		{BundleKey: "ns/bundle-b", Mutation: bundleadapter.JobControlMutation{Object: ClientObject{APIVersion: "batch/v1", Kind: "Job", Key: "ns/job-b", Name: "job-b", Namespace: "ns"}, Action: request.Action}, State: controlProgressPending},
	}}
	first.revision = 1
	first.controls[request.IdempotencyKey] = record
	client.MemoryClient.mu.Lock()
	delete(client.MemoryClient.objects, "ns/job-a")
	client.MemoryClient.mu.Unlock()
	if err := first.persistControlStateLocked(); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Control(context.Background(), request)
	if err != nil || result == nil || !result.Accepted {
		t.Fatalf("delete restart retry = %+v, err = %v", result, err)
	}
	if client.calls["ns/job-a"] != 0 || client.calls["ns/job-b"] != 1 {
		t.Fatalf("delete calls = %+v; absent in-flight delete must be treated as complete", client.calls)
	}
	if !wantMetadata.RetireRun {
		t.Fatal("transport terminal control did not retain run-retirement authority")
	}
	object, found, err := client.Get(context.Background(), restarted.adapter.BundleObject(firstBundle.Key))
	if err != nil || !found {
		t.Fatalf("get terminal bundle: found=%v err=%v", found, err)
	}
	metadata, found, err := DecodeControlMetadata(object.Payload)
	if err != nil || !found || !metadata.RetireRun {
		t.Fatalf("terminal metadata lost run-retirement authority: metadata=%+v found=%v err=%v", metadata, found, err)
	}
}

func TestKubernetesBackendResumesAfterSecondDeleteFailsWithoutRepeatingFirst(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	client := &deleteControlClient{MemoryClient: NewMemoryClient(), failObjectKey: "ns/job-b", failCount: 1}
	first, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	_, _, request := twoBundleControlFixture(t, first)
	request.Action = tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE
	request.IdempotencyKey = "terminate-partial"
	if result, err := first.Control(context.Background(), request); result != nil || !errors.Is(err, ErrControlOutcomeAmbiguous) {
		t.Fatalf("first delete = %+v, err = %v; want partial ambiguous result", result, err)
	}
	if _, found, err := client.Get(context.Background(), ClientObject{APIVersion: "batch/v1", Kind: "Job", Key: "ns/job-a", Name: "job-a", Namespace: "ns"}); err != nil || found {
		t.Fatalf("first deleted job: found=%v err=%v", found, err)
	}

	restarted, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Control(context.Background(), request)
	if err != nil || result == nil || !result.Accepted {
		t.Fatalf("restart delete retry = %+v, err = %v", result, err)
	}
	if client.calls["ns/job-a"] != 1 || client.calls["ns/job-b"] != 2 {
		t.Fatalf("delete calls = %+v; irreversible first delete must not replay", client.calls)
	}
}

func TestKubernetesBackendLeavesChangedInFlightTargetAmbiguous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	client := &partialControlClient{MemoryClient: NewMemoryClient()}
	backend, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	_, _, request := twoBundleControlFixture(t, backend)
	request.Action = tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE
	request.IdempotencyKey = "ambiguous-delete"
	job, found, err := client.Get(context.Background(), ClientObject{APIVersion: "batch/v1", Kind: "Job", Key: "ns/job-a", Name: "job-a", Namespace: "ns"})
	if err != nil || !found {
		t.Fatalf("get job: found=%v err=%v", found, err)
	}
	var payload map[string]any
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	payload["metadata"].(map[string]any)["resourceVersion"] = "8"
	job.Payload, _ = json.Marshal(payload)
	job.Version = "8"
	client.MemoryClient.mu.Lock()
	client.MemoryClient.objects[job.Key] = *job
	client.MemoryClient.mu.Unlock()
	backend.revision = 1
	backend.controls[request.IdempotencyKey] = controlRecord{Digest: controlDigest(request), Pending: true, Request: request, BundleKeys: []string{"ns/bundle-a", "ns/bundle-b"}, Result: &ControlResult{BackendRevision: 1}, TargetProgress: []controlTargetProgress{
		{BundleKey: "ns/bundle-a", Mutation: bundleadapter.JobControlMutation{Object: ClientObject{APIVersion: "batch/v1", Kind: "Job", Key: "ns/job-a", Name: "job-a", Namespace: "ns"}, Action: request.Action, ExpectedVersion: "7"}, State: controlProgressInFlight},
	}}
	result, err := backend.Control(context.Background(), request)
	if result != nil || !errors.Is(err, ErrControlOutcomeAmbiguous) {
		t.Fatalf("changed target result = %+v, err = %v", result, err)
	}
	if client.calls["ns/job-a"] != 0 {
		t.Fatalf("ambiguous delete was replayed: calls = %+v", client.calls)
	}
}

func TestFakeBackendRestoresControlIdempotencyRevision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	first := NewFake()
	if err := first.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	bundle.SourceRunID, bundle.SourceJobID = "run-1", "job-1"
	bundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", Generation: 1}}
	if _, err := first.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	request := ControlRequest{Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, JobID: "job-1", RunID: "run-1", IdempotencyKey: "pause-restart", Targets: []ControlTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", ExpectedGeneration: 1}}}
	created, err := first.Control(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewFake()
	if err := restarted.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.Control(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Idempotent || replayed.BackendRevision != created.BackendRevision {
		t.Fatalf("replayed = %+v, created = %+v", replayed, created)
	}
}

func TestFakeBackendRestoresObservableControlCausality(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	first := NewFake()
	if err := first.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	bundle.SourceRunID, bundle.SourceJobID = "run-1", "job-1"
	bundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", Generation: 1}}
	if _, err := first.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	result, err := first.Control(context.Background(), ControlRequest{Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, JobID: "job-1", RunID: "run-1", RequestID: "request-pause", IdempotencyKey: "pause-visible", Targets: []ControlTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", ExpectedGeneration: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	restarted := NewFake()
	if err := restarted.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := restarted.Snapshot(context.Background(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.ControlCommitted || snapshot.ControlRequestID != "request-pause" || snapshot.ControlIdempotencyKey != "pause-visible" || snapshot.ControlAction != tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE || snapshot.ControlBackendRevision != result.BackendRevision {
		t.Fatalf("recovered causality = %+v", snapshot)
	}
}

func TestKubernetesBackendRecoversPartialCommittedMetadataAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	client := &committedMetadataFailureClient{MemoryClient: NewMemoryClient()}
	first, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	firstBundle := testBundle()
	firstBundle.Key, firstBundle.Fingerprint, firstBundle.SourceRunID, firstBundle.SourceJobID = "ns/bundle-a", "fp-a", "run-1", "job-1"
	firstBundle.Job.ObjectMeta.Name = "job-a"
	firstBundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", Generation: 1}}
	secondBundle, err := api.CloneBundle(firstBundle)
	if err != nil {
		t.Fatal(err)
	}
	secondBundle.Key, secondBundle.Fingerprint, secondBundle.Job.ObjectMeta.Name = "ns/bundle-b", "fp-b", "job-b"
	secondBundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-b", SandboxID: "sandbox-b", Generation: 1}}
	for _, bundle := range []*api.Bundle{firstBundle, secondBundle} {
		if _, err := first.Apply(context.Background(), bundle); err != nil {
			t.Fatal(err)
		}
	}
	client.failBundleKey = secondBundle.Key
	client.failWrites = 1
	request := ControlRequest{
		Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, JobID: "job-1", RunID: "run-1",
		RequestID: "request-both", IdempotencyKey: "pause-both-restart",
		Targets: []ControlTarget{{RuntimeUnitID: "unit-a", SandboxID: "sandbox-a", ExpectedGeneration: 1}, {RuntimeUnitID: "unit-b", SandboxID: "sandbox-b", ExpectedGeneration: 1}},
	}
	if result, err := first.Control(context.Background(), request); err == nil || result != nil {
		t.Fatalf("control result = %+v, err = %v; want committed metadata failure", result, err)
	}
	if client.controlCalls != 2 {
		t.Fatalf("backend mutations = %d, want 2", client.controlCalls)
	}
	for _, bundle := range []*api.Bundle{firstBundle, secondBundle} {
		object, found, err := client.Get(context.Background(), first.adapter.BundleObject(bundle.Key))
		if err != nil || !found {
			t.Fatalf("get bundle %s: found=%v err=%v", bundle.Key, found, err)
		}
		metadata, found, err := DecodeControlMetadata(object.Payload)
		if err != nil || !found || metadata.Committed {
			t.Fatalf("bundle %s raw metadata = %+v, found=%v err=%v; want pending", bundle.Key, metadata, found, err)
		}
	}

	restarted, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	for _, bundle := range []*api.Bundle{firstBundle, secondBundle} {
		snapshots, err := restarted.fakeSnapshots(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		observed := snapshots[len(snapshots)-1]
		if !observed.ControlCommitted || observed.ControlRequestID != request.RequestID || observed.ControlIdempotencyKey != request.IdempotencyKey || observed.ControlBackendRevision != 1 {
			t.Fatalf("bundle %s recovered causality = %+v", bundle.Key, observed)
		}
	}
	replayed, err := restarted.Control(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Accepted || !replayed.Idempotent || replayed.BackendRevision != 1 {
		t.Fatalf("replayed result = %+v", replayed)
	}
	if client.controlCalls != 2 {
		t.Fatalf("backend mutation replayed: calls = %d, want 2", client.controlCalls)
	}
	for _, bundle := range []*api.Bundle{firstBundle, secondBundle} {
		object, _, err := client.Get(context.Background(), restarted.adapter.BundleObject(bundle.Key))
		if err != nil {
			t.Fatal(err)
		}
		metadata, found, err := DecodeControlMetadata(object.Payload)
		if err != nil || !found || !metadata.Committed || metadata.IdempotencyKey != request.IdempotencyKey || metadata.BackendRevision != 1 {
			t.Fatalf("bundle %s repaired metadata = %+v, found=%v err=%v", bundle.Key, metadata, found, err)
		}
	}
}

func TestKubernetesBackendRestoreUsesLatestAcceptedRevision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	client := NewMemoryClient()
	first, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	bundle.SourceRunID, bundle.SourceJobID = "run-1", "job-1"
	bundle.RuntimeTargets = []api.RuntimeTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", Generation: 1}}
	if _, err := first.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	targets := []ControlTarget{{RuntimeUnitID: "unit-1", SandboxID: "sandbox-1", ExpectedGeneration: 1}}
	if _, err := first.Control(context.Background(), ControlRequest{Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, JobID: "job-1", RunID: "run-1", RequestID: "request-1", IdempotencyKey: "control-1", Targets: targets}); err != nil {
		t.Fatal(err)
	}
	stale, found, err := client.Get(context.Background(), first.adapter.BundleObject(bundle.Key))
	if err != nil || !found {
		t.Fatalf("get stale bundle: found=%v err=%v", found, err)
	}
	if _, err := first.Control(context.Background(), ControlRequest{Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME, JobID: "job-1", RunID: "run-1", RequestID: "request-2", IdempotencyKey: "control-2", Targets: targets}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Upsert(context.Background(), *stale); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	if err := restarted.RestoreControlMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	object, found, err := client.Get(context.Background(), restarted.adapter.BundleObject(bundle.Key))
	if err != nil || !found {
		t.Fatalf("get restored bundle: found=%v err=%v", found, err)
	}
	metadata, found, err := DecodeControlMetadata(object.Payload)
	if err != nil || !found || !metadata.Committed || metadata.IdempotencyKey != "control-2" || metadata.BackendRevision != 2 {
		t.Fatalf("restored metadata = %+v, found=%v err=%v", metadata, found, err)
	}
}

func TestControlMetadataForBundleKeepsLegacyAndNewestCommittedCausality(t *testing.T) {
	client := NewMemoryClient()
	backend, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	if _, err := backend.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	object, found, err := client.Get(context.Background(), backend.adapter.BundleObject(bundle.Key))
	if err != nil || !found {
		t.Fatalf("get bundle: found=%v err=%v", found, err)
	}
	legacy := ControlMetadata{RequestID: "legacy-request", IdempotencyKey: "legacy-key", Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, BackendRevision: 2, Committed: true}
	updated, err := encodeControlMetadata(*object, legacy)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Upsert(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	metadata, found, err := backend.controlMetadataForBundle(context.Background(), bundle.Key)
	if err != nil || !found || metadata != legacy {
		t.Fatalf("legacy metadata = %+v, found=%v err=%v", metadata, found, err)
	}

	backend.controls["unrelated-newer-ledger"] = controlRecord{
		Digest: "unrelated", Request: ControlRequest{RequestID: "unrelated-request", IdempotencyKey: "unrelated-newer-ledger", Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME},
		BundleKeys: []string{"ns/other-bundle"}, Result: &ControlResult{Accepted: true, BackendRevision: 3},
	}
	backend.controls["older-ledger"] = controlRecord{
		Digest: "older", Request: ControlRequest{RequestID: "older-request", IdempotencyKey: "older-ledger", Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME},
		BundleKeys: []string{bundle.Key}, Result: &ControlResult{Accepted: true, BackendRevision: 1},
	}
	metadata, found, err = backend.controlMetadataForBundle(context.Background(), bundle.Key)
	if err != nil || !found || metadata != legacy {
		t.Fatalf("metadata with older ledger = %+v, found=%v err=%v", metadata, found, err)
	}
	newest := ControlMetadata{RequestID: "newest-request", IdempotencyKey: "newest-ledger", Action: tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE, BackendRevision: 4, Committed: true, RetireRun: true}
	backend.controls[newest.IdempotencyKey] = controlRecord{
		Digest: "newest", Request: ControlRequest{RequestID: newest.RequestID, IdempotencyKey: newest.IdempotencyKey, Action: newest.Action},
		BundleKeys: []string{bundle.Key}, Result: &ControlResult{Accepted: true, BackendRevision: newest.BackendRevision},
	}
	metadata, found, err = backend.controlMetadataForBundle(context.Background(), bundle.Key)
	if err != nil || !found || metadata != newest {
		t.Fatalf("metadata with newer ledger = %+v, found=%v err=%v", metadata, found, err)
	}
}

func TestControlMetadataForBundlePrefersCommittedLedgerOverStalePendingAnnotation(t *testing.T) {
	client := NewMemoryClient()
	backend, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	bundle := testBundle()
	if _, err := backend.Apply(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}

	committed := ControlMetadata{
		RequestID:       "request-committed",
		IdempotencyKey:  "control-committed",
		Action:          tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE,
		BackendRevision: 2,
		Committed:       true,
	}
	backend.controls[committed.IdempotencyKey] = controlRecord{
		Digest:     "committed",
		Request:    ControlRequest{RequestID: committed.RequestID, IdempotencyKey: committed.IdempotencyKey, Action: committed.Action},
		BundleKeys: []string{bundle.Key},
		Result:     &ControlResult{Accepted: true, BackendRevision: committed.BackendRevision},
	}

	object, found, err := client.Get(context.Background(), backend.adapter.BundleObject(bundle.Key))
	if err != nil || !found {
		t.Fatalf("get bundle: found=%v err=%v", found, err)
	}
	stalePending, err := encodeControlMetadata(*object, ControlMetadata{
		RequestID:       "request-stale",
		IdempotencyKey:  "control-stale",
		Action:          tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME,
		BackendRevision: 3,
		Committed:       false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.Upsert(context.Background(), stalePending); err != nil {
		t.Fatal(err)
	}

	metadata, found, err := backend.controlMetadataForBundle(context.Background(), bundle.Key)
	if err != nil || !found {
		t.Fatalf("controlMetadataForBundle found=%v err=%v", found, err)
	}
	if metadata != committed {
		t.Fatalf("metadata = %+v, want committed ledger %+v", metadata, committed)
	}
}

func TestKubernetesBackendRestoreCommittedLedgerAfterSecondControlMetadataWriteFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "controls.json")
	client := &committedMetadataFailureClient{MemoryClient: NewMemoryClient()}
	first, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	firstBundle, secondBundle, request := twoBundleControlFixture(t, first)

	if _, err := first.Control(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	request2 := request
	request2.Action = tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME
	request2.RequestID = "request-resume-both"
	request2.IdempotencyKey = "resume-both"
	client.failBundleKey = secondBundle.Key
	client.failWrites = 1
	if result, err := first.Control(context.Background(), request2); err == nil || result != nil {
		t.Fatalf("second control result = %+v, err = %v; want committed metadata failure", result, err)
	}

	for _, bundle := range []*api.Bundle{firstBundle, secondBundle} {
		object, found, err := client.Get(context.Background(), first.adapter.BundleObject(bundle.Key))
		if err != nil || !found {
			t.Fatalf("get bundle %s: found=%v err=%v", bundle.Key, found, err)
		}
		metadata, found, err := DecodeControlMetadata(object.Payload)
		if err != nil || !found {
			t.Fatalf("decode bundle %s metadata: %+v found=%v err=%v", bundle.Key, metadata, found, err)
		}
		if metadata.IdempotencyKey != request2.IdempotencyKey || metadata.BackendRevision != 2 || metadata.Committed {
			t.Fatalf("bundle %s metadata = %+v, want second control pending annotation", bundle.Key, metadata)
		}
	}

	restarted, err := NewKubernetes(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.SetControlStatePath(path); err != nil {
		t.Fatal(err)
	}
	for _, bundle := range []*api.Bundle{firstBundle, secondBundle} {
		snapshots, err := restarted.fakeSnapshots(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		snapshot := snapshots[len(snapshots)-1]
		if !snapshot.ControlCommitted || snapshot.ControlRequestID != request2.RequestID || snapshot.ControlIdempotencyKey != request2.IdempotencyKey || snapshot.ControlBackendRevision != 2 {
			t.Fatalf("bundle %s recovered causality = %+v", bundle.Key, snapshot)
		}
	}
	if err := restarted.RestoreControlMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, bundle := range []*api.Bundle{firstBundle, secondBundle} {
		object, found, err := client.Get(context.Background(), restarted.adapter.BundleObject(bundle.Key))
		if err != nil || !found {
			t.Fatalf("get restored bundle %s: found=%v err=%v", bundle.Key, found, err)
		}
		metadata, found, err := DecodeControlMetadata(object.Payload)
		if err != nil || !found || !metadata.Committed || metadata.IdempotencyKey != request2.IdempotencyKey || metadata.BackendRevision != 2 {
			t.Fatalf("restored bundle %s metadata = %+v, found=%v err=%v", bundle.Key, metadata, found, err)
		}
	}
}

func testBundle() *api.Bundle {
	return &api.Bundle{
		Key:         "ns/bundle",
		Namespace:   "ns",
		Generation:  1,
		Fingerprint: "fp-1",
		Workload: api.Workload{
			TypeMeta:   api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"},
			ObjectMeta: api.ObjectMeta{Name: "workload-a", Namespace: "ns", Generation: 1},
		},
		Job: api.Job{
			TypeMeta:   api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
			ObjectMeta: api.ObjectMeta{Name: "job-a", Namespace: "ns", Generation: 1},
		},
		RuntimeClass: &api.RuntimeClass{
			TypeMeta:   api.TypeMeta{APIVersion: "node.k8s.io/v1", Kind: "RuntimeClass"},
			ObjectMeta: api.ObjectMeta{Name: "runtime-a", Generation: 1},
			Handler:    "runc",
		},
		ResourceClaim: &api.ResourceClaim{
			TypeMeta:   api.TypeMeta{APIVersion: "resource.k8s.io/v1beta1", Kind: "ResourceClaim"},
			ObjectMeta: api.ObjectMeta{Name: "claim-a", Namespace: "ns", Generation: 1},
		},
	}
}
