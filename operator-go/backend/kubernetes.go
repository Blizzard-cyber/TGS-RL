package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

type ClientObject = bundleadapter.Object

const (
	bundleFinalizer             = "tgsrl.io/operator-protect"
	bundleObjectKindsAnnotation = "tgsrl.io/object-kinds"
	bundleGenerationAnnotation  = "tgsrl.io/bundle-generation"
	bundleSelectedGPUProfile    = "tgsrl.io/selected-gpu-profile"
)

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

type DeleteClient interface {
	Delete(ctx context.Context, object ClientObject) (deleted bool, err error)
}

type CapabilityClient interface {
	DiscoverCapabilities(ctx context.Context) (compiler.CapabilitySet, error)
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
	previous, ok, err := b.getLocked(ctx, clone.Key)
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
	if markerObject != nil {
		annotatedMarker, err := b.prepareBundleMarker(*markerObject, clone, desiredObjects)
		if err != nil {
			return nil, err
		}
		markerObject = &annotatedMarker
	}
	for _, object := range desiredObjects {
		if err := b.upsertDesiredObject(ctx, object); err != nil {
			return nil, err
		}
	}
	// Generation-scoped objects are prepared before the previous generation is
	// removed. This keeps the last committed workload runnable when creating a
	// replacement fails partway through. The marker remains the commit point and
	// is updated only after the old generation has been cleaned up.
	if ok {
		if err := b.cleanupOrphansLocked(ctx, previous, clone, desiredObjects); err != nil {
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
		// A committed marker proves this generation was fully materialized.
		// Kueue and Kubernetes then own admission, suspend, selector, owner and
		// status fields on Workload/Job/claims. Replaying a full PUT would erase
		// those fields or collide with immutable Job selectors. Only recreate a
		// missing generation-scoped object; lifecycle changes use ControlJob.
		_, found, err := b.client.Get(ctx, object)
		if err != nil {
			return err
		}
		if found {
			continue
		}
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
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	return b.getLocked(ctx, key)
}

func (b *KubernetesBackend) List(ctx context.Context) ([]*api.Bundle, error) {
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	return b.listLocked(ctx)
}

// Cleanup removes every object managed by a bundle and then its durable
// marker. It is idempotent and is called only after the terminal observation
// has been persisted by Runtime.
func (b *KubernetesBackend) Cleanup(ctx context.Context, key string, expectedGeneration uint64) error {
	b.mutationMu.Lock()
	defer b.mutationMu.Unlock()
	marker := b.adapter.BundleObject(key)
	object, found, err := b.client.Get(ctx, marker)
	if err != nil || !found {
		return err
	}
	bundle, metadata, err := decodeStoredBundleObject(*object)
	if err != nil {
		return err
	}
	if expectedGeneration == 0 || bundle.Generation != expectedGeneration {
		return nil
	}
	deleteClient, ok := b.client.(DeleteClient)
	if !ok {
		return fmt.Errorf("backend client does not support bundle cleanup")
	}
	managed, err := b.managedObjectsForStoredBundle(bundle, metadata)
	if err != nil {
		return err
	}
	for _, child := range managed {
		if child.Kind == "RuntimeClass" {
			continue
		}
		if _, err := deleteClient.Delete(ctx, child); err != nil {
			return err
		}
	}
	updated, _, err := updateStoredBundleMetadata(*object, func(meta *api.ObjectMeta) {
		meta.Finalizers = removeString(meta.Finalizers, bundleFinalizer)
	})
	if err != nil {
		return err
	}
	if _, _, err := b.client.Upsert(ctx, updated); err != nil {
		return err
	}
	_, err = deleteClient.Delete(ctx, marker)
	return err
}

func (b *KubernetesBackend) getLocked(ctx context.Context, key string) (*api.Bundle, bool, error) {
	object, ok, err := b.client.Get(ctx, b.adapter.BundleObject(key))
	if err != nil || !ok {
		return nil, ok, err
	}
	object, ok, err = b.reconcileDeletingBundleLocked(ctx, *object)
	if err != nil || !ok {
		return nil, ok, err
	}
	bundle, _, err := decodeStoredBundleObject(*object)
	if err != nil {
		return nil, false, err
	}
	return bundle, true, nil
}

func (b *KubernetesBackend) listLocked(ctx context.Context) ([]*api.Bundle, error) {
	objects, err := b.client.ListBundles(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	bundles := make([]*api.Bundle, 0, len(objects))
	for _, object := range objects {
		current, ok, err := b.reconcileDeletingBundleLocked(ctx, object)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		bundle, _, err := decodeStoredBundleObject(*current)
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, bundle)
	}
	return bundles, nil
}

func (b *KubernetesBackend) DiscoverCapabilities(ctx context.Context) (compiler.CapabilitySet, error) {
	if client, ok := b.client.(CapabilityClient); ok {
		return client.DiscoverCapabilities(ctx)
	}
	return compiler.DefaultCapabilitySet(), nil
}

func (b *KubernetesBackend) prepareBundleMarker(marker ClientObject, bundle *api.Bundle, desiredObjects []ClientObject) (ClientObject, error) {
	stored, err := decodeStoredBundlePayload(marker.Payload)
	if err != nil {
		return ClientObject{}, err
	}
	if stored.Metadata.Annotations == nil {
		stored.Metadata.Annotations = make(map[string]string)
	}
	stored.Metadata.Annotations[bundleGenerationAnnotation] = fmt.Sprintf("%d", bundle.Generation)
	stored.Metadata.Annotations[bundleSelectedGPUProfile] = bundle.GPUProfile
	stored.Metadata.Annotations[bundleObjectKindsAnnotation] = encodeObjectKinds(desiredObjects)
	if !containsString(stored.Metadata.Finalizers, bundleFinalizer) {
		stored.Metadata.Finalizers = append(stored.Metadata.Finalizers, bundleFinalizer)
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return ClientObject{}, fmt.Errorf("encode bundle marker: %w", err)
	}
	marker.Payload = payload
	return marker, nil
}

func (b *KubernetesBackend) cleanupOrphansLocked(ctx context.Context, previous, current *api.Bundle, desiredObjects []ClientObject) error {
	if previous == nil {
		return nil
	}
	deleteClient, ok := b.client.(DeleteClient)
	if !ok {
		return nil
	}
	desiredKeys := make(map[string]ClientObject, len(desiredObjects))
	for _, object := range desiredObjects {
		desiredKeys[object.Key] = object
	}
	previousObjects, err := b.adapter.Materialize(previous)
	if err != nil {
		return err
	}
	oldDesired, _ := splitCompletionMarker(previousObjects)
	for _, object := range oldDesired {
		if _, keep := desiredKeys[object.Key]; keep {
			continue
		}
		if object.Kind == "RuntimeClass" && current != nil && current.RuntimeClass != nil && current.RuntimeClass.ObjectMeta.Name == object.Name {
			continue
		}
		if _, err := deleteClient.Delete(ctx, object); err != nil {
			return err
		}
	}
	return nil
}

func encodeObjectKinds(objects []ClientObject) string {
	values := make([]string, 0, len(objects))
	for _, object := range objects {
		values = append(values, object.Kind+":"+object.Key)
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func removeString(values []string, want string) []string {
	if len(values) == 0 {
		return nil
	}
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if value != want {
			filtered = append(filtered, value)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

type storedBundlePayload struct {
	APIVersion string         `json:"apiVersion"`
	Kind       string         `json:"kind"`
	Metadata   api.ObjectMeta `json:"metadata"`
	Spec       struct {
		Bundle api.Bundle `json:"bundle"`
	} `json:"spec"`
}

func decodeStoredBundlePayload(payload []byte) (storedBundlePayload, error) {
	var stored storedBundlePayload
	if err := json.Unmarshal(payload, &stored); err != nil {
		return storedBundlePayload{}, fmt.Errorf("decode bundle marker: %w", err)
	}
	return stored, nil
}

func decodeStoredBundleObject(object ClientObject) (*api.Bundle, api.ObjectMeta, error) {
	stored, err := decodeStoredBundlePayload(object.Payload)
	if err != nil {
		return nil, api.ObjectMeta{}, err
	}
	bundle, err := api.CloneBundle(&stored.Spec.Bundle)
	if err != nil {
		return nil, api.ObjectMeta{}, err
	}
	return bundle, stored.Metadata, nil
}

func splitObjectKey(key string) (string, string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", key
}

func objectFromKindAndKey(kind, key string) ClientObject {
	namespace, name := splitObjectKey(key)
	return ClientObject{Kind: kind, Key: key, Name: name, Namespace: namespace}
}

func parseManagedObjects(encoded string) []ClientObject {
	if strings.TrimSpace(encoded) == "" {
		return nil
	}
	parts := strings.Split(encoded, ",")
	objects := make([]ClientObject, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		fields := strings.SplitN(part, ":", 2)
		if len(fields) != 2 {
			continue
		}
		kind := strings.TrimSpace(fields[0])
		key := strings.TrimSpace(fields[1])
		if kind == "" || key == "" {
			continue
		}
		objects = append(objects, objectFromKindAndKey(kind, key))
	}
	return objects
}

func (b *KubernetesBackend) managedObjectsForStoredBundle(bundle *api.Bundle, metadata api.ObjectMeta) ([]ClientObject, error) {
	if metadata.Annotations != nil {
		if objects := parseManagedObjects(metadata.Annotations[bundleObjectKindsAnnotation]); len(objects) > 0 {
			return objects, nil
		}
	}
	objects, err := mustMaterializeBundle(b.adapter, bundle)
	if err != nil {
		return nil, err
	}
	desiredObjects, _ := splitCompletionMarker(objects)
	return desiredObjects, nil
}

func mustMaterializeBundle(adapter bundleadapter.Adapter, bundle *api.Bundle) ([]ClientObject, error) {
	objects, err := adapter.Materialize(bundle)
	if err != nil {
		return nil, err
	}
	return objects, nil
}

func updateStoredBundleMetadata(object ClientObject, mutate func(*api.ObjectMeta)) (ClientObject, api.ObjectMeta, error) {
	stored, err := decodeStoredBundlePayload(object.Payload)
	if err != nil {
		return ClientObject{}, api.ObjectMeta{}, err
	}
	mutate(&stored.Metadata)
	if stored.APIVersion == "" {
		stored.APIVersion = object.APIVersion
	}
	if stored.Kind == "" {
		stored.Kind = object.Kind
	}
	payload, err := json.Marshal(stored)
	if err != nil {
		return ClientObject{}, api.ObjectMeta{}, fmt.Errorf("encode bundle marker: %w", err)
	}
	object.Payload = payload
	return object, stored.Metadata, nil
}

func (b *KubernetesBackend) reconcileDeletingBundleLocked(ctx context.Context, object ClientObject) (*ClientObject, bool, error) {
	bundle, metadata, err := decodeStoredBundleObject(object)
	if err != nil {
		return nil, false, err
	}
	if strings.TrimSpace(metadata.DeletionTimestamp) == "" {
		return &object, true, nil
	}
	deleteClient, ok := b.client.(DeleteClient)
	if !ok {
		return &object, true, nil
	}
	managedObjects, err := b.managedObjectsForStoredBundle(bundle, metadata)
	if err != nil {
		return nil, false, err
	}
	allGone := true
	for _, managed := range managedObjects {
		if _, err := deleteClient.Delete(ctx, managed); err != nil {
			return nil, false, err
		}
		if _, found, err := b.client.Get(ctx, managed); err != nil {
			return nil, false, err
		} else if found {
			allGone = false
		}
	}
	if !allGone {
		return &object, true, nil
	}
	if containsString(metadata.Finalizers, bundleFinalizer) {
		updated, updatedMetadata, err := updateStoredBundleMetadata(object, func(meta *api.ObjectMeta) {
			meta.Finalizers = removeString(meta.Finalizers, bundleFinalizer)
		})
		if err != nil {
			return nil, false, err
		}
		if _, _, err := b.client.Upsert(ctx, updated); err != nil {
			return nil, false, err
		}
		object = updated
		metadata = updatedMetadata
	}
	if strings.TrimSpace(metadata.DeletionTimestamp) != "" && len(metadata.Finalizers) == 0 {
		if _, err := deleteClient.Delete(ctx, object); err != nil {
			return nil, false, err
		}
	}
	current, found, err := b.client.Get(ctx, b.adapter.BundleObject(bundle.Key))
	if err != nil || !found {
		return nil, found, err
	}
	return current, true, nil
}

// Snapshot returns a point-in-time backend observation. It is used by the
// long-lived ObservationManager; lifecycle RPCs never publish observations.
