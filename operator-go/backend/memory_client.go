package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

type MemoryClient struct {
	mu           sync.RWMutex
	objects      map[string]ClientObject
	nextVersion  uint64
	capabilities compiler.CapabilitySet
}

func NewMemoryClient() *MemoryClient {
	return &MemoryClient{
		objects:      make(map[string]ClientObject),
		nextVersion:  1,
		capabilities: compiler.DefaultCapabilitySet(),
	}
}

func (c *MemoryClient) Upsert(_ context.Context, object ClientObject) (bool, *ClientObject, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	object = c.withResourceVersionLocked(object)
	if previous, ok := c.objects[object.Key]; ok {
		c.objects[object.Key] = object
		clone := previous
		return false, &clone, nil
	}
	c.objects[object.Key] = object
	return true, nil, nil
}

func (c *MemoryClient) EnsureRuntimeClass(ctx context.Context, object ClientObject) (bool, *ClientObject, error) {
	if object.Kind != "RuntimeClass" {
		return c.Upsert(ctx, object)
	}
	current, found, err := c.Get(ctx, object)
	if err != nil {
		return false, nil, err
	}
	if !found {
		return c.Upsert(ctx, object)
	}
	desiredHandler, err := runtimeClassHandlerFromPayload(object.Payload)
	if err != nil {
		return false, current, err
	}
	existingHandler, err := runtimeClassHandlerFromPayload(current.Payload)
	if err != nil {
		return false, current, err
	}
	if desiredHandler != existingHandler {
		return false, current, fmt.Errorf("runtime class %s handler conflict: existing=%q desired=%q", object.Name, existingHandler, desiredHandler)
	}
	return false, current, nil
}

func (c *MemoryClient) Delete(_ context.Context, object ClientObject) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.objects[object.Key]; !ok {
		return false, nil
	}
	delete(c.objects, object.Key)
	return true, nil
}

func (c *MemoryClient) Get(_ context.Context, object ClientObject) (*ClientObject, bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	stored, ok := c.objects[object.Key]
	if !ok {
		return nil, false, nil
	}
	clone := stored
	return &clone, true, nil
}

func (c *MemoryClient) ListBundles(_ context.Context) ([]ClientObject, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	objects := make([]ClientObject, 0)
	for _, object := range c.objects {
		if object.Kind == "JobRunBundle" {
			objects = append(objects, object)
		}
	}
	return objects, nil
}

func (c *MemoryClient) ControlJob(_ context.Context, object ClientObject, action tgsrlv1.JobCommandType, _ string) (*bundleadapter.JobControlReadback, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.controlJobLocked(object, action)
}

func (c *MemoryClient) SetBundleControlMetadata(_ context.Context, bundleKeys []string, metadata ControlMetadata) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	updates := make(map[string]ClientObject, len(bundleKeys))
	for _, key := range bundleKeys {
		stored, ok := c.objects[key]
		if !ok {
			return fmt.Errorf("%w: bundle %q", ErrRuntimeNotFound, key)
		}
		updated, err := encodeControlMetadata(stored, metadata)
		if err != nil {
			return err
		}
		updates[key] = updated
	}
	for key, updated := range updates {
		updated = c.withResourceVersionLocked(updated)
		c.objects[key] = updated
	}
	return nil
}

func (c *MemoryClient) DiscoverCapabilities(_ context.Context) (compiler.CapabilitySet, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return compiler.CapabilitySet{
		GPUProfiles:         cloneBoolMap(c.capabilities.GPUProfiles),
		RuntimeClasses:      cloneStringMap(c.capabilities.RuntimeClasses),
		NodeSelectors:       cloneNestedStringMap(c.capabilities.NodeSelectors),
		DefaultNodeSelector: cloneStringMap(c.capabilities.DefaultNodeSelector),
	}, nil
}

func (c *MemoryClient) SetCapabilities(values compiler.CapabilitySet) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capabilities = compiler.CapabilitySet{
		GPUProfiles:         cloneBoolMap(values.GPUProfiles),
		RuntimeClasses:      cloneStringMap(values.RuntimeClasses),
		NodeSelectors:       cloneNestedStringMap(values.NodeSelectors),
		DefaultNodeSelector: cloneStringMap(values.DefaultNodeSelector),
	}
}

func (c *MemoryClient) controlJobLocked(object ClientObject, action tgsrlv1.JobCommandType) (*bundleadapter.JobControlReadback, error) {
	stored, ok := c.objects[object.Key]
	if !ok {
		return nil, ErrRuntimeNotFound
	}
	var job map[string]any
	if err := json.Unmarshal(stored.Payload, &job); err != nil {
		return nil, fmt.Errorf("decode fake job: %w", err)
	}
	spec, _ := job["spec"].(map[string]any)
	if spec == nil {
		spec = make(map[string]any)
		job["spec"] = spec
	}
	status, _ := job["status"].(map[string]any)
	if status == nil {
		status = make(map[string]any)
		job["status"] = status
	}
	switch action {
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE:
		spec["suspend"] = true
		status["paused"] = true
		status["active"] = float64(0)
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:
		spec["suspend"] = false
		status["paused"] = false
		status["active"] = float64(1)
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE:
		status["paused"] = false
		status["active"] = float64(0)
		status["deleted"] = true
		payload, err := json.Marshal(job)
		if err != nil {
			return nil, err
		}
		stored.Payload = payload
		stored = c.withResourceVersionLocked(stored)
		c.objects[object.Key] = stored
		return &bundleadapter.JobControlReadback{Deleted: true, ObservedAt: time.Now().UTC()}, nil
	default:
		return nil, fmt.Errorf("unsupported fake job action %s", action)
	}
	payload, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}
	stored.Payload = payload
	stored = c.withResourceVersionLocked(stored)
	c.objects[object.Key] = stored
	return &bundleadapter.JobControlReadback{
		Paused: asBool(status["paused"]), Active: asUint32(status["active"]),
		Succeeded: asUint32(status["succeeded"]), Failed: asUint32(status["failed"]),
		ObservedAt: time.Now().UTC(),
	}, nil
}

func (c *MemoryClient) setInitialJobState(bundle *api.Bundle) {
	if bundle == nil {
		return
	}
	workloadObject := metaObject(bundle.Workload.TypeMeta.APIVersion, bundle.Workload.TypeMeta.Kind, bundle.Workload.ObjectMeta)
	claimObject := ClientObject{}
	if bundle.ResourceClaim != nil {
		claimObject = metaObject(bundle.ResourceClaim.TypeMeta.APIVersion, bundle.ResourceClaim.TypeMeta.Kind, bundle.ResourceClaim.ObjectMeta)
	}
	object := metaObject(bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind, bundle.Job.ObjectMeta)
	c.mu.Lock()
	defer c.mu.Unlock()
	if workload, ok := c.objects[workloadObject.Key]; ok {
		var storedWorkload api.Workload
		if json.Unmarshal(workload.Payload, &storedWorkload) == nil {
			storedWorkload.Status.Admitted = true
			storedWorkload.Status.Phase = "admitted"
			storedWorkload.Status.Reason = "fake-backend-admitted"
			if payload, err := json.Marshal(storedWorkload); err == nil {
				workload.Payload = payload
				workload = c.withResourceVersionLocked(workload)
				c.objects[workloadObject.Key] = workload
			}
		}
	}
	if claimObject.Key != "" {
		if claim, ok := c.objects[claimObject.Key]; ok {
			var storedClaim api.ResourceClaim
			if json.Unmarshal(claim.Payload, &storedClaim) == nil {
				storedClaim.Status.Allocation = &api.AllocationResult{Devices: map[string]any{"allocated": true}}
				if payload, err := json.Marshal(storedClaim); err == nil {
					claim.Payload = payload
					claim = c.withResourceVersionLocked(claim)
					c.objects[claimObject.Key] = claim
				}
			}
		}
	}
	stored, ok := c.objects[object.Key]
	if !ok {
		return
	}
	var job api.Job
	if json.Unmarshal(stored.Payload, &job) != nil {
		return
	}
	job.Status = api.JobStatus{Active: maxUint32(1, bundle.Job.Spec.Parallelism)}
	payload, err := json.Marshal(job)
	if err != nil {
		return
	}
	stored.Payload = payload
	stored = c.withResourceVersionLocked(stored)
	c.objects[object.Key] = stored
}

func (c *MemoryClient) withResourceVersionLocked(object ClientObject) ClientObject {
	version := fmt.Sprintf("%d", c.nextVersion)
	c.nextVersion++
	if payload, err := injectObjectResourceVersion(object.Payload, version); err == nil {
		object.Payload = payload
	}
	object.Version = version
	return object
}

func cloneBoolMap(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]bool, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func cloneNestedStringMap(src map[string]map[string]string) map[string]map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]map[string]string, len(src))
	for key, value := range src {
		dst[key] = cloneStringMap(value)
	}
	return dst
}

func injectObjectResourceVersion(payload []byte, resourceVersion string) ([]byte, error) {
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		return nil, err
	}
	metadata, _ := object["metadata"].(map[string]any)
	if metadata == nil {
		metadata = map[string]any{}
		object["metadata"] = metadata
	}
	metadata["resourceVersion"] = resourceVersion
	return json.Marshal(object)
}

func runtimeClassHandlerFromPayload(payload []byte) (string, error) {
	var object struct {
		Handler string `json:"handler"`
	}
	if err := json.Unmarshal(payload, &object); err != nil {
		return "", fmt.Errorf("decode runtime class: %w", err)
	}
	if object.Handler == "" {
		return "", fmt.Errorf("runtime class handler is required")
	}
	return object.Handler, nil
}

func maxUint32(left, right uint32) uint32 {
	if left > right {
		return left
	}
	return right
}

func asBool(value any) bool {
	result, _ := value.(bool)
	return result
}

func asUint32(value any) uint32 {
	switch typed := value.(type) {
	case float64:
		return uint32(typed)
	case uint32:
		return typed
	case int:
		return uint32(typed)
	default:
		return 0
	}
}
