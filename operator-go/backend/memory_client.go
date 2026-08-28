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
)

type MemoryClient struct {
	mu      sync.RWMutex
	objects map[string]ClientObject
}

func NewMemoryClient() *MemoryClient {
	return &MemoryClient{
		objects: make(map[string]ClientObject),
	}
}

func (c *MemoryClient) Upsert(_ context.Context, object ClientObject) (bool, *ClientObject, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if previous, ok := c.objects[object.Key]; ok {
		c.objects[object.Key] = object
		clone := previous
		return false, &clone, nil
	}
	c.objects[object.Key] = object
	return true, nil, nil
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
		c.objects[key] = updated
	}
	return nil
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
	object := metaObject(bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind, bundle.Job.ObjectMeta)
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.objects[object.Key] = stored
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
