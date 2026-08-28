package bundleadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
)

var (
	ErrNotFound     = errors.New("backend object not found")
	ErrBundleAbsent = errors.New("materialized bundle is absent")
)

type Object struct {
	APIVersion string
	Kind       string
	Key        string
	Name       string
	Namespace  string
	Generation uint64
	Version    string
	Payload    []byte
}

type Snapshot struct {
	ObservedGeneration      uint64
	WorkloadAdmitted        bool
	ResourceClaimsAllocated bool
	JobActive               uint32
	JobSucceeded            uint32
	JobFailed               uint32
	JobPaused               bool
	JobDeleted              bool
	Reason                  string
	ObservedAt              time.Time
	ControlRequestID        string
	ControlIdempotencyKey   string
	ControlAction           tgsrlv1.JobCommandType
	ControlBackendRevision  uint64
	ControlCommitted        bool
}

// JobControlReadback is the minimal backend-neutral state returned after a
// lifecycle mutation has been read back from the execution substrate.
type JobControlReadback struct {
	Paused     bool
	Deleted    bool
	Deleting   bool
	Active     uint32
	Succeeded  uint32
	Failed     uint32
	ObservedAt time.Time
}

// JobControlMutation contains the backend object and optimistic concurrency
// precondition for one lifecycle mutation. It lives in this neutral package so
// Kubernetes clients do not depend back on the backend orchestration package.
type JobControlMutation struct {
	Object          Object
	Action          tgsrlv1.JobCommandType
	ExpectedVersion string
}

type Reader interface {
	GetPath(ctx context.Context, path string) ([]byte, error)
}

type Stream interface {
	Recv() (*Snapshot, error)
}

type Adapter interface {
	BundleObject(key string) Object
	Materialize(bundle *api.Bundle) ([]Object, error)
	DecodeBundle(object Object) (*api.Bundle, error)
	CollectionPath(object Object, defaultNamespace string) string
	ObjectPath(object Object, defaultNamespace string) string
	Watch(ctx context.Context, reader Reader, bundle *api.Bundle) (Stream, error)
}

type bundleObject struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Metadata   api.ObjectMeta   `json:"metadata"`
	Spec       bundleObjectSpec `json:"spec"`
}

type bundleObjectSpec struct {
	Bundle api.Bundle `json:"bundle"`
}

type resourceDescriptor struct {
	group      string
	version    string
	resource   string
	namespaced bool
}

func descriptorFor(object Object) resourceDescriptor {
	switch kind := defaultKindFor(object); kind {
	case "JobRunBundle":
		return resourceDescriptor{group: "tgsrl.io", version: "v1alpha1", resource: "jobrunbundles", namespaced: true}
	case "Workload":
		return descriptorWithVersion(object, "kueue.x-k8s.io", "v1beta1", "workloads", true)
	case "Job":
		return descriptorWithVersion(object, "batch", "v1", "jobs", true)
	case "RuntimeClass":
		return descriptorWithVersion(object, "node.k8s.io", "v1", "runtimeclasses", false)
	case "ResourceClaim":
		return descriptorWithVersion(object, "resource.k8s.io", "v1beta1", "resourceclaims", true)
	default:
		return resourceDescriptor{group: "tgsrl.io", version: "v1alpha1", resource: strings.ToLower(kind) + "s", namespaced: true}
	}
}

func descriptorWithVersion(object Object, group, fallbackVersion, resource string, namespaced bool) resourceDescriptor {
	version := fallbackVersion
	if parts := strings.SplitN(object.APIVersion, "/", 2); len(parts) == 2 && parts[0] == group && parts[1] != "" {
		version = parts[1]
	}
	return resourceDescriptor{group: group, version: version, resource: resource, namespaced: namespaced}
}

func defaultKindFor(object Object) string {
	if strings.TrimSpace(object.Kind) != "" {
		return object.Kind
	}
	switch {
	case strings.Contains(object.APIVersion, "kueue.x-k8s.io"):
		return "Workload"
	case strings.HasPrefix(object.APIVersion, "batch/"):
		return "Job"
	case strings.HasPrefix(object.APIVersion, "node.k8s.io/"):
		return "RuntimeClass"
	case strings.Contains(object.APIVersion, "resource.k8s.io"):
		return "ResourceClaim"
	default:
		return object.Kind
	}
}

func collectionPath(object Object, defaultNamespace string) string {
	resource := descriptorFor(object)
	if resource.namespaced {
		namespace := namespaceFor(object.Namespace, defaultNamespace)
		if resource.group == "" {
			return fmt.Sprintf("/api/%s/namespaces/%s/%s", resource.version, namespace, resource.resource)
		}
		return fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s", resource.group, resource.version, namespace, resource.resource)
	}
	if resource.group == "" {
		return fmt.Sprintf("/api/%s/%s", resource.version, resource.resource)
	}
	return fmt.Sprintf("/apis/%s/%s/%s", resource.group, resource.version, resource.resource)
}

func objectPath(object Object, defaultNamespace string) string {
	return collectionPath(object, defaultNamespace) + "/" + object.Name
}

func bundleObjectFor(key string) Object {
	namespace, name := splitKey(key)
	return Object{
		APIVersion: "tgsrl.io/v1alpha1",
		Kind:       "JobRunBundle",
		Key:        key,
		Name:       name,
		Namespace:  namespace,
	}
}

func newBundleObject(bundle *api.Bundle) bundleObject {
	namespace, name := splitKey(bundle.Key)
	return bundleObject{
		APIVersion: "tgsrl.io/v1alpha1",
		Kind:       "JobRunBundle",
		Metadata: api.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: bundleObjectSpec{
			Bundle: *bundle,
		},
	}
}

func metaFor(apiVersion, kind string, objectMeta api.ObjectMeta) Object {
	return Object{
		APIVersion: apiVersion,
		Kind:       kind,
		Key:        objectMeta.Namespace + "/" + objectMeta.Name,
		Name:       objectMeta.Name,
		Namespace:  objectMeta.Namespace,
		Generation: uint64(objectMeta.Generation),
	}
}

func marshalObject(meta Object, value any) (Object, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return Object{}, fmt.Errorf("marshal %s %s: %w", meta.Kind, meta.Key, err)
	}
	meta.Payload = payload
	return meta, nil
}

func cloneBundle(bundle *api.Bundle) (*api.Bundle, error) {
	if bundle == nil {
		return nil, fmt.Errorf("bundle is required")
	}
	return api.CloneBundle(bundle)
}

func decodeBundlePayload(payload []byte) (*api.Bundle, error) {
	var stored bundleObject
	if err := json.Unmarshal(payload, &stored); err != nil {
		return nil, fmt.Errorf("decode jobrunbundle: %w", err)
	}
	bundle := stored.Spec.Bundle
	return api.CloneBundle(&bundle)
}

func splitKey(key string) (string, string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", key
}

func namespaceFor(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func defaultReason(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

type sliceStream struct {
	snapshots []*Snapshot
	index     int
}

func (s *sliceStream) Recv() (*Snapshot, error) {
	if s.index >= len(s.snapshots) {
		return nil, io.EOF
	}
	snapshot := *s.snapshots[s.index]
	s.index++
	return &snapshot, nil
}
