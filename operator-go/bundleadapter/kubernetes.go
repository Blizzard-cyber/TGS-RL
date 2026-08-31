package bundleadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

type KubernetesAdapter struct {
	interval time.Duration
	now      func() time.Time
}

func controlMetadataFromBundleBody(body []byte) (string, string, tgsrlv1.JobCommandType, uint64, bool, error) {
	var object struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &object); err != nil {
		return "", "", 0, 0, false, err
	}
	values := object.Metadata.Annotations
	if values == nil || values["tgsrl.io/control-idempotency-key"] == "" {
		return "", "", 0, 0, false, nil
	}
	actionValue, ok := tgsrlv1.JobCommandType_value[values["tgsrl.io/control-action"]]
	if !ok {
		return "", "", 0, 0, false, fmt.Errorf("unknown control action %q", values["tgsrl.io/control-action"])
	}
	revision, err := strconv.ParseUint(values["tgsrl.io/control-backend-revision"], 10, 64)
	if err != nil {
		return "", "", 0, 0, false, err
	}
	committed, err := strconv.ParseBool(values["tgsrl.io/control-committed"])
	if err != nil {
		return "", "", 0, 0, false, err
	}
	return values["tgsrl.io/control-request-id"], values["tgsrl.io/control-idempotency-key"], tgsrlv1.JobCommandType(actionValue), revision, committed, nil
}

func NewKubernetes(interval time.Duration) *KubernetesAdapter {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &KubernetesAdapter{
		interval: interval,
		now:      time.Now,
	}
}

func (a *KubernetesAdapter) BundleObject(key string) Object {
	return bundleObjectFor(key)
}

func (a *KubernetesAdapter) Materialize(bundle *api.Bundle) ([]Object, error) {
	clone, err := cloneBundle(bundle)
	if err != nil {
		return nil, err
	}
	if err := validateSupportedKubernetesContract(clone); err != nil {
		return nil, err
	}
	objects := make([]Object, 0, 5)
	if object, err := marshalObject(bundleObjectFor(clone.Key), newBundleObject(clone)); err != nil {
		return nil, err
	} else {
		objects = append(objects, object)
	}
	if clone.RuntimeClass != nil {
		if object, err := marshalDesiredObject(metaFor(clone.RuntimeClass.TypeMeta.APIVersion, clone.RuntimeClass.TypeMeta.Kind, clone.RuntimeClass.ObjectMeta), clone.RuntimeClass); err != nil {
			return nil, err
		} else {
			objects = append(objects, object)
		}
	}
	if clone.ResourceClaim != nil {
		object, err := marshalDesiredObject(metaFor(clone.ResourceClaim.TypeMeta.APIVersion, clone.ResourceClaim.TypeMeta.Kind, clone.ResourceClaim.ObjectMeta), clone.ResourceClaim)
		if err != nil {
			return nil, err
		}
		objects = append(objects, object)
	}
	if object, err := marshalDesiredObject(metaFor(clone.Workload.TypeMeta.APIVersion, clone.Workload.TypeMeta.Kind, clone.Workload.ObjectMeta), clone.Workload); err != nil {
		return nil, err
	} else {
		objects = append(objects, object)
	}
	if object, err := marshalDesiredObject(metaFor(clone.Job.TypeMeta.APIVersion, clone.Job.TypeMeta.Kind, clone.Job.ObjectMeta), clone.Job); err != nil {
		return nil, err
	} else {
		objects = append(objects, object)
	}
	return objects, nil
}

func validateSupportedKubernetesContract(bundle *api.Bundle) error {
	if !supportedAPIVersion(bundle.Workload.TypeMeta.APIVersion, "kueue.x-k8s.io/v1beta2", "kueue.x-k8s.io/v1beta1") || bundle.Workload.TypeMeta.Kind != "Workload" {
		return fmt.Errorf("unsupported Kueue Workload contract %s %s", bundle.Workload.TypeMeta.APIVersion, bundle.Workload.TypeMeta.Kind)
	}
	if bundle.Job.TypeMeta.APIVersion != "batch/v1" || bundle.Job.TypeMeta.Kind != "Job" {
		return fmt.Errorf("unsupported Kubernetes Job contract %s %s", bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind)
	}
	if bundle.RuntimeClass != nil && (bundle.RuntimeClass.TypeMeta.APIVersion != "node.k8s.io/v1" || bundle.RuntimeClass.TypeMeta.Kind != "RuntimeClass") {
		return fmt.Errorf("unsupported Kubernetes RuntimeClass contract %s %s", bundle.RuntimeClass.TypeMeta.APIVersion, bundle.RuntimeClass.TypeMeta.Kind)
	}
	if bundle.ResourceClaim != nil && (!supportedAPIVersion(bundle.ResourceClaim.TypeMeta.APIVersion, "resource.k8s.io/v1", "resource.k8s.io/v1beta2", "resource.k8s.io/v1beta1") || bundle.ResourceClaim.TypeMeta.Kind != "ResourceClaim") {
		return fmt.Errorf("unsupported Kubernetes ResourceClaim contract %s %s", bundle.ResourceClaim.TypeMeta.APIVersion, bundle.ResourceClaim.TypeMeta.Kind)
	}
	return nil
}

func supportedAPIVersion(value string, supported ...string) bool {
	for _, candidate := range supported {
		if value == candidate {
			return true
		}
	}
	return false
}

// marshalDesiredObject protects the Kubernetes main-resource endpoint from
// server-owned metadata and observed status, even when a restored bundle
// contains values returned by an API server.
func marshalDesiredObject(meta Object, value any) (Object, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return Object{}, fmt.Errorf("marshal %s %s: %w", meta.Kind, meta.Key, err)
	}
	var object map[string]any
	if err := json.Unmarshal(payload, &object); err != nil {
		return Object{}, fmt.Errorf("sanitize %s %s: %w", meta.Kind, meta.Key, err)
	}
	metadata, _ := object["metadata"].(map[string]any)
	for _, field := range []string{"uid", "generation", "resourceVersion", "managedFields", "creationTimestamp"} {
		delete(metadata, field)
	}
	delete(metadata, "ownerReferences")
	if meta.Kind == "RuntimeClass" {
		delete(metadata, "namespace")
	}
	delete(object, "status")
	payload, err = json.Marshal(object)
	if err != nil {
		return Object{}, fmt.Errorf("marshal sanitized %s %s: %w", meta.Kind, meta.Key, err)
	}
	meta.Payload = payload
	return meta, nil
}

func (a *KubernetesAdapter) DecodeBundle(object Object) (*api.Bundle, error) {
	return decodeBundlePayload(object.Payload)
}

func (a *KubernetesAdapter) CollectionPath(object Object, defaultNamespace string) string {
	return collectionPath(object, defaultNamespace)
}

func (a *KubernetesAdapter) ObjectPath(object Object, defaultNamespace string) string {
	return objectPath(object, defaultNamespace)
}

func (a *KubernetesAdapter) Watch(ctx context.Context, reader Reader, bundle *api.Bundle) (Stream, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader is required")
	}
	clone, err := cloneBundle(bundle)
	if err != nil {
		return nil, err
	}
	return &kubernetesStream{
		ctx:      ctx,
		reader:   reader,
		adapter:  a,
		bundle:   clone,
		interval: a.interval,
	}, nil
}

func (a *KubernetesAdapter) ObserveOnce(ctx context.Context, reader Reader, bundle *api.Bundle) (*Snapshot, bool, error) {
	return a.observeOnce(ctx, reader, bundle)
}

type kubernetesStream struct {
	ctx      context.Context
	reader   Reader
	adapter  *KubernetesAdapter
	bundle   *api.Bundle
	interval time.Duration
	done     bool
}

func (s *kubernetesStream) Recv() (*Snapshot, error) {
	if s.done {
		return nil, io.EOF
	}
	snapshot, terminal, err := s.adapter.observeOnce(s.ctx, s.reader, s.bundle)
	if err != nil {
		return nil, err
	}
	if terminal {
		s.done = true
		return snapshot, nil
	}
	timer := time.NewTimer(s.interval)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case <-timer.C:
		return snapshot, nil
	}
}

func (a *KubernetesAdapter) observeOnce(ctx context.Context, reader Reader, bundle *api.Bundle) (*Snapshot, bool, error) {
	workloadObject := metaFor(defaultAPIVersion(bundle.Workload.TypeMeta.APIVersion, "kueue.x-k8s.io/v1beta1"), defaultKind(bundle.Workload.TypeMeta.Kind, "Workload"), bundle.Workload.ObjectMeta)
	workloadBody, err := reader.GetPath(ctx, objectPath(workloadObject, bundle.Namespace))
	if err != nil {
		return nil, false, err
	}
	var requestID, idempotencyKey string
	var controlAction tgsrlv1.JobCommandType
	var backendRevision uint64
	var controlCommitted bool
	if strings.TrimSpace(bundle.Key) != "" {
		bundleBody, err := reader.GetPath(ctx, objectPath(a.BundleObject(bundle.Key), bundle.Namespace))
		if err != nil {
			return nil, false, err
		}
		requestID, idempotencyKey, controlAction, backendRevision, controlCommitted, err = controlMetadataFromBundleBody(bundleBody)
		if err != nil {
			return nil, false, fmt.Errorf("decode bundle control metadata: %w", err)
		}
	}
	jobObject := metaFor(defaultAPIVersion(bundle.Job.TypeMeta.APIVersion, "batch/v1"), defaultKind(bundle.Job.TypeMeta.Kind, "Job"), bundle.Job.ObjectMeta)
	jobBody, err := reader.GetPath(ctx, objectPath(jobObject, bundle.Namespace))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			if err := confirmBundlePresent(ctx, reader, a.BundleObject(bundle.Key), bundle.Namespace); err != nil {
				return nil, false, err
			}
			now := time.Now
			if a != nil && a.now != nil {
				now = a.now
			}
			return &Snapshot{ObservedGeneration: bundle.Generation, WorkloadAdmitted: true, JobDeleted: true, Reason: "kubernetes job deleted", ObservedAt: now().UTC(), ControlRequestID: requestID, ControlIdempotencyKey: idempotencyKey, ControlAction: controlAction, ControlBackendRevision: backendRevision, ControlCommitted: controlCommitted}, true, nil
		}
		return nil, false, err
	}
	var claimBody []byte
	if bundle.ResourceClaim != nil {
		claimObject := metaFor(defaultAPIVersion(bundle.ResourceClaim.TypeMeta.APIVersion, "resource.k8s.io/v1beta1"), defaultKind(bundle.ResourceClaim.TypeMeta.Kind, "ResourceClaim"), bundle.ResourceClaim.ObjectMeta)
		claimBody, err = reader.GetPath(ctx, objectPath(claimObject, bundle.Namespace))
		if err != nil {
			return nil, false, err
		}
	}

	now := time.Now
	if a != nil && a.now != nil {
		now = a.now
	}
	snapshot := &Snapshot{ObservedGeneration: bundle.Generation, ObservedAt: now().UTC(), ControlRequestID: requestID, ControlIdempotencyKey: idempotencyKey, ControlAction: controlAction, ControlBackendRevision: backendRevision, ControlCommitted: controlCommitted}
	var workload struct {
		Metadata struct {
			Generation uint64 `json:"generation"`
		} `json:"metadata"`
		Status struct {
			Admission *struct {
				ClusterQueue string `json:"clusterQueue"`
			} `json:"admission"`
			Conditions []struct {
				Type               string `json:"type"`
				Status             string `json:"status"`
				ObservedGeneration uint64 `json:"observedGeneration"`
				Reason             string `json:"reason"`
				Message            string `json:"message"`
			} `json:"conditions"`
		} `json:"status"`
	}
	if err := json.Unmarshal(workloadBody, &workload); err != nil {
		return nil, false, fmt.Errorf("decode workload status: %w", err)
	}
	snapshot.ObservedGeneration = bundle.Generation
	for _, condition := range workload.Status.Conditions {
		if condition.Type != "Admitted" {
			continue
		}
		if condition.ObservedGeneration != 0 && condition.ObservedGeneration != workload.Metadata.Generation {
			continue
		}
		snapshot.WorkloadAdmitted = condition.Status == "True" && workload.Status.Admission != nil
		snapshot.Reason = defaultReason(snapshot.Reason, condition.Message, condition.Reason)
		break
	}

	var job struct {
		Spec struct {
			Suspend bool `json:"suspend"`
		} `json:"spec"`
		Status struct {
			Active    uint32 `json:"active"`
			Succeeded uint32 `json:"succeeded"`
			Failed    uint32 `json:"failed"`
		} `json:"status"`
	}
	if err := json.Unmarshal(jobBody, &job); err != nil {
		return nil, false, fmt.Errorf("decode job status: %w", err)
	}
	snapshot.JobActive = job.Status.Active
	snapshot.JobSucceeded = job.Status.Succeeded
	snapshot.JobFailed = job.Status.Failed
	snapshot.JobPaused = job.Spec.Suspend

	if bundle.ResourceClaim == nil {
		snapshot.ResourceClaimsAllocated = true
	} else {
		if bundle.GPUProfile == compiler.GPUProfileKubernetesDRA {
			allocated, deviceIDs, err := observeDRAAllocation(ctx, reader, bundle, claimBody)
			if err != nil {
				return nil, false, err
			}
			snapshot.ResourceClaimsAllocated = allocated
			snapshot.AllocatedDeviceIDs = deviceIDs
		} else {
			var claim struct {
				Status struct {
					Allocation json.RawMessage `json:"allocation"`
				} `json:"status"`
			}
			if err := json.Unmarshal(claimBody, &claim); err != nil {
				return nil, false, fmt.Errorf("decode resourceclaim status: %w", err)
			}
			snapshot.ResourceClaimsAllocated = len(claim.Status.Allocation) > 0 && string(claim.Status.Allocation) != "null"
		}
	}

	terminal := snapshot.JobSucceeded > 0 || snapshot.JobFailed > 0
	return snapshot, terminal, nil
}

func observeDRAAllocation(ctx context.Context, reader Reader, bundle *api.Bundle, claimBody []byte) (bool, []string, error) {
	var claim struct {
		Status struct {
			Allocation *struct {
				Devices struct {
					Results []api.DeviceRequestAllocationResult `json:"results"`
				} `json:"devices"`
			} `json:"allocation"`
		} `json:"status"`
	}
	if err := json.Unmarshal(claimBody, &claim); err != nil {
		return false, nil, fmt.Errorf("decode resourceclaim status: %w", err)
	}
	if claim.Status.Allocation == nil || len(claim.Status.Allocation.Devices.Results) == 0 {
		return false, nil, nil
	}
	expected := expectedDRADeviceIDs(bundle)
	if len(expected) == 0 {
		return false, nil, fmt.Errorf("resourceclaim has an allocation but bundle has no expected device identities")
	}
	results := claim.Status.Allocation.Devices.Results
	if len(results) != len(expected) {
		return false, nil, fmt.Errorf("resourceclaim allocated %d devices, want %d", len(results), len(expected))
	}
	for _, result := range results {
		if result.Request != "accelerator" || result.Driver != compiler.NVIDIADRADriver || result.Pool == "" || result.Device == "" {
			return false, nil, fmt.Errorf("resourceclaim returned an invalid NVIDIA DRA allocation result")
		}
	}
	claimAPI := defaultAPIVersion(bundle.ResourceClaim.APIVersion, compiler.DRAResourceClaimV1Beta1)
	sliceBody, err := reader.GetPath(ctx, "/apis/"+claimAPI+"/resourceslices")
	if err != nil {
		return false, nil, fmt.Errorf("read DRA resource slices: %w", err)
	}
	actual, err := ResolveDRAAllocationUUIDs(sliceBody, compiler.NVIDIADRADriver, results)
	if err != nil {
		return false, nil, err
	}
	if !equalStrings(actual, expected) {
		return false, nil, fmt.Errorf("resourceclaim allocated device UUIDs %v, want binding device_ids %v", actual, expected)
	}
	return true, actual, nil
}

func expectedDRADeviceIDs(bundle *api.Bundle) []string {
	if bundle == nil || len(bundle.RuntimeTargets) != 1 {
		return nil
	}
	result := append([]string(nil), bundle.RuntimeTargets[0].DeviceIDs...)
	for index := range result {
		result[index] = strings.TrimSpace(result[index])
	}
	sort.Strings(result)
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func confirmBundlePresent(ctx context.Context, reader Reader, object Object, defaultNamespace string) error {
	if _, err := reader.GetPath(ctx, objectPath(object, defaultNamespace)); err != nil {
		if errors.Is(err, ErrNotFound) {
			return fmt.Errorf("%w: %s", ErrBundleAbsent, object.Key)
		}
		return err
	}
	return nil
}

func defaultKind(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}

func defaultAPIVersion(value, fallback string) string {
	if strings.TrimSpace(value) != "" {
		return value
	}
	return fallback
}
