package statuswatch

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

type httpObserverClient struct {
	baseURL string
	client  *http.Client
}

func (c httpObserverClient) GetPath(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, bundleadapter.ErrNotFound
	}
	return io.ReadAll(resp.Body)
}

func TestKubernetesObserverMapsObservedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io/v1beta1/namespaces/test-ns/workloads/workload-a":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"metadata": map[string]any{"generation": 4},
				"status": map[string]any{
					"admission":  map[string]any{"clusterQueue": "cluster-queue"},
					"conditions": []any{map[string]any{"type": "Admitted", "status": "True", "observedGeneration": 4, "reason": "AdmittedByTest", "message": "quota reserved"}},
				},
			})
		case "/apis/batch/v1/namespaces/test-ns/jobs/job-a":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": map[string]any{"active": 1, "succeeded": 0, "failed": 0},
			})
		case "/apis/resource.k8s.io/v1beta1/namespaces/test-ns/resourceclaims/claim-a":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"status": map[string]any{"allocation": map[string]any{"devices": map[string]any{"results": []any{map[string]any{"request": "accelerator", "driver": compiler.NVIDIADRADriver, "pool": "node-a", "device": "gpu-0"}}}}},
			})
		case "/apis/resource.k8s.io/v1beta1/resourceslices":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"spec": map[string]any{"driver": compiler.NVIDIADRADriver, "pool": map[string]any{"name": "node-a", "generation": 1}, "devices": []any{map[string]any{"name": "gpu-0", "basic": map[string]any{"attributes": map[string]any{"uuid": map[string]any{"string": "GPU-aaaa"}}}}}}}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	observer, err := NewKubernetes(httpObserverClient{baseURL: server.URL, client: server.Client()}, time.Millisecond)
	if err != nil {
		t.Fatalf("new observer failed: %v", err)
	}
	stream, err := observer.Watch(context.Background(), Request{
		Bundle: &api.Bundle{
			Namespace:      "test-ns",
			Generation:     4,
			GPUProfile:     compiler.GPUProfileKubernetesDRA,
			RuntimeTargets: []api.RuntimeTarget{{DeviceIDs: []string{"GPU-aaaa"}}},
			Workload: api.Workload{
				ObjectMeta: api.ObjectMeta{Name: "workload-a", Namespace: "test-ns"},
			},
			Job: api.Job{
				ObjectMeta: api.ObjectMeta{Name: "job-a", Namespace: "test-ns"},
			},
			ResourceClaim: &api.ResourceClaim{
				ObjectMeta: api.ObjectMeta{Name: "claim-a", Namespace: "test-ns"},
			},
		},
		Bindings: []*tgsrlv1.Binding{{BindingId: "binding-a"}},
	})
	if err != nil {
		t.Fatalf("watch failed: %v", err)
	}
	snapshot, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv failed: %v", err)
	}
	if !snapshot.WorkloadAdmitted || !snapshot.ResourceClaimsAllocated || len(snapshot.AllocatedDeviceIDs) == 0 || snapshot.JobActive != 1 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if len(snapshot.AllocatedDeviceIDs) != 1 || snapshot.AllocatedDeviceIDs[0] != "GPU-aaaa" {
		t.Fatalf("allocated device IDs = %v, want GPU-aaaa", snapshot.AllocatedDeviceIDs)
	}
}

func TestKubernetesObserverRejectsDRADeviceIdentityMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io/v1/namespaces/test-ns/workloads/workload-a":
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"generation": 4}, "status": map[string]any{"admission": map[string]any{"clusterQueue": "queue"}, "conditions": []any{map[string]any{"type": "Admitted", "status": "True", "observedGeneration": 4}}}})
		case "/apis/batch/v1/namespaces/test-ns/jobs/job-a":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"active": 1}})
		case "/apis/resource.k8s.io/v1/namespaces/test-ns/resourceclaims/claim-a":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": map[string]any{"allocation": map[string]any{"devices": map[string]any{"results": []any{map[string]any{"request": "accelerator", "driver": compiler.NVIDIADRADriver, "pool": "node-a", "device": "gpu-0"}}}}}})
		case "/apis/resource.k8s.io/v1/resourceslices":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"spec": map[string]any{"driver": compiler.NVIDIADRADriver, "pool": map[string]any{"name": "node-a", "generation": 1}, "devices": []any{map[string]any{"name": "gpu-0", "attributes": map[string]any{"uuid": map[string]any{"string": "GPU-other"}}}}}}}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	observer, err := NewKubernetes(httpObserverClient{baseURL: server.URL, client: server.Client()}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	bundle := &api.Bundle{Namespace: "test-ns", Generation: 4, GPUProfile: compiler.GPUProfileKubernetesDRA, RuntimeTargets: []api.RuntimeTarget{{DeviceIDs: []string{"GPU-expected"}}}, Workload: api.Workload{TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1", Kind: "Workload"}, ObjectMeta: api.ObjectMeta{Name: "workload-a", Namespace: "test-ns"}}, Job: api.Job{ObjectMeta: api.ObjectMeta{Name: "job-a", Namespace: "test-ns"}}, ResourceClaim: &api.ResourceClaim{TypeMeta: api.TypeMeta{APIVersion: "resource.k8s.io/v1", Kind: "ResourceClaim"}, ObjectMeta: api.ObjectMeta{Name: "claim-a", Namespace: "test-ns"}}}
	stream, err := observer.Watch(context.Background(), Request{Bundle: bundle, Bindings: []*tgsrlv1.Binding{{BindingId: "binding-a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err == nil || !strings.Contains(err.Error(), "want binding device_ids") {
		t.Fatalf("Recv() error = %v, want device identity mismatch", err)
	}
}

func TestKubernetesObserverTreatsJob404AsTerminalOnlyForRegisteredBundle(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io/v1beta1/namespaces/test-ns/workloads/workload-a":
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"generation": 4}, "status": map[string]any{"admission": map[string]any{"clusterQueue": "cluster-queue"}, "conditions": []any{map[string]any{"type": "Admitted", "status": "True", "observedGeneration": 4}}}})
		case "/apis/batch/v1/namespaces/test-ns/jobs/job-a":
			http.NotFound(w, r)
		case "/apis/tgsrl.io/v1alpha1/namespaces/test-ns/jobrunbundles/bundle-a":
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"name": "bundle-a"}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	observer, err := NewKubernetes(httpObserverClient{baseURL: server.URL, client: server.Client()}, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := observer.Watch(context.Background(), Request{Bundle: &api.Bundle{Key: "test-ns/bundle-a", Namespace: "test-ns", Generation: 4, Workload: api.Workload{ObjectMeta: api.ObjectMeta{Name: "workload-a", Namespace: "test-ns"}}, Job: api.Job{ObjectMeta: api.ObjectMeta{Name: "job-a", Namespace: "test-ns"}}}, Bindings: []*tgsrlv1.Binding{{BindingId: "binding-a"}}})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	projection := Project(snapshot, false)
	if !snapshot.JobDeleted || projection.State != tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED {
		t.Fatalf("snapshot=%+v projection=%+v", snapshot, projection)
	}
}
