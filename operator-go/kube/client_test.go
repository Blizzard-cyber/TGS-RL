package kube

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

func TestClientGetAndUpsertAgainstHTTPServer(t *testing.T) {
	var (
		stored       = map[string][]byte{}
		resourceVers = map[string]string{}
		seenMethods  []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
			t.Fatalf("unexpected auth header: %q", got)
		}
		seenMethods = append(seenMethods, r.Method+" "+r.URL.Path)
		key := r.URL.Path
		switch r.Method {
		case http.MethodGet:
			body, ok := stored[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(body)
		case http.MethodPost:
			defer r.Body.Close()
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode request failed: %v", err)
			}
			meta := payload["metadata"].(map[string]any)
			name := meta["name"].(string)
			meta["resourceVersion"] = "1"
			body, _ := json.Marshal(payload)
			stored[r.URL.Path+"/"+name] = body
			resourceVers[r.URL.Path+"/"+name] = "1"
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write(body)
		case http.MethodPut:
			defer r.Body.Close()
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode request failed: %v", err)
			}
			meta := payload["metadata"].(map[string]any)
			if meta["resourceVersion"] != resourceVers[key] {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte("resourceVersion conflict"))
				return
			}
			meta["resourceVersion"] = "2"
			body, _ := json.Marshal(payload)
			stored[key] = body
			resourceVers[key] = "2"
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	client, err := NewClient(&Config{
		Host:        server.URL,
		BearerToken: "token-1",
		Namespace:   "test-ns",
		HTTPClient:  server.Client(),
	})
	if err != nil {
		t.Fatalf("new client failed: %v", err)
	}

	payload, _ := json.Marshal(map[string]any{
		"apiVersion": "tgsrl.io/v1alpha1",
		"kind":       "JobRunBundle",
		"metadata": map[string]any{
			"name":      "bundle-a",
			"namespace": "test-ns",
		},
		"spec": map[string]any{
			"bundle": map[string]any{
				"key":       "test-ns/bundle-a",
				"namespace": "test-ns",
			},
		},
	})
	created, previous, err := client.Upsert(context.Background(), bundleadapter.Object{
		APIVersion: "tgsrl.io/v1alpha1",
		Kind:       "JobRunBundle",
		Key:        "test-ns/bundle-a",
		Name:       "bundle-a",
		Namespace:  "test-ns",
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	if !created || previous != nil {
		t.Fatalf("unexpected create result: created=%v previous=%v", created, previous)
	}

	got, ok, err := client.Get(context.Background(), bundleadapter.Object{
		APIVersion: "tgsrl.io/v1alpha1",
		Kind:       "JobRunBundle",
		Key:        "test-ns/bundle-a",
		Name:       "bundle-a",
		Namespace:  "test-ns",
	})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected stored object")
	}
	if !strings.Contains(string(got.Payload), `"resourceVersion":"1"`) || !strings.Contains(string(got.Payload), `"name":"bundle-a"`) {
		t.Fatalf("unexpected payload: %s", string(got.Payload))
	}
	_, previous, err = client.Upsert(context.Background(), bundleadapter.Object{
		APIVersion: "tgsrl.io/v1alpha1",
		Kind:       "JobRunBundle",
		Key:        "test-ns/bundle-a",
		Name:       "bundle-a",
		Namespace:  "test-ns",
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if previous == nil || previous.Version != "1" {
		t.Fatalf("expected previous resourceVersion 1, got %+v", previous)
	}
	wantCalls := []string{
		"GET /apis/tgsrl.io/v1alpha1/namespaces/test-ns/jobrunbundles/bundle-a",
		"POST /apis/tgsrl.io/v1alpha1/namespaces/test-ns/jobrunbundles",
		"GET /apis/tgsrl.io/v1alpha1/namespaces/test-ns/jobrunbundles/bundle-a",
		"GET /apis/tgsrl.io/v1alpha1/namespaces/test-ns/jobrunbundles/bundle-a",
		"PUT /apis/tgsrl.io/v1alpha1/namespaces/test-ns/jobrunbundles/bundle-a",
	}
	if strings.Join(seenMethods, "|") != strings.Join(wantCalls, "|") {
		t.Fatalf("unexpected verb/path sequence: %v", seenMethods)
	}
}

func TestClientUpsertRetriesCreateConflictUntilReadable(t *testing.T) {
	var (
		stored        map[string]any
		getCalls      int
		postConflicts int
		putCalls      int
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			getCalls++
			if stored == nil {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(stored)
		case http.MethodPost:
			postConflicts++
			if postConflicts == 1 {
				w.WriteHeader(http.StatusConflict)
				_, _ = w.Write([]byte("create conflict"))
				stored = map[string]any{
					"apiVersion": "tgsrl.io/v1alpha1",
					"kind":       "JobRunBundle",
					"metadata": map[string]any{
						"name":            "bundle-a",
						"namespace":       "test-ns",
						"resourceVersion": "9",
					},
					"spec": map[string]any{"bundle": map[string]any{"key": "test-ns/bundle-a", "namespace": "test-ns"}},
				}
				return
			}
			t.Fatalf("unexpected extra POST after conflict")
		case http.MethodPut:
			putCalls++
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			payload["metadata"].(map[string]any)["resourceVersion"] = "10"
			stored = payload
			_ = json.NewEncoder(w).Encode(stored)
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test-ns", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"apiVersion": "tgsrl.io/v1alpha1",
		"kind":       "JobRunBundle",
		"metadata": map[string]any{
			"name":      "bundle-a",
			"namespace": "test-ns",
		},
		"spec": map[string]any{"bundle": map[string]any{"key": "test-ns/bundle-a", "namespace": "test-ns"}},
	})
	created, previous, err := client.Upsert(context.Background(), bundleadapter.Object{
		APIVersion: "tgsrl.io/v1alpha1",
		Kind:       "JobRunBundle",
		Key:        "test-ns/bundle-a",
		Name:       "bundle-a",
		Namespace:  "test-ns",
		Payload:    payload,
	})
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if created || previous == nil || previous.Version != "9" {
		t.Fatalf("create conflict retry result = created:%v previous:%+v", created, previous)
	}
	if getCalls < 2 || postConflicts != 1 || putCalls != 1 {
		t.Fatalf("getCalls=%d postConflicts=%d putCalls=%d", getCalls, postConflicts, putCalls)
	}
}

func TestClientUpsertFailsAfterUpdateConflictRetryBudget(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"apiVersion": "tgsrl.io/v1alpha1",
				"kind":       "JobRunBundle",
				"metadata": map[string]any{
					"name":            "bundle-a",
					"namespace":       "test-ns",
					"resourceVersion": "7",
				},
				"spec": map[string]any{"bundle": map[string]any{"key": "test-ns/bundle-a", "namespace": "test-ns"}},
			})
		case http.MethodPut:
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte("resourceVersion conflict"))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test-ns", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"apiVersion": "tgsrl.io/v1alpha1",
		"kind":       "JobRunBundle",
		"metadata": map[string]any{
			"name":      "bundle-a",
			"namespace": "test-ns",
		},
		"spec": map[string]any{"bundle": map[string]any{"key": "test-ns/bundle-a", "namespace": "test-ns"}},
	})
	_, _, err = client.Upsert(context.Background(), bundleadapter.Object{
		APIVersion: "tgsrl.io/v1alpha1",
		Kind:       "JobRunBundle",
		Key:        "test-ns/bundle-a",
		Name:       "bundle-a",
		Namespace:  "test-ns",
		Payload:    payload,
	})
	if err == nil || !strings.Contains(err.Error(), "retry budget exhausted") {
		t.Fatalf("Upsert() error = %v, want retry budget exhausted", err)
	}
}

func TestClientControlJobUsesSuspendPatchAndDeleteReadback(t *testing.T) {
	var (
		suspended     bool
		deleted       bool
		methods       []string
		patchVersion  string
		deleteVersion string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		switch r.Method {
		case http.MethodPatch:
			var patch struct {
				Metadata struct {
					ResourceVersion string `json:"resourceVersion"`
				} `json:"metadata"`
				Spec struct {
					Suspend bool `json:"suspend"`
				} `json:"spec"`
			}
			if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
				t.Fatal(err)
			}
			patchVersion = patch.Metadata.ResourceVersion
			suspended = patch.Spec.Suspend
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			var options struct {
				Preconditions struct {
					ResourceVersion string `json:"resourceVersion"`
				} `json:"preconditions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
				t.Fatal(err)
			}
			deleteVersion = options.Preconditions.ResourceVersion
			deleted = true
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if deleted {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{"suspend": suspended}, "status": map[string]any{"active": 1}})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	job := bundleadapter.Object{APIVersion: "batch/v1", Kind: "Job", Key: "test/job-1", Name: "job-1", Namespace: "test"}
	paused, err := client.ControlJob(context.Background(), job, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE, "7")
	if err != nil || !paused.Paused {
		t.Fatalf("pause readback = %+v, err = %v", paused, err)
	}
	resumed, err := client.ControlJob(context.Background(), job, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME, "7")
	if err != nil || resumed.Paused {
		t.Fatalf("resume readback = %+v, err = %v", resumed, err)
	}
	terminated, err := client.ControlJob(context.Background(), job, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE, "7")
	if err != nil || !terminated.Deleting {
		t.Fatalf("terminate readback = %+v, err = %v", terminated, err)
	}
	want := []string{"PATCH /apis/batch/v1/namespaces/test/jobs/job-1", "GET /apis/batch/v1/namespaces/test/jobs/job-1", "PATCH /apis/batch/v1/namespaces/test/jobs/job-1", "GET /apis/batch/v1/namespaces/test/jobs/job-1", "DELETE /apis/batch/v1/namespaces/test/jobs/job-1"}
	if strings.Join(methods, "|") != strings.Join(want, "|") {
		t.Fatalf("methods = %v, want %v", methods, want)
	}
	if patchVersion != "7" || deleteVersion != "7" {
		t.Fatalf("resourceVersion preconditions = patch %q delete %q", patchVersion, deleteVersion)
	}
}

func TestClientControlJobAcceptsForegroundDeletionReadback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			w.WriteHeader(http.StatusAccepted)
		case http.MethodGet:
			_ = json.NewEncoder(w).Encode(map[string]any{"metadata": map[string]any{"deletionTimestamp": "2026-08-28T00:00:00Z"}})
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	readback, err := client.ControlJob(context.Background(), bundleadapter.Object{APIVersion: "batch/v1", Kind: "Job", Key: "test/job-1", Name: "job-1", Namespace: "test"}, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP, "7")
	if err != nil || !readback.Deleting || readback.Deleted {
		t.Fatalf("readback = %+v, err = %v", readback, err)
	}
}

func TestClientListBundlesDecodesCollectionItems(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/apis/tgsrl.io/v1alpha1/namespaces/test/jobrunbundles" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []any{map[string]any{
				"apiVersion": "tgsrl.io/v1alpha1",
				"kind":       "JobRunBundle",
				"metadata": map[string]any{
					"name": "bundle-1", "namespace": "test",
					"generation": 4, "resourceVersion": "12",
				},
				"spec": map[string]any{"bundle": map[string]any{
					"key": "test/bundle-1", "namespace": "test", "sourceRunId": "run-1",
				}},
			}},
		})
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := client.ListBundles(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 || objects[0].Key != "test/bundle-1" || objects[0].Version != "12" {
		t.Fatalf("objects = %+v", objects)
	}
}

func TestClientEnsureRuntimeClassCreatesOnceAndVerifiesExistingHandler(t *testing.T) {
	var (
		stored      []byte
		resourceVer = "3"
		methods     []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		switch r.Method {
		case http.MethodGet:
			if stored == nil {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(stored)
		case http.MethodPost:
			defer r.Body.Close()
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			payload["metadata"].(map[string]any)["resourceVersion"] = resourceVer
			stored, _ = json.Marshal(payload)
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write(stored)
		case http.MethodPut:
			t.Fatal("RuntimeClass must not be updated via PUT")
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"apiVersion": "node.k8s.io/v1",
		"kind":       "RuntimeClass",
		"metadata": map[string]any{
			"name": "runtime-a",
		},
		"handler":    "kata-qemu",
		"overhead":   map[string]any{"podFixed": map[string]any{"cpu": "1"}},
		"scheduling": map[string]any{"nodeSelector": map[string]any{"managed": "true"}},
	})
	object := bundleadapter.Object{
		APIVersion: "node.k8s.io/v1",
		Kind:       "RuntimeClass",
		Key:        "/runtime-a",
		Name:       "runtime-a",
		Payload:    payload,
	}
	created, previous, err := client.EnsureRuntimeClass(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	if !created || previous != nil {
		t.Fatalf("create result = created:%v previous:%v", created, previous)
	}
	created, previous, err = client.EnsureRuntimeClass(context.Background(), object)
	if err != nil {
		t.Fatal(err)
	}
	if created || previous == nil || previous.Version != resourceVer {
		t.Fatalf("verify result = created:%v previous:%+v", created, previous)
	}
	if strings.Join(methods, "|") != strings.Join([]string{
		"GET /apis/node.k8s.io/v1/runtimeclasses/runtime-a",
		"POST /apis/node.k8s.io/v1/runtimeclasses",
		"GET /apis/node.k8s.io/v1/runtimeclasses/runtime-a",
	}, "|") {
		t.Fatalf("methods = %v", methods)
	}
}

func TestClientEnsureRuntimeClassRejectsHandlerConflictWithoutOverwrite(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method %s", r.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "node.k8s.io/v1",
			"kind":       "RuntimeClass",
			"metadata": map[string]any{
				"name":            "runtime-a",
				"resourceVersion": "4",
			},
			"handler":    "nvidia",
			"overhead":   map[string]any{"podFixed": map[string]any{"memory": "1Gi"}},
			"scheduling": map[string]any{"nodeSelector": map[string]any{"managed": "true"}},
		})
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{
		"apiVersion": "node.k8s.io/v1",
		"kind":       "RuntimeClass",
		"metadata": map[string]any{
			"name": "runtime-a",
		},
		"handler": "kata-qemu",
	})
	_, _, err = client.EnsureRuntimeClass(context.Background(), bundleadapter.Object{
		APIVersion: "node.k8s.io/v1",
		Kind:       "RuntimeClass",
		Key:        "/runtime-a",
		Name:       "runtime-a",
		Payload:    payload,
	})
	if err == nil || !strings.Contains(err.Error(), "handler conflict") {
		t.Fatalf("EnsureRuntimeClass() error = %v, want handler conflict", err)
	}
	if strings.Join(methods, "|") != "GET /apis/node.k8s.io/v1/runtimeclasses/runtime-a" {
		t.Fatalf("methods = %v, want single GET", methods)
	}
}

func TestClientDeleteUsesForegroundDeletion(t *testing.T) {
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		if r.Method != http.MethodDelete {
			t.Fatalf("unexpected method %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := client.Delete(context.Background(), bundleadapter.Object{APIVersion: "batch/v1", Kind: "Job", Key: "test/job-1", Name: "job-1", Namespace: "test"})
	if err != nil || !deleted {
		t.Fatalf("Delete() = %v, %v", deleted, err)
	}
	if strings.Join(methods, "|") != "DELETE /apis/batch/v1/namespaces/test/jobs/job-1" {
		t.Fatalf("methods = %v", methods)
	}
}

func TestClientDiscoverCapabilitiesFallsClosedWhenEndpointAndProbeMissing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.DiscoverCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(capabilities.GPUProfiles) != 1 || !capabilities.GPUProfiles["none"] {
		t.Fatalf("capabilities = %+v, want fail-closed none-only profile set", capabilities)
	}
}

func TestClientDiscoverCapabilitiesRequiresNVIDIADRAUUIDInventory(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1beta2", "v1beta2", "v1beta1"))
		case "/apis/resource.k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1beta1", "v1beta1"))
		case "/apis/node.k8s.io/v1/runtimeclasses":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case "/apis/resource.k8s.io/v1beta1/deviceclasses":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": compiler.NVIDIADRAFullGPUDeviceClass}}}})
		case "/apis/resource.k8s.io/v1beta1/resourceslices":
			_ = json.NewEncoder(w).Encode(nvidiaDRAResourceSliceList(true, "GPU-aaaa"))
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.DiscoverCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GPUProfiles["none"] || !capabilities.GPUProfiles["kubernetes-dra"] {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	if capabilities.DRADevices["GPU-aaaa"].DeviceClass != compiler.NVIDIADRAFullGPUDeviceClass {
		t.Fatalf("DRA devices = %+v, want typed GPU-aaaa", capabilities.DRADevices)
	}
	if strings.Join(paths, "|") != strings.Join([]string{
		"/apis/kueue.x-k8s.io",
		"/apis/resource.k8s.io",
		"/apis/node.k8s.io/v1/runtimeclasses",
		"/api/v1/nodes",
		"/apis/resource.k8s.io/v1beta1/deviceclasses",
		"/apis/resource.k8s.io/v1beta1/resourceslices",
	}, "|") {
		t.Fatalf("paths = %v", paths)
	}
}

func TestClientDiscoverCapabilitiesRejectsDRAClassWithoutUUIDInventory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1", "v1"))
		case "/apis/resource.k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1", "v1"))
		case "/apis/node.k8s.io/v1/runtimeclasses", "/api/v1/nodes", "/apis/resource.k8s.io/v1/resourceslices":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
		case "/apis/resource.k8s.io/v1/deviceclasses":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"metadata": map[string]any{"name": compiler.NVIDIADRAFullGPUDeviceClass}}}})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.DiscoverCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.GPUProfiles[compiler.GPUProfileKubernetesDRA] || len(capabilities.DRADevices) != 0 {
		t.Fatalf("capabilities = %+v, want DRA disabled without UUID inventory", capabilities)
	}
}

func TestClientDiscoverCapabilitiesDoesNotClaimExactPlacementForCountOnlyGPUAPIs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1beta2", "v1beta2"))
		case "/apis/resource.k8s.io", "/apis/node.k8s.io/v1/runtimeclasses", "/apis/resource.k8s.io/v1/resourceslices":
			http.NotFound(w, r)
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"status": map[string]any{"allocatable": map[string]any{"nvidia.com/gpu": "2", "volcano.sh/gpu": "2"}}}}})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.DiscoverCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GPUProfiles[compiler.GPUProfileNVIDIADevicePlugin] || !capabilities.GPUProfiles[compiler.GPUProfileVolcanoHAMI] {
		t.Fatalf("GPU profiles = %+v, want discovered count-only profiles", capabilities.GPUProfiles)
	}
	if capabilities.ExactDevicePlacement[compiler.GPUProfileNVIDIADevicePlugin] || capabilities.ExactDevicePlacement[compiler.GPUProfileVolcanoHAMI] {
		t.Fatalf("exact placement = %+v, count-only profiles must not claim UUID enforcement", capabilities.ExactDevicePlacement)
	}
}

func TestClientDiscoverCapabilitiesUsesHAMINVIDIAInventoryForExactPlacement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1beta2", "v1beta2"))
		case "/apis/resource.k8s.io", "/apis/node.k8s.io/v1/runtimeclasses":
			http.NotFound(w, r)
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []any{map[string]any{
					"metadata": map[string]any{
						"name": "a10-node",
						"annotations": map[string]any{
							compiler.HAMINVIDIARegisterAnnotation: `[{"id":"GPU-a10","count":10,"devmem":23028,"devcore":100,"type":"NVIDIA-A10","numa":0,"health":true,"index":0,"mode":"hami-core"}]`,
						},
					},
					"status": map[string]any{"allocatable": map[string]any{"nvidia.com/gpu": "10"}},
				}},
			})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.DiscoverCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GPUProfiles[compiler.GPUProfileHAMIVGPU] || !capabilities.ExactDevicePlacement[compiler.GPUProfileHAMIVGPU] {
		t.Fatalf("HAMi capabilities = %+v", capabilities)
	}
	device := capabilities.HAMIDevices["GPU-a10"]
	if device.Node != "a10-node" || device.Model != "NVIDIA-A10" || device.Mode != "hami-core" || device.SplitCount != 10 || device.CorePercent != 100 || device.MemoryBytes != 23028<<20 || !device.Healthy {
		t.Fatalf("HAMi device = %+v", device)
	}
}

func TestParseHAMINVIDIADevicesRejectsDuplicateAndInvalidRows(t *testing.T) {
	if _, err := parseHAMINVIDIADevices("node-a", "GPU-a,10,23028,100,NVIDIA-A10,0,true:GPU-a,10,23028,100,NVIDIA-A10,0,true:"); err == nil || !strings.Contains(err.Error(), "duplicates UUID") {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := parseHAMINVIDIADevices("node-a", "GPU-a,0,23028,100,NVIDIA-A10,0,true:"); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("invalid capacity error = %v", err)
	}
	if _, err := parseHAMINVIDIADevices("node-a", "MIG-a,10,23028,100,NVIDIA-A10,0,true:"); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("non-physical UUID error = %v", err)
	}
	if _, err := parseHAMINVIDIADevices("node-a", "GPU-a,10,23028,101,NVIDIA-A10,0,true:"); err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("invalid core limit error = %v", err)
	}
}

func TestClientIgnoresStaleHAMINVIDIAInventoryWithoutAllocatableResource(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1beta2", "v1beta2"))
		case "/apis/resource.k8s.io", "/apis/node.k8s.io/v1/runtimeclasses":
			http.NotFound(w, r)
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []any{map[string]any{
					"metadata": map[string]any{
						"name": "stale-node",
						"annotations": map[string]any{
							compiler.HAMINVIDIARegisterAnnotation: `[{"id":"GPU-stale","count":10,"devmem":23028,"devcore":100,"type":"NVIDIA-A10","health":true,"mode":"hami-core"}]`,
						},
					},
					"status": map[string]any{"allocatable": map[string]any{"nvidia.com/gpu": "0"}},
				}},
			})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.DiscoverCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.GPUProfiles[compiler.GPUProfileHAMIVGPU] || len(capabilities.HAMIDevices) != 0 {
		t.Fatalf("stale HAMi inventory was advertised: %+v", capabilities)
	}
}

func TestClientRejectsHAMINVIDIAUUIDPublishedByMultipleNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1beta2", "v1beta2"))
		case "/apis/resource.k8s.io", "/apis/node.k8s.io/v1/runtimeclasses":
			http.NotFound(w, r)
		case "/api/v1/nodes":
			registration := `[{"id":"GPU-shared","count":10,"devmem":23028,"devcore":100,"type":"NVIDIA-A10","health":true,"mode":"hami-core"}]`
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []any{
					map[string]any{
						"metadata": map[string]any{"name": "node-a", "annotations": map[string]any{compiler.HAMINVIDIARegisterAnnotation: registration}},
						"status":   map[string]any{"allocatable": map[string]any{"nvidia.com/gpu": "10"}},
					},
					map[string]any{
						"metadata": map[string]any{"name": "node-b", "annotations": map[string]any{compiler.HAMINVIDIARegisterAnnotation: registration}},
						"status":   map[string]any{"allocatable": map[string]any{"nvidia.com/gpu": "10"}},
					},
				},
			})
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DiscoverCapabilities(context.Background()); err == nil || !strings.Contains(err.Error(), "published more than once") {
		t.Fatalf("duplicate HAMi UUID error = %v", err)
	}
}

func TestClientDiscoverCapabilitiesProbesNodeAndRuntimeClassSurfaces(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/apis/kueue.x-k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1beta2", "v1beta2", "v1beta1"))
		case "/apis/resource.k8s.io":
			_ = json.NewEncoder(w).Encode(apiGroupDiscovery("v1", "v1"))
		case "/apis/node.k8s.io/v1/runtimeclasses":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []any{
					map[string]any{"metadata": map[string]any{"name": "kata-gpu"}, "handler": "kata-qemu"},
				},
			})
		case "/api/v1/nodes":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []any{
					map[string]any{"status": map[string]any{
						"allocatable": map[string]any{"nvidia.com/gpu": "2", "volcano.sh/gpu": "1"},
					}},
				},
			})
		case "/apis/resource.k8s.io/v1/deviceclasses":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"items": []any{
					map[string]any{"metadata": map[string]any{"name": compiler.NVIDIADRAFullGPUDeviceClass}},
				},
			})
		case "/apis/resource.k8s.io/v1/resourceslices":
			_ = json.NewEncoder(w).Encode(nvidiaDRAResourceSliceList(false, "GPU-bbbb"))
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := NewClient(&Config{Host: server.URL, Namespace: "test", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := client.DiscoverCapabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !capabilities.GPUProfiles["none"] || !capabilities.GPUProfiles["nvidia-device-plugin"] || !capabilities.GPUProfiles["volcano-hami"] || !capabilities.GPUProfiles["kubernetes-dra"] {
		t.Fatalf("capabilities = %+v", capabilities)
	}
	if capabilities.RuntimeClasses["kata-gpu"] != "kata-qemu" {
		t.Fatalf("runtime classes = %+v", capabilities.RuntimeClasses)
	}
	if capabilities.KubernetesAPIs.KueueWorkload != compiler.KueueWorkloadV1Beta2 || capabilities.KubernetesAPIs.DRAResourceClaim != compiler.DRAResourceClaimV1 {
		t.Fatalf("API versions = %+v", capabilities.KubernetesAPIs)
	}
	if capabilities.DRADevices["GPU-bbbb"].DeviceClass != compiler.NVIDIADRAFullGPUDeviceClass {
		t.Fatalf("DRA devices = %+v, want typed GPU-bbbb", capabilities.DRADevices)
	}
}

func nvidiaDRAResourceSliceList(legacy bool, deviceIDs ...string) map[string]any {
	devices := make([]any, 0, len(deviceIDs))
	for index, deviceID := range deviceIDs {
		attributes := map[string]any{"uuid": map[string]any{"string": deviceID}, "type": map[string]any{"string": "gpu"}}
		device := map[string]any{"name": fmt.Sprintf("gpu-%d", index), "attributes": attributes}
		if legacy {
			device = map[string]any{"name": fmt.Sprintf("gpu-%d", index), "basic": map[string]any{"attributes": attributes}}
		}
		devices = append(devices, device)
	}
	return map[string]any{"items": []any{map[string]any{"spec": map[string]any{"driver": compiler.NVIDIADRADriver, "pool": map[string]any{"name": "node-a", "generation": 1}, "devices": devices}}}}
}

func apiGroupDiscovery(preferred string, served ...string) map[string]any {
	versions := make([]any, 0, len(served))
	for _, version := range served {
		versions = append(versions, map[string]any{"version": version})
	}
	return map[string]any{
		"preferredVersion": map[string]any{"version": preferred},
		"versions":         versions,
	}
}

func TestLoadKubeconfigParsesServerTokenAndNamespace(t *testing.T) {
	dir := t.TempDir()
	certPEM, serverURL := newTLSServerConfig(t)
	kubeconfigPath := filepath.Join(dir, "config")
	kubeconfig := strings.Join([]string{
		"apiVersion: v1",
		"kind: Config",
		"clusters:",
		"- name: test",
		"  cluster:",
		"    server: " + serverURL,
		"    certificate-authority-data: " + base64.StdEncoding.EncodeToString(certPEM),
		"users:",
		"- name: test",
		"  user:",
		"    token: token-2",
		"contexts:",
		"- name: test",
		"  context:",
		"    namespace: ns-a",
		"current-context: test",
	}, "\n")
	if err := os.WriteFile(kubeconfigPath, []byte(kubeconfig), 0o644); err != nil {
		t.Fatalf("write kubeconfig failed: %v", err)
	}

	config, err := LoadConfig(kubeconfigPath, "")
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if config.Host != serverURL || config.BearerToken != "token-2" || config.Namespace != "ns-a" {
		t.Fatalf("unexpected config: %+v", config)
	}
}

func newTLSServerConfig(t *testing.T) ([]byte, string) {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	cert := server.Certificate()
	if cert == nil {
		t.Fatalf("missing server certificate")
	}
	return pemEncodeCert(t, cert.Raw), server.URL
}

func pemEncodeCert(t *testing.T, raw []byte) []byte {
	t.Helper()
	block := base64.StdEncoding.EncodeToString(raw)
	decoded, err := base64.StdEncoding.DecodeString(block)
	if err != nil {
		t.Fatalf("decode cert failed: %v", err)
	}
	return append([]byte("-----BEGIN CERTIFICATE-----\n"), append([]byte(base64.StdEncoding.EncodeToString(decoded)), []byte("\n-----END CERTIFICATE-----\n")...)...)
}
