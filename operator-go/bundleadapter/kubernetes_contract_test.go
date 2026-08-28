package bundleadapter

import (
	"encoding/json"
	"testing"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
)

func TestMaterializeSanitizesKubernetesDesiredObjects(t *testing.T) {
	bundle := &api.Bundle{
		Key: "test/bundle-a", Namespace: "test", Generation: 7,
		Workload:     api.Workload{TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"}, ObjectMeta: api.ObjectMeta{Name: "workload-a", Namespace: "test", UID: "server-uid", Generation: 9}, Spec: api.WorkloadSpec{QueueName: "default", PodSets: []api.PodSet{{Name: "main", Count: 1, Template: testPodTemplate()}}}, Status: api.WorkloadStatus{Admitted: true}},
		Job:          api.Job{TypeMeta: api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: api.ObjectMeta{Name: "job-a", Namespace: "test", UID: "server-uid", Generation: 9}, Spec: api.JobSpec{Parallelism: 1, Completions: 1, Template: testPodTemplate()}, Status: api.JobStatus{Active: 1}},
		RuntimeClass: &api.RuntimeClass{TypeMeta: api.TypeMeta{APIVersion: "node.k8s.io/v1", Kind: "RuntimeClass"}, ObjectMeta: api.ObjectMeta{Name: "runtime-a", Namespace: "test", UID: "server-uid", Generation: 9}, Handler: "runc"},
	}
	objects, err := NewKubernetes(0).Materialize(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 4 {
		t.Fatalf("materialized objects = %d, want bundle, runtime class, workload, job", len(objects))
	}
	for _, object := range objects[1:] {
		var payload map[string]any
		if err := json.Unmarshal(object.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		metadata, _ := payload["metadata"].(map[string]any)
		for _, field := range []string{"uid", "generation", "resourceVersion", "ownerReferences"} {
			if _, ok := metadata[field]; ok {
				t.Fatalf("%s desired payload contains metadata.%s: %s", object.Kind, field, object.Payload)
			}
		}
		if _, ok := payload["status"]; ok {
			t.Fatalf("%s desired payload contains status: %s", object.Kind, object.Payload)
		}
		if object.Kind == "RuntimeClass" {
			if _, ok := metadata["namespace"]; ok {
				t.Fatalf("RuntimeClass desired payload contains namespace: %s", object.Payload)
			}
		}
	}
}

func TestMaterializeSkipsRuntimeClassWhenBundleOnlyReferencesPreconfiguredName(t *testing.T) {
	bundle := &api.Bundle{
		Key: "test/bundle-b", Namespace: "test", Generation: 8,
		Workload: api.Workload{TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"}, ObjectMeta: api.ObjectMeta{Name: "workload-b", Namespace: "test"}, Spec: api.WorkloadSpec{QueueName: "default", PodSets: []api.PodSet{{Name: "main", Count: 1, Template: testPodTemplate()}}}},
		Job:      api.Job{TypeMeta: api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}, ObjectMeta: api.ObjectMeta{Name: "job-b", Namespace: "test"}, Spec: api.JobSpec{Parallelism: 1, Completions: 1, Template: api.PodTemplateSpec{Spec: api.PodSpec{RuntimeClassName: "kata-preconfigured", Containers: []api.Container{{Name: "main", Image: "example.invalid/image@sha256:abc"}}, RestartPolicy: "Never"}}}},
	}
	objects, err := NewKubernetes(0).Materialize(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 3 {
		t.Fatalf("materialized objects = %d, want bundle, workload, job", len(objects))
	}
	for _, object := range objects {
		if object.Kind == "RuntimeClass" {
			t.Fatalf("unexpected runtime class materialization for preconfigured reference: %+v", object)
		}
	}
}

func testPodTemplate() api.PodTemplateSpec {
	return api.PodTemplateSpec{Spec: api.PodSpec{Containers: []api.Container{{Name: "main", Image: "example.invalid/image@sha256:abc"}}, RestartPolicy: "Never"}}
}
