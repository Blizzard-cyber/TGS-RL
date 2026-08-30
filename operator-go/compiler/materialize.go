package compiler

import (
	"fmt"
	"math"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
)

func buildBundle(input *normalizedInput) (*api.Bundle, error) {
	labels := buildLabels(input)
	annotations := buildAnnotations(input)
	workloadName := buildObjectName("workload", input)
	labels["kueue.x-k8s.io/queue-name"] = queueName(input.Run)
	podLabels := api.CloneMap(labels)
	podAnnotations := api.CloneMap(annotations)

	template := api.PodTemplateSpec{
		ObjectMeta: api.ObjectMeta{
			Labels:      podLabels,
			Annotations: podAnnotations,
		},
		Spec: api.PodSpec{
			RuntimeClassName: input.Runtime.RuntimeClass.Name,
			NodeSelector:     buildNodeSelector(input),
			Containers: []api.Container{
				{
					Name:      "main",
					Image:     primaryImage(input.Manifest),
					Command:   append([]string(nil), input.Run.GetRuntime().GetCommand()...),
					Args:      append([]string(nil), input.Run.GetRuntime().GetArgs()...),
					Env:       buildEnv(input),
					Resources: buildResources(input),
				},
			},
			RestartPolicy: "Never",
		},
	}

	workload := api.Workload{
		TypeMeta: api.TypeMeta{APIVersion: "kueue.x-k8s.io/v1beta1", Kind: "Workload"},
		ObjectMeta: api.ObjectMeta{
			Name:        workloadName,
			Namespace:   input.Namespace,
			Labels:      api.CloneMap(labels),
			Annotations: api.CloneMap(annotations),
		},
		Spec: api.WorkloadSpec{
			QueueName: queueName(input.Run),
			Priority:  input.priority,
			PodSets: []api.PodSet{{
				Name:     "main",
				Count:    1,
				Template: template,
			}},
		},
	}

	job := api.Job{
		TypeMeta: api.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: api.ObjectMeta{
			Name:        buildObjectName("job", input),
			Namespace:   input.Namespace,
			Labels:      api.CloneMap(labels),
			Annotations: api.CloneMap(annotations),
		},
		Spec: api.JobSpec{
			Parallelism: 1,
			Completions: 1,
			Suspend:     true,
			Template:    template,
		},
	}
	job.ObjectMeta.Labels["kueue.x-k8s.io/prebuilt-workload-name"] = workloadName

	var runtimeClass *api.RuntimeClass
	if input.Runtime.RuntimeClass.Create {
		runtimeClass = &api.RuntimeClass{
			TypeMeta: api.TypeMeta{APIVersion: "node.k8s.io/v1", Kind: "RuntimeClass"},
			ObjectMeta: api.ObjectMeta{
				Name:        input.Runtime.RuntimeClass.Name,
				Labels:      api.CloneMap(labels),
				Annotations: api.CloneMap(annotations),
			},
			Handler: input.Runtime.RuntimeClass.Handler,
		}
	}

	var resourceClaim *api.ResourceClaim
	if requiresResourceClaim(input.GPUProfile, input.resourcesPerUnit.GetAcceleratorUnits()) {
		resourceClaim = &api.ResourceClaim{
			TypeMeta: api.TypeMeta{APIVersion: "resource.k8s.io/v1beta1", Kind: "ResourceClaim"},
			ObjectMeta: api.ObjectMeta{
				Name:        buildObjectName("resourceclaim", input),
				Namespace:   input.Namespace,
				Labels:      api.CloneMap(labels),
				Annotations: api.CloneMap(annotations),
			},
			Spec: api.ResourceClaimSpec{
				Devices: api.DeviceClaim{Requests: []api.DeviceRequest{{
					Name:            "accelerator",
					DeviceClassName: deviceClass(input.GPUProfile),
					AllocationMode:  "ExactCount",
					Count:           int64(math.Ceil(input.resourcesPerUnit.GetAcceleratorUnits())),
				}}},
				Count: uint32(math.Ceil(input.resourcesPerUnit.GetAcceleratorUnits())),
			},
		}
		job.Spec.Template.Spec.ResourceClaims = []api.PodResourceClaim{{
			Name:              "accelerator",
			ResourceClaimName: resourceClaim.ObjectMeta.Name,
		}}
		job.Spec.Template.Spec.Containers[0].Resources.Claims = []api.ResourceClaimReference{{
			Name:    "accelerator",
			Request: "accelerator",
		}}
	}

	return &api.Bundle{
		Key:            bundleKey(input),
		Namespace:      input.Namespace,
		Generation:     input.Generation,
		GPUProfile:     input.GPUProfile,
		SourceRunID:    input.Run.GetRunId(),
		SourceJobID:    input.Run.GetJobId(),
		SourceTraceID:  input.Run.GetTraceId(),
		RuntimeTargets: buildRuntimeTargets(input.Plan, input.Generation),
		Workload:       workload,
		Job:            job,
		RuntimeClass:   runtimeClass,
		ResourceClaim:  resourceClaim,
		Admission: api.AdmissionSpec{
			Queue:           queueName(input.Run),
			QuotaGroup:      quotaGroup(input.Run),
			Requests:        api.CloneResourceList(job.Spec.Template.Spec.Containers[0].Resources.Requests),
			AllowPreemption: preemptionAllowed(input.Run),
		},
		AdmissionStatus: api.AdmissionStatus{
			Phase:  "pending-admission",
			Reason: "waiting-for-backend-admission",
		},
		ControllerStatus: api.ControllerStatus{
			ObservedGeneration: input.Generation,
			AppliedGeneration:  0,
			Phase:              "compiled",
			Reason:             "bundle-validated",
			Idempotent:         true,
		},
		StatusProjection: api.StatusProjection{
			RunState: input.Run.GetRunState().String(),
			JobState: input.Run.GetState().String(),
			Reason:   statusReason(input.Run),
		},
	}, nil
}

func validateBundle(bundle *api.Bundle) error {
	if bundle.Key == "" {
		return fmt.Errorf("bundle key is required")
	}
	if bundle.Workload.ObjectMeta.Name == "" || bundle.Job.ObjectMeta.Name == "" {
		return fmt.Errorf("bundle objects must all be named")
	}
	if bundle.Workload.ObjectMeta.Namespace != bundle.Namespace || bundle.Job.ObjectMeta.Namespace != bundle.Namespace {
		return fmt.Errorf("bundle namespace mismatch")
	}
	if bundle.Workload.ObjectMeta.UID != "" || bundle.Workload.ObjectMeta.Generation != 0 || bundle.Job.ObjectMeta.UID != "" || bundle.Job.ObjectMeta.Generation != 0 {
		return fmt.Errorf("kubernetes server-managed metadata must be empty")
	}
	if len(bundle.Workload.ObjectMeta.OwnerReferences) != 0 {
		return fmt.Errorf("workload must not self-reference ownership")
	}
	if bundle.RuntimeClass != nil {
		if bundle.RuntimeClass.ObjectMeta.Name == "" {
			return fmt.Errorf("runtime class must be named when present")
		}
		if err := validateRuntimeClassName(bundle.RuntimeClass.ObjectMeta.Name); err != nil {
			return err
		}
		if err := validateRuntimeClassHandler(bundle.RuntimeClass.Handler); err != nil {
			return err
		}
		if bundle.RuntimeClass.ObjectMeta.Namespace != "" || len(bundle.RuntimeClass.ObjectMeta.OwnerReferences) != 0 {
			return fmt.Errorf("runtime class must be cluster-scoped and unowned")
		}
	}
	if bundle.ResourceClaim != nil && bundle.ResourceClaim.ObjectMeta.Namespace != bundle.Namespace {
		return fmt.Errorf("resource claim namespace mismatch")
	}
	if bundle.Admission.Queue == "" || bundle.Admission.QuotaGroup == "" {
		return fmt.Errorf("admission metadata is incomplete")
	}
	if bundle.RuntimeClass == nil {
		if bundle.Job.Spec.Template.Spec.RuntimeClassName != "" {
			if err := validateRuntimeClassName(bundle.Job.Spec.Template.Spec.RuntimeClassName); err != nil {
				return err
			}
		}
		goto runtimeClassValidated
	}
	if bundle.Job.Spec.Template.Spec.RuntimeClassName != bundle.RuntimeClass.ObjectMeta.Name {
		return fmt.Errorf("job template runtime class must reference bundle runtime class")
	}
runtimeClassValidated:
	if bundle.Job.Spec.Template.Spec.RestartPolicy != "Never" && bundle.Job.Spec.Template.Spec.RestartPolicy != "OnFailure" {
		return fmt.Errorf("job pod restart policy must be Never or OnFailure")
	}
	if bundle.ResourceClaim == nil && len(bundle.Job.Spec.Template.Spec.ResourceClaims) > 0 {
		return fmt.Errorf("job template references absent resource claim")
	}
	if bundle.ResourceClaim != nil {
		if len(bundle.Job.Spec.Template.Spec.ResourceClaims) != 1 {
			return fmt.Errorf("job template must include exactly one resource claim reference")
		}
		if bundle.Job.Spec.Template.Spec.ResourceClaims[0].ResourceClaimName != bundle.ResourceClaim.ObjectMeta.Name {
			return fmt.Errorf("job template references unexpected resource claim")
		}
	}
	if len(bundle.Job.ObjectMeta.OwnerReferences) != 0 || bundle.ResourceClaim != nil && len(bundle.ResourceClaim.ObjectMeta.OwnerReferences) != 0 {
		return fmt.Errorf("materialized objects must not contain unresolved owner references")
	}
	return nil
}
