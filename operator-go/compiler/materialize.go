package compiler

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/Blizzard-cyber/TGS-RL/internal/bootstrapauth"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
)

const (
	WorkerBootstrapMountPath  = "/var/run/tgsrl-bootstrap"
	WorkerBootstrapBinaryPath = WorkerBootstrapMountPath + "/tgsrl-worker-bootstrap"
)

func buildBundle(input *normalizedInput) (*api.Bundle, error) {
	zeroBackoff := int32(0)
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
					Name:       "main",
					Image:      primaryImage(input.Manifest),
					Command:    append([]string(nil), input.Manifest.GetCommand()...),
					Args:       append([]string(nil), input.Manifest.GetArgs()...),
					Env:        buildEnv(input),
					WorkingDir: input.Manifest.GetWorkingDirectory(),
					Resources:  buildResources(input),
				},
			},
			RestartPolicy: "Never",
		},
	}
	workload := api.Workload{
		TypeMeta: api.TypeMeta{APIVersion: input.KubernetesAPIs.KueueWorkload, Kind: "Workload"},
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
			Parallelism:  1,
			Completions:  1,
			BackoffLimit: &zeroBackoff,
			Suspend:      true,
			Template:     template,
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

	var resourceClaimTemplate *api.ResourceClaimTemplate
	if requiresResourceClaim(input.GPUProfile, input.resourcesPerUnit.GetAcceleratorUnits()) {
		deviceIDs, err := concreteDeviceIDs(input.binding.GetDeviceIds())
		if err != nil {
			return nil, err
		}
		if len(input.draDevices) != len(deviceIDs) {
			return nil, fmt.Errorf("NVIDIA DRA device inventory is incomplete")
		}
		deviceClass := input.draDevices[0].DeviceClass
		selectors := []api.DeviceSelector{{CEL: &api.CELDeviceSelector{Expression: nvidiaDRADeviceSelector(input.draDevices)}}}
		request := api.DeviceRequest{Name: "accelerator"}
		if input.KubernetesAPIs.DRAResourceClaim != DRAResourceClaimV1Beta1 {
			request.Exactly = &api.ExactDeviceRequest{
				DeviceClassName: deviceClass,
				Selectors:       selectors,
				AllocationMode:  "ExactCount",
				Count:           int64(len(deviceIDs)),
			}
		} else {
			request.DeviceClassName = deviceClass
			request.Selectors = selectors
			request.AllocationMode = "ExactCount"
			request.Count = int64(len(deviceIDs))
		}
		resourceClaimTemplate = &api.ResourceClaimTemplate{
			TypeMeta: api.TypeMeta{APIVersion: input.KubernetesAPIs.DRAResourceClaim, Kind: "ResourceClaimTemplate"},
			ObjectMeta: api.ObjectMeta{
				Name:        buildObjectName("claimtemplate", input),
				Namespace:   input.Namespace,
				Labels:      api.CloneMap(labels),
				Annotations: api.CloneMap(annotations),
			},
			Spec: api.ResourceClaimTemplateSpec{
				ObjectMeta: api.ObjectMeta{Labels: api.CloneMap(labels), Annotations: api.CloneMap(annotations)},
				Spec: api.ResourceClaimSpec{
					Devices: api.DeviceClaim{Requests: []api.DeviceRequest{request}},
					Count:   uint32(len(deviceIDs)),
				},
			},
		}
		job.Spec.Template.Spec.ResourceClaims = []api.PodResourceClaim{{
			Name:                      "accelerator",
			ResourceClaimTemplateName: resourceClaimTemplate.ObjectMeta.Name,
		}}
		job.Spec.Template.Spec.Containers[0].Resources.Claims = []api.ResourceClaimReference{{
			Name:    "accelerator",
			Request: "accelerator",
		}}
		// Kueue admission evaluates the Workload pod set, while Kubernetes runs
		// the Job template. Keep both projections byte-for-byte aligned after
		// adding the DRA claim so admission and execution use the same device.
		workload.Spec.PodSets[0].Template = job.Spec.Template
	}
	if input.Runtime.Bootstrap.Enabled {
		// Resource claims are added after the base template is constructed. Re-run
		// bootstrap wiring so mandatory DRA device verification is reflected in
		// the final Job and Kueue pod specs.
		if err := configureWorkerBootstrap(&job.Spec.Template, input); err != nil {
			return nil, err
		}
		workload.Spec.PodSets[0].Template = job.Spec.Template
	}
	if IsHAMIGPUProfile(input.GPUProfile) {
		workload.Spec.PodSets[0].Template = job.Spec.Template
	}

	return &api.Bundle{
		Key:                   bundleKey(input),
		Namespace:             input.Namespace,
		Generation:            input.Generation,
		GPUProfile:            input.GPUProfile,
		SourceRunID:           input.Run.GetRunId(),
		SourceJobID:           input.Run.GetJobId(),
		SourceTraceID:         input.Run.GetTraceId(),
		RuntimeTargets:        buildRuntimeTargets(input.Plan, input.Generation),
		Workload:              workload,
		Job:                   job,
		RuntimeClass:          runtimeClass,
		ResourceClaimTemplate: resourceClaimTemplate,
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

func configureWorkerBootstrap(template *api.PodTemplateSpec, input *normalizedInput) error {
	if template == nil || len(template.Spec.Containers) != 1 {
		return fmt.Errorf("worker bootstrap requires exactly one workload container")
	}
	bootstrap := input.Runtime.Bootstrap
	registrationToken, err := bootstrapauth.Sign(bootstrap.RegistrySigningKey, bootstrapauth.Claims{
		RunID: input.Run.GetRunId(), JobID: input.Run.GetJobId(), RuntimeUnitID: bindingRuntimeUnitID(input.binding),
		SandboxID: input.binding.GetSandboxId(), BindingID: input.binding.GetBindingId(),
		Generation: input.Generation, DeviceIDs: input.binding.GetDeviceIds(),
	})
	if err != nil {
		return fmt.Errorf("derive scoped worker registration token: %w", err)
	}
	const (
		volumeName = "tgsrl-bootstrap"
	)
	main := &template.Spec.Containers[0]
	applyKubernetesContainerDefaults(main)
	workingDirectory := main.WorkingDir
	workload := append([]string(nil), main.Command...)
	workload = append(workload, main.Args...)
	main.Command = []string{WorkerBootstrapBinaryPath}
	main.Args = []string{"--listen", "0.0.0.0:50092", "--"}
	main.Args = append(main.Args, workload...)
	main.WorkingDir = ""
	main.VolumeMounts = append(main.VolumeMounts, api.VolumeMount{Name: volumeName, MountPath: WorkerBootstrapMountPath, ReadOnly: true})
	main.Ports = append(main.Ports, api.ContainerPort{Name: "tgsrl-control", ContainerPort: 50092, Protocol: "TCP"})
	main.ReadinessProbe = &api.Probe{HTTPGet: &api.HTTPGetAction{Path: "/readyz", Port: 50092, Scheme: "HTTP"}, PeriodSeconds: 2, TimeoutSeconds: 1, FailureThreshold: 3, SuccessThreshold: 1}
	main.Env = append(main.Env,
		api.EnvVar{Name: "TGSRL_WORKER_REGISTRY_URL", Value: bootstrap.RegistryURL},
		api.EnvVar{Name: "TGSRL_WORKER_REGISTRY_TOKEN", Value: registrationToken},
		api.EnvVar{Name: "TGSRL_POD_IP", ValueFrom: &api.EnvVarSource{FieldRef: &api.ObjectFieldSelector{APIVersion: "v1", FieldPath: "status.podIP"}}},
		api.EnvVar{Name: "TGSRL_POD_UID", ValueFrom: &api.EnvVarSource{FieldRef: &api.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.uid"}}},
	)
	if workingDirectory != "" {
		main.Env = append(main.Env, api.EnvVar{Name: "TGSRL_WORKING_DIRECTORY", Value: workingDirectory})
	}
	if bootstrap.VerifyDeviceIDs || len(main.Resources.Claims) > 0 || IsHAMIGPUProfile(input.GPUProfile) {
		main.Env = append(main.Env, api.EnvVar{Name: "TGSRL_VERIFY_DEVICE_IDENTITIES", Value: "true"})
	}
	sort.Slice(main.Env, func(i, j int) bool { return main.Env[i].Name < main.Env[j].Name })
	template.Spec.InitContainers = []api.Container{{
		Name:         "install-tgsrl-bootstrap",
		Image:        bootstrap.InstallerImage,
		Command:      []string{"/usr/local/bin/tgsrl-worker-bootstrap"},
		Args:         []string{"install", "--target", WorkerBootstrapBinaryPath},
		VolumeMounts: []api.VolumeMount{{Name: volumeName, MountPath: WorkerBootstrapMountPath}},
	}}
	applyKubernetesContainerDefaults(&template.Spec.InitContainers[0])
	template.Spec.SecurityContext = &api.PodSecurityContext{FSGroup: 65532, FSGroupChangePolicy: "OnRootMismatch"}
	if bootstrap.HostNetwork {
		template.Spec.HostNetwork = true
		template.Spec.DNSPolicy = "ClusterFirstWithHostNet"
	}
	template.Spec.Volumes = []api.Volume{{Name: volumeName, EmptyDir: &api.EmptyDirVolumeSource{}}}
	return nil
}

func applyKubernetesContainerDefaults(container *api.Container) {
	if container == nil {
		return
	}
	container.ImagePullPolicy = "IfNotPresent"
	container.TerminationMessagePath = "/dev/termination-log"
	container.TerminationMessagePolicy = "File"
}

func nvidiaDRADeviceSelector(devices []DRADevice) string {
	quoted := make([]string, 0, len(devices))
	for _, device := range devices {
		quoted = append(quoted, strconv.Quote(device.UUID))
	}
	sort.Strings(quoted)
	return `device.driver == "` + NVIDIADRADriver + `" && device.attributes["` + NVIDIADRADriver + `"].type == "` + devices[0].Type + `" && device.attributes["` + NVIDIADRADriver + `"].uuid in [` + strings.Join(quoted, ", ") + `]`
}

func nvidiaDRADeviceSelectorForClass(deviceIDs []string, deviceClass string) string {
	deviceType := ""
	switch deviceClass {
	case NVIDIADRAFullGPUDeviceClass:
		deviceType = "gpu"
	case NVIDIADRAMIGDeviceClass:
		deviceType = "mig"
	}
	devices := make([]DRADevice, 0, len(deviceIDs))
	for _, deviceID := range deviceIDs {
		devices = append(devices, DRADevice{UUID: deviceID, Type: deviceType})
	}
	return nvidiaDRADeviceSelector(devices)
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
	if bundle.ResourceClaim != nil && bundle.ResourceClaimTemplate != nil {
		return fmt.Errorf("bundle cannot contain both a resource claim and resource claim template")
	}
	if bundle.ResourceClaim != nil && bundle.ResourceClaim.ObjectMeta.Namespace != bundle.Namespace {
		return fmt.Errorf("resource claim namespace mismatch")
	}
	if bundle.ResourceClaimTemplate != nil && bundle.ResourceClaimTemplate.ObjectMeta.Namespace != bundle.Namespace {
		return fmt.Errorf("resource claim template namespace mismatch")
	}
	if bundle.GPUProfile == GPUProfileKubernetesDRA {
		if err := validateDRAResourceClaim(bundle); err != nil {
			return err
		}
	} else if IsHAMIGPUProfile(bundle.GPUProfile) {
		if err := validateHAMIProjection(bundle); err != nil {
			return err
		}
	} else if bundle.ResourceClaim != nil || bundle.ResourceClaimTemplate != nil {
		return fmt.Errorf("resource claim or template requires kubernetes-dra GPU profile")
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
	if bundle.ResourceClaim == nil && bundle.ResourceClaimTemplate == nil && len(bundle.Job.Spec.Template.Spec.ResourceClaims) > 0 {
		return fmt.Errorf("job template references absent resource claim or template")
	}
	if bundle.ResourceClaim != nil || bundle.ResourceClaimTemplate != nil {
		if len(bundle.Job.Spec.Template.Spec.ResourceClaims) != 1 {
			return fmt.Errorf("job template must include exactly one resource claim reference")
		}
		reference := bundle.Job.Spec.Template.Spec.ResourceClaims[0]
		if bundle.ResourceClaim != nil && (reference.ResourceClaimName != bundle.ResourceClaim.ObjectMeta.Name || reference.ResourceClaimTemplateName != "") {
			return fmt.Errorf("job template references unexpected resource claim")
		}
		if bundle.ResourceClaimTemplate != nil && (reference.ResourceClaimTemplateName != bundle.ResourceClaimTemplate.ObjectMeta.Name || reference.ResourceClaimName != "") {
			return fmt.Errorf("job template references unexpected resource claim template")
		}
	}
	if len(bundle.Workload.Spec.PodSets) != 1 || !reflect.DeepEqual(bundle.Workload.Spec.PodSets[0].Template.Spec, bundle.Job.Spec.Template.Spec) {
		return fmt.Errorf("workload pod set and job template must use the same pod specification")
	}
	if len(bundle.Job.ObjectMeta.OwnerReferences) != 0 || bundle.ResourceClaim != nil && len(bundle.ResourceClaim.ObjectMeta.OwnerReferences) != 0 || bundle.ResourceClaimTemplate != nil && len(bundle.ResourceClaimTemplate.ObjectMeta.OwnerReferences) != 0 {
		return fmt.Errorf("materialized objects must not contain unresolved owner references")
	}
	return nil
}

func validateHAMIProjection(bundle *api.Bundle) error {
	if bundle == nil || len(bundle.RuntimeTargets) != 1 || len(bundle.Job.Spec.Template.Spec.Containers) != 1 {
		return fmt.Errorf("HAMi vGPU bundle requires one runtime target and one workload container")
	}
	deviceIDs, err := concreteDeviceIDs(bundle.RuntimeTargets[0].DeviceIDs)
	if err != nil {
		return err
	}
	if len(deviceIDs) != 1 {
		return fmt.Errorf("HAMi vGPU bundle requires exactly one physical GPU UUID")
	}
	annotations := bundle.Job.Spec.Template.ObjectMeta.Annotations
	if annotations[HAMINVIDIAUseUUIDAnnotation] != deviceIDs[0] || annotations[HAMINVIDIAModeAnnotation] != "hami-core" {
		return fmt.Errorf("HAMi vGPU annotations do not match the concrete binding")
	}
	resources := bundle.Job.Spec.Template.Spec.Containers[0].Resources
	for _, name := range []string{HAMINVIDIAResource, HAMINVIDIACoreResource, HAMINVIDIAMemoryPercent} {
		if resources.Requests[name] == "" || resources.Limits[name] != resources.Requests[name] {
			return fmt.Errorf("HAMi vGPU resource %q is missing or inconsistent", name)
		}
	}
	if resources.Requests[HAMINVIDIAResource] != "1" {
		return fmt.Errorf("HAMi vGPU must request exactly one physical GPU")
	}
	return nil
}

func validateDRAResourceClaim(bundle *api.Bundle) error {
	claimSpec, apiVersion := bundleDRAClaimSpec(bundle)
	if claimSpec == nil {
		return fmt.Errorf("kubernetes-dra profile requires resource claim template")
	}
	if len(bundle.RuntimeTargets) != 1 {
		return fmt.Errorf("kubernetes-dra bundle requires exactly one runtime target")
	}
	deviceIDs, err := concreteDeviceIDs(bundle.RuntimeTargets[0].DeviceIDs)
	if err != nil {
		return err
	}
	requests := claimSpec.Devices.Requests
	if len(requests) != 1 {
		return fmt.Errorf("kubernetes-dra resource claim requires exactly one device request")
	}
	request := requests[0]
	class, selectors, mode, count := request.DeviceClassName, request.Selectors, request.AllocationMode, request.Count
	if apiVersion != DRAResourceClaimV1Beta1 {
		if request.Exactly == nil {
			return fmt.Errorf("kubernetes-dra resource claim requires an exact device request")
		}
		class, selectors, mode, count = request.Exactly.DeviceClassName, request.Exactly.Selectors, request.Exactly.AllocationMode, request.Exactly.Count
	} else if request.Exactly != nil {
		return fmt.Errorf("resource.k8s.io/v1beta1 requires a flat device request")
	}
	if request.Name != "accelerator" || (class != NVIDIADRAFullGPUDeviceClass && class != NVIDIADRAMIGDeviceClass) || mode != "ExactCount" || count != int64(len(bundle.RuntimeTargets[0].DeviceIDs)) {
		return fmt.Errorf("kubernetes-dra resource request does not match the concrete binding")
	}
	if len(selectors) != 1 || selectors[0].CEL == nil || selectors[0].CEL.Expression != nvidiaDRADeviceSelectorForClass(deviceIDs, class) {
		return fmt.Errorf("kubernetes-dra resource request must select the binding device UUIDs")
	}
	return nil
}

func bundleDRAClaimSpec(bundle *api.Bundle) (*api.ResourceClaimSpec, string) {
	if bundle == nil {
		return nil, ""
	}
	if bundle.ResourceClaimTemplate != nil {
		return &bundle.ResourceClaimTemplate.Spec.Spec, bundle.ResourceClaimTemplate.APIVersion
	}
	if bundle.ResourceClaim != nil {
		return &bundle.ResourceClaim.Spec, bundle.ResourceClaim.APIVersion
	}
	return nil, ""
}
