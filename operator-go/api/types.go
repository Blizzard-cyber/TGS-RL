package api

import (
	"encoding/json"
	"sort"
)

type TypeMeta struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
}

type OwnerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid,omitempty"`
	Controller bool   `json:"controller,omitempty"`
}

type ObjectMeta struct {
	Name              string            `json:"name,omitempty"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	Finalizers        []string          `json:"finalizers,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	OwnerReferences   []OwnerReference  `json:"ownerReferences,omitempty"`
}

type ResourceList map[string]string

type ResourceRequirements struct {
	Requests ResourceList             `json:"requests,omitempty"`
	Limits   ResourceList             `json:"limits,omitempty"`
	Claims   []ResourceClaimReference `json:"claims,omitempty"`
}

type ResourceClaimReference struct {
	Name    string `json:"name"`
	Request string `json:"request,omitempty"`
}

type EnvVar struct {
	Name      string        `json:"name"`
	Value     string        `json:"value,omitempty"`
	ValueFrom *EnvVarSource `json:"valueFrom,omitempty"`
}

type EnvVarSource struct {
	FieldRef *ObjectFieldSelector `json:"fieldRef,omitempty"`
}

type ObjectFieldSelector struct {
	APIVersion string `json:"apiVersion,omitempty"`
	FieldPath  string `json:"fieldPath"`
}

type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
	ReadOnly  bool   `json:"readOnly,omitempty"`
}

type EmptyDirVolumeSource struct{}

type Volume struct {
	Name     string                `json:"name"`
	EmptyDir *EmptyDirVolumeSource `json:"emptyDir,omitempty"`
}

type PodSecurityContext struct {
	FSGroup             int64  `json:"fsGroup,omitempty"`
	FSGroupChangePolicy string `json:"fsGroupChangePolicy,omitempty"`
}

type PodResourceClaim struct {
	Name                      string `json:"name"`
	ResourceClaimName         string `json:"resourceClaimName,omitempty"`
	ResourceClaimTemplateName string `json:"resourceClaimTemplateName,omitempty"`
}

type Container struct {
	Name                     string               `json:"name"`
	Image                    string               `json:"image"`
	Command                  []string             `json:"command,omitempty"`
	Args                     []string             `json:"args,omitempty"`
	Env                      []EnvVar             `json:"env,omitempty"`
	WorkingDir               string               `json:"workingDir,omitempty"`
	Resources                ResourceRequirements `json:"resources,omitempty"`
	VolumeMounts             []VolumeMount        `json:"volumeMounts,omitempty"`
	Ports                    []ContainerPort      `json:"ports,omitempty"`
	ReadinessProbe           *Probe               `json:"readinessProbe,omitempty"`
	ImagePullPolicy          string               `json:"imagePullPolicy,omitempty"`
	TerminationMessagePath   string               `json:"terminationMessagePath,omitempty"`
	TerminationMessagePolicy string               `json:"terminationMessagePolicy,omitempty"`
}

type ContainerPort struct {
	Name          string `json:"name,omitempty"`
	ContainerPort int32  `json:"containerPort"`
	Protocol      string `json:"protocol,omitempty"`
}

type HTTPGetAction struct {
	Path   string `json:"path"`
	Port   int32  `json:"port"`
	Scheme string `json:"scheme,omitempty"`
}

type Probe struct {
	HTTPGet          *HTTPGetAction `json:"httpGet,omitempty"`
	PeriodSeconds    int32          `json:"periodSeconds,omitempty"`
	TimeoutSeconds   int32          `json:"timeoutSeconds,omitempty"`
	FailureThreshold int32          `json:"failureThreshold,omitempty"`
	SuccessThreshold int32          `json:"successThreshold,omitempty"`
}

type PodSpec struct {
	RuntimeClassName string              `json:"runtimeClassName,omitempty"`
	HostNetwork      bool                `json:"hostNetwork,omitempty"`
	DNSPolicy        string              `json:"dnsPolicy,omitempty"`
	SecurityContext  *PodSecurityContext `json:"securityContext,omitempty"`
	ResourceClaims   []PodResourceClaim  `json:"resourceClaims,omitempty"`
	NodeSelector     map[string]string   `json:"nodeSelector,omitempty"`
	InitContainers   []Container         `json:"initContainers,omitempty"`
	Containers       []Container         `json:"containers"`
	Volumes          []Volume            `json:"volumes,omitempty"`
	RestartPolicy    string              `json:"restartPolicy"`
}

type PodTemplateSpec struct {
	ObjectMeta ObjectMeta `json:"metadata"`
	Spec       PodSpec    `json:"spec"`
}

type PodSet struct {
	Name     string          `json:"name"`
	Count    uint32          `json:"count"`
	Template PodTemplateSpec `json:"template"`
}

type WorkloadSpec struct {
	QueueName string   `json:"queueName"`
	Priority  int32    `json:"priority"`
	PodSets   []PodSet `json:"podSets"`
}

type WorkloadStatus struct {
	Admitted      bool         `json:"admitted"`
	Phase         string       `json:"phase,omitempty"`
	Reason        string       `json:"reason,omitempty"`
	ReservedQuota ResourceList `json:"reservedQuota,omitempty"`
	PreemptedKeys []string     `json:"preemptedKeys,omitempty"`
}

type Workload struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Spec       WorkloadSpec   `json:"spec"`
	Status     WorkloadStatus `json:"status,omitempty"`
}

type JobSpec struct {
	Parallelism  uint32          `json:"parallelism"`
	Completions  uint32          `json:"completions"`
	BackoffLimit *int32          `json:"backoffLimit,omitempty"`
	Suspend      bool            `json:"suspend,omitempty"`
	Template     PodTemplateSpec `json:"template"`
}

type JobStatus struct {
	Active    uint32 `json:"active"`
	Succeeded uint32 `json:"succeeded"`
	Failed    uint32 `json:"failed"`
	Paused    bool   `json:"paused,omitempty"`
	Deleted   bool   `json:"deleted,omitempty"`
}

type Job struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Spec       JobSpec   `json:"spec"`
	Status     JobStatus `json:"status,omitempty"`
}

type RuntimeClass struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Handler    string `json:"handler"`
}

type DeviceRequest struct {
	Name            string              `json:"name"`
	Exactly         *ExactDeviceRequest `json:"exactly,omitempty"`
	DeviceClassName string              `json:"deviceClassName,omitempty"`
	Selectors       []DeviceSelector    `json:"selectors,omitempty"`
	AllocationMode  string              `json:"allocationMode,omitempty"`
	Count           int64               `json:"count,omitempty"`
}

type ExactDeviceRequest struct {
	DeviceClassName string           `json:"deviceClassName"`
	Selectors       []DeviceSelector `json:"selectors,omitempty"`
	AllocationMode  string           `json:"allocationMode,omitempty"`
	Count           int64            `json:"count,omitempty"`
}

type DeviceSelector struct {
	CEL *CELDeviceSelector `json:"cel,omitempty"`
}

type CELDeviceSelector struct {
	Expression string `json:"expression"`
}

type DeviceClaim struct {
	Requests []DeviceRequest `json:"requests"`
}

type ResourceClaimSpec struct {
	Devices DeviceClaim `json:"devices"`
	// Count is an internal compatibility projection for the fake backend.
	// Kubernetes wire payloads use Devices.Requests[].Count exclusively.
	Count uint32 `json:"-"`
}

type DeviceRequestAllocationResult struct {
	Request string `json:"request"`
	Driver  string `json:"driver"`
	Pool    string `json:"pool"`
	Device  string `json:"device"`
}

type DeviceAllocationResult struct {
	Results []DeviceRequestAllocationResult `json:"results,omitempty"`
}

type AllocationResult struct {
	Devices DeviceAllocationResult `json:"devices,omitempty"`
}

type ResourceClaimStatus struct {
	Allocation *AllocationResult `json:"allocation,omitempty"`
}

type ResourceClaim struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Spec       ResourceClaimSpec   `json:"spec"`
	Status     ResourceClaimStatus `json:"status,omitempty"`
}

type ResourceClaimTemplateSpec struct {
	ObjectMeta ObjectMeta        `json:"metadata,omitempty"`
	Spec       ResourceClaimSpec `json:"spec"`
}

type ResourceClaimTemplate struct {
	TypeMeta   `json:",inline"`
	ObjectMeta `json:"metadata"`
	Spec       ResourceClaimTemplateSpec `json:"spec"`
}

type StatusProjection struct {
	RunState string `json:"runState"`
	JobState string `json:"jobState"`
	Reason   string `json:"reason,omitempty"`
}

type AdmissionSpec struct {
	Queue           string       `json:"queue"`
	QuotaGroup      string       `json:"quotaGroup"`
	Requests        ResourceList `json:"requests,omitempty"`
	AllowPreemption bool         `json:"allowPreemption"`
}

type AdmissionStatus struct {
	Allowed       bool         `json:"allowed"`
	Phase         string       `json:"phase,omitempty"`
	Reason        string       `json:"reason,omitempty"`
	ReservedQuota ResourceList `json:"reservedQuota,omitempty"`
	PreemptedKeys []string     `json:"preemptedKeys,omitempty"`
}

type ControllerStatus struct {
	ObservedGeneration uint64 `json:"observedGeneration"`
	AppliedGeneration  uint64 `json:"appliedGeneration"`
	Phase              string `json:"phase,omitempty"`
	Reason             string `json:"reason,omitempty"`
	BundleFingerprint  string `json:"bundleFingerprint,omitempty"`
	Idempotent         bool   `json:"idempotent,omitempty"`
}

// RuntimeTarget preserves the concrete runtime identity compiled from a
// scheduler binding. Lifecycle control uses this mapping to fence requests
// against the exact materialized generation instead of guessing from names.
type RuntimeTarget struct {
	RuntimeUnitID string   `json:"runtimeUnitId"`
	SandboxID     string   `json:"sandboxId"`
	BindingID     string   `json:"bindingId,omitempty"`
	DecisionID    string   `json:"decisionId,omitempty"`
	PlanID        string   `json:"planId,omitempty"`
	ActionID      string   `json:"actionId,omitempty"`
	DeviceIDs     []string `json:"deviceIds,omitempty"`
	CPUMillis     uint64   `json:"cpuMillis,omitempty"`
	MemoryBytes   uint64   `json:"memoryBytes,omitempty"`
	Accelerators  float64  `json:"accelerators,omitempty"`
	Generation    uint64   `json:"generation"`
}

type Bundle struct {
	Key            string          `json:"key"`
	Namespace      string          `json:"namespace"`
	Generation     uint64          `json:"generation"`
	Fingerprint    string          `json:"fingerprint"`
	GPUProfile     string          `json:"gpuProfile"`
	SourceRunID    string          `json:"sourceRunId"`
	SourceJobID    string          `json:"sourceJobId"`
	SourceTraceID  string          `json:"sourceTraceId"`
	RuntimeTargets []RuntimeTarget `json:"runtimeTargets,omitempty"`
	Workload       Workload        `json:"workload"`
	Job            Job             `json:"job"`
	RuntimeClass   *RuntimeClass   `json:"runtimeClass,omitempty"`
	// ResourceClaim is retained for decoding bundles written before the
	// ResourceClaimTemplate migration. New bundles use ResourceClaimTemplate so
	// Kueue can account for DRA devices before Kubernetes creates the Pod claim.
	ResourceClaim         *ResourceClaim         `json:"resourceClaim,omitempty"`
	ResourceClaimTemplate *ResourceClaimTemplate `json:"resourceClaimTemplate,omitempty"`
	Admission             AdmissionSpec          `json:"admission"`
	AdmissionStatus       AdmissionStatus        `json:"admissionStatus"`
	ControllerStatus      ControllerStatus       `json:"controllerStatus"`
	StatusProjection      StatusProjection       `json:"statusProjection"`
}

func CloneMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func SortedKeys(src map[string]string) []string {
	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func CloneResourceList(src ResourceList) ResourceList {
	if len(src) == 0 {
		return nil
	}
	dst := make(ResourceList, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func CloneBundle(src *Bundle) (*Bundle, error) {
	if src == nil {
		return nil, nil
	}
	payload, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}
	var dst Bundle
	if err := json.Unmarshal(payload, &dst); err != nil {
		return nil, err
	}
	return &dst, nil
}
