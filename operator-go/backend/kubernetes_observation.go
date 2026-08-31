package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
)

func (b *KubernetesBackend) Snapshot(ctx context.Context, bundle *api.Bundle) (*ObservationSnapshot, bool, error) {
	reader, ok := b.client.(bundleadapter.Reader)
	if !ok {
		return nil, false, fmt.Errorf("backend client does not support observation readback")
	}
	adapter := bundleadapter.NewKubernetes(0)
	snapshot, terminal, err := snapshotFromAdapter(adapter.ObserveOnce(ctx, reader, bundle))
	if err != nil || snapshot == nil {
		return snapshot, terminal, err
	}
	metadata, found, err := b.controlMetadataForBundle(ctx, bundle.Key)
	if err != nil {
		return nil, terminal, err
	}
	if found {
		applyControlMetadata(snapshot, metadata)
	}
	return snapshot, terminal, nil
}

func (b *KubernetesBackend) fakeSnapshots(ctx context.Context, bundle *api.Bundle) ([]*ObservationSnapshot, error) {
	workloadObject, workloadFound, err := b.client.Get(ctx, metaObject(bundle.Workload.TypeMeta.APIVersion, bundle.Workload.TypeMeta.Kind, bundle.Workload.ObjectMeta))
	if err != nil {
		return nil, err
	}
	if !workloadFound {
		return nil, ErrRuntimeNotFound
	}
	object, ok, err := b.client.Get(ctx, metaObject(bundle.Job.TypeMeta.APIVersion, bundle.Job.TypeMeta.Kind, bundle.Job.ObjectMeta))
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrRuntimeNotFound
	}
	var job api.Job
	if err := json.Unmarshal(object.Payload, &job); err != nil {
		return nil, err
	}
	var workload api.Workload
	if err := json.Unmarshal(workloadObject.Payload, &workload); err != nil {
		return nil, err
	}
	claimAllocated := true
	deviceIDs := []string(nil)
	if bundle.ResourceClaim != nil {
		claimAllocated = false
		claimObject, found, err := b.client.Get(ctx, metaObject(bundle.ResourceClaim.TypeMeta.APIVersion, bundle.ResourceClaim.TypeMeta.Kind, bundle.ResourceClaim.ObjectMeta))
		if err != nil {
			return nil, err
		}
		if found {
			var claim api.ResourceClaim
			if err := json.Unmarshal(claimObject.Payload, &claim); err != nil {
				return nil, err
			}
			if bundle.GPUProfile == compiler.GPUProfileKubernetesDRA {
				claimAllocated = claim.Status.Allocation != nil && len(claim.Status.Allocation.Devices.Results) > 0
				if claimAllocated && len(bundle.RuntimeTargets) == 1 && len(claim.Status.Allocation.Devices.Results) == len(bundle.RuntimeTargets[0].DeviceIDs) {
					deviceIDs = append([]string(nil), bundle.RuntimeTargets[0].DeviceIDs...)
					sort.Strings(deviceIDs)
				}
			} else {
				claimAllocated = claim.Status.Allocation != nil
			}
		}
	}
	metadata, hasMetadata, err := b.controlMetadataForBundle(ctx, bundle.Key)
	if err != nil {
		return nil, err
	}
	workloadAdmitted := workload.Status.Admitted
	bound := &ObservationSnapshot{ObservedGeneration: bundle.Generation, WorkloadAdmitted: workloadAdmitted, ResourceClaimsAllocated: claimAllocated, AllocatedDeviceIDs: deviceIDs, Reason: "fake backend observed workload admission", ObservedAt: time.Now().UTC()}
	observed := &ObservationSnapshot{ObservedGeneration: bundle.Generation, WorkloadAdmitted: workloadAdmitted, ResourceClaimsAllocated: claimAllocated, AllocatedDeviceIDs: append([]string(nil), deviceIDs...), JobActive: job.Status.Active, JobSucceeded: job.Status.Succeeded, JobFailed: job.Status.Failed, JobPaused: job.Status.Paused, JobDeleted: job.Status.Deleted, Reason: "fake backend observed job state", ObservedAt: time.Now().UTC()}
	if hasMetadata {
		applyControlMetadata(bound, metadata)
		applyControlMetadata(observed, metadata)
	}
	return []*ObservationSnapshot{bound, observed}, nil
}

func applyControlMetadata(snapshot *ObservationSnapshot, metadata ControlMetadata) {
	if snapshot == nil {
		return
	}
	snapshot.ControlRequestID = metadata.RequestID
	snapshot.ControlIdempotencyKey = metadata.IdempotencyKey
	snapshot.ControlAction = metadata.Action
	snapshot.ControlBackendRevision = metadata.BackendRevision
	snapshot.ControlCommitted = metadata.Committed
}

func snapshotFromAdapter(snapshot *bundleadapter.Snapshot, terminal bool, err error) (*ObservationSnapshot, bool, error) {
	if err != nil || snapshot == nil {
		return nil, terminal, err
	}
	return &ObservationSnapshot{ObservedGeneration: snapshot.ObservedGeneration, WorkloadAdmitted: snapshot.WorkloadAdmitted, ResourceClaimsAllocated: snapshot.ResourceClaimsAllocated, AllocatedDeviceIDs: append([]string(nil), snapshot.AllocatedDeviceIDs...), JobActive: snapshot.JobActive, JobSucceeded: snapshot.JobSucceeded, JobFailed: snapshot.JobFailed, JobPaused: snapshot.JobPaused, JobDeleted: snapshot.JobDeleted, Reason: snapshot.Reason, ObservedAt: snapshot.ObservedAt, ControlRequestID: snapshot.ControlRequestID, ControlIdempotencyKey: snapshot.ControlIdempotencyKey, ControlAction: snapshot.ControlAction, ControlBackendRevision: snapshot.ControlBackendRevision, ControlCommitted: snapshot.ControlCommitted}, terminal, nil
}
