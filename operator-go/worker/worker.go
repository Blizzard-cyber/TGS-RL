package worker

import (
	"context"
	"fmt"
	"io"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/admission"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/cursor"
	runtimepub "github.com/Blizzard-cyber/TGS-RL/operator-go/runtime"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/statuswatch"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type DecisionSource interface {
	Watch(ctx context.Context, after cursor.Cursor) (DecisionStream, error)
}

type DecisionStream interface {
	Recv() (*tgsrlv1.DecisionRecord, error)
}

type JobRunClient interface {
	GetJobRun(ctx context.Context, in *tgsrlv1.GetJobRunRequest, opts ...grpc.CallOption) (*tgsrlv1.GetJobRunResponse, error)
}

type RuntimeManifestClient interface {
	GetRuntimeManifest(ctx context.Context, in *tgsrlv1.GetRuntimeManifestRequest, opts ...grpc.CallOption) (*tgsrlv1.GetRuntimeManifestResponse, error)
}

type Reconciler interface {
	Reconcile(ctx context.Context, input compiler.CompileInput, policy admission.QueuePolicy) (*ReconcileResult, error)
}

type ReconcileResult struct {
	Bundle     *api.Bundle
	Applied    bool
	Idempotent bool
}

type Worker struct {
	source       DecisionSource
	jobRuns      JobRunClient
	manifests    RuntimeManifestClient
	reconciler   Reconciler
	cursors      cursor.Repository
	deliveries   DeliveryRepository
	observations *ObservationManager
	queuePolicy  admission.QueuePolicy
	namespace    string
	gpuProfiles  []string
	retryBase    time.Duration
	retryMax     time.Duration
}

type Config struct {
	Source        DecisionSource
	JobRuns       JobRunClient
	Manifests     RuntimeManifestClient
	Reconciler    Reconciler
	Observer      statuswatch.Observer
	Publisher     runtimepub.Publisher
	Cursors       cursor.Repository
	Deliveries    DeliveryRepository
	Registrations RegistrationRepository
	Bundles       BundleSource
	QueuePolicy   admission.QueuePolicy
	Namespace     string
	GPUProfiles   []string
}

func New(config Config) (*Worker, error) {
	if config.Source == nil {
		return nil, fmt.Errorf("decision source is required")
	}
	if config.JobRuns == nil {
		return nil, fmt.Errorf("job run client is required")
	}
	if config.Manifests == nil {
		return nil, fmt.Errorf("runtime manifest client is required")
	}
	if config.Reconciler == nil {
		return nil, fmt.Errorf("reconciler is required")
	}
	if config.Observer == nil {
		return nil, fmt.Errorf("status observer is required")
	}
	if config.Publisher == nil {
		return nil, fmt.Errorf("runtime publisher is required")
	}
	if config.Cursors == nil {
		return nil, fmt.Errorf("cursor repository is required")
	}
	if config.Deliveries == nil {
		return nil, fmt.Errorf("delivery repository is required")
	}
	registrations := config.Registrations
	if registrations == nil {
		registrations, _ = config.Deliveries.(RegistrationRepository)
	}
	if registrations == nil {
		return nil, fmt.Errorf("registration repository is required")
	}
	observations, err := NewObservationManager(config.Observer, config.Publisher, registrations)
	if err != nil {
		return nil, err
	}
	observations.SetBundleSource(config.Bundles)
	return &Worker{
		source:       config.Source,
		jobRuns:      config.JobRuns,
		manifests:    config.Manifests,
		reconciler:   config.Reconciler,
		cursors:      config.Cursors,
		deliveries:   config.Deliveries,
		observations: observations,
		queuePolicy:  config.QueuePolicy,
		namespace:    config.Namespace,
		gpuProfiles:  append([]string(nil), config.GPUProfiles...),
		retryBase:    200 * time.Millisecond,
		retryMax:     5 * time.Second,
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 2)
	go func() { errCh <- w.observations.Run(runCtx) }()
	go func() { errCh <- w.runDecisions(runCtx) }()

	firstErr := <-errCh
	cancel()
	secondErr := <-errCh
	if ctx.Err() != nil {
		return nil
	}
	if firstErr != nil {
		return firstErr
	}
	return secondErr
}

func (w *Worker) runDecisions(ctx context.Context) error {
	backoff := w.retryBase
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}
	maxBackoff := w.retryMax
	if maxBackoff <= 0 || maxBackoff < backoff {
		maxBackoff = 5 * time.Second
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		err := w.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
		} else {
			backoff = w.retryBase
			if backoff <= 0 {
				backoff = 200 * time.Millisecond
			}
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (w *Worker) RunOnce(ctx context.Context) error {
	after, err := w.cursors.Load()
	if err != nil {
		return err
	}
	record, err := w.deliveries.Load()
	if err != nil {
		return err
	}
	if record.Sequence > 0 && record.Sequence <= after.Sequence {
		if err := w.deliveries.Clear(); err != nil {
			return err
		}
	}
	stream, err := w.source.Watch(ctx, after)
	if err != nil {
		return err
	}
	for {
		decision, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if decision == nil {
			continue
		}
		if err := w.handleDecision(ctx, decision); err != nil {
			return err
		}
	}
}

func (w *Worker) handleDecision(ctx context.Context, decision *tgsrlv1.DecisionRecord) error {
	record, err := w.startOrResumeDelivery(decision)
	if err != nil {
		return err
	}
	if !shouldProcess(decision) {
		return w.completeDelivery(decision)
	}
	plan := decision.GetSelectedPlan()
	manifestResp, err := w.manifests.GetRuntimeManifest(ctx, &tgsrlv1.GetRuntimeManifestRequest{
		RunId: decision.GetRunId(),
	})
	if err != nil {
		return err
	}
	jobRunResp, err := w.jobRuns.GetJobRun(ctx, &tgsrlv1.GetJobRunRequest{
		JobId: manifestResp.GetManifest().GetJobId(),
		RunId: decision.GetRunId(),
	})
	if err != nil {
		return err
	}
	input := compiler.CompileInput{
		Namespace:        w.namespace,
		GPUProfiles:      append([]string(nil), w.gpuProfiles...),
		Generation:       decisionGeneration(decision),
		JobRun:           proto.Clone(jobRunResp.GetRun()).(*tgsrlv1.JobRun),
		RuntimeManifest:  proto.Clone(manifestResp.GetManifest()).(*tgsrlv1.RuntimeManifest),
		PlacementPlan:    proto.Clone(plan).(*tgsrlv1.PlacementPlan),
		AdmissionAllowed: true,
	}
	result, err := w.reconciler.Reconcile(ctx, input, w.queuePolicy)
	if err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("reconcile decision %q returned no result", decision.GetDecisionId())
	}
	if !result.Applied && !result.Idempotent {
		return w.completeDelivery(decision)
	}
	record.Phase = "reconciled"
	if err := w.deliveries.Save(record); err != nil {
		return err
	}
	if result.Bundle == nil {
		return fmt.Errorf("reconcile applied decision %q without a bundle", decision.GetDecisionId())
	}
	if err := w.observations.Register(ctx, ObservationRegistration{
		BundleKey:         result.Bundle.Key,
		Bundle:            result.Bundle,
		Decision:          decision,
		JobRun:            jobRunResp.GetRun(),
		PublishedEventIDs: record.PublishedEventIDs,
	}); err != nil {
		return err
	}
	return w.completeDelivery(decision)
}

// ObservationRegistrar exposes the durable recovery handoff without coupling
// callers to the worker's decision loop.
func (w *Worker) ObservationRegistrar() Registrar {
	return w.observations
}

func (w *Worker) startOrResumeDelivery(decision *tgsrlv1.DecisionRecord) (DeliveryRecord, error) {
	record, err := w.deliveries.Load()
	if err != nil {
		return DeliveryRecord{}, err
	}
	if record.DecisionID == decision.GetDecisionId() && record.Sequence == decision.GetSequence() {
		if record.PublishedEventIDs == nil {
			record.PublishedEventIDs = map[string]bool{}
		}
		return record, nil
	}
	if record.DecisionID != "" || record.Sequence != 0 {
		return DeliveryRecord{}, fmt.Errorf("unfinished delivery %q at sequence %d conflicts with decision %q at sequence %d", record.DecisionID, record.Sequence, decision.GetDecisionId(), decision.GetSequence())
	}
	record = DeliveryRecord{
		DecisionID:        decision.GetDecisionId(),
		Sequence:          decision.GetSequence(),
		Cursor:            decision.GetCursor(),
		Phase:             "reconciling",
		PublishedEventIDs: map[string]bool{},
	}
	if err := w.deliveries.Save(record); err != nil {
		return DeliveryRecord{}, err
	}
	return record, nil
}

func (w *Worker) completeDelivery(decision *tgsrlv1.DecisionRecord) error {
	if err := w.cursors.Save(cursor.Cursor{
		DecisionID: decision.GetDecisionId(),
		Sequence:   decision.GetSequence(),
		Cursor:     decision.GetCursor(),
	}); err != nil {
		return err
	}
	return w.deliveries.Clear()
}

func shouldProcess(decision *tgsrlv1.DecisionRecord) bool {
	if decision == nil || decision.GetSelectedPlan() == nil || decision.GetFallback() {
		return false
	}
	if len(decision.GetActionResults()) == 0 {
		return false
	}
	for _, result := range decision.GetActionResults() {
		if result.GetStatus() != tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED {
			return false
		}
	}
	return true
}

func decisionGeneration(decision *tgsrlv1.DecisionRecord) uint64 {
	if decision.GetGeneration() != 0 {
		return decision.GetGeneration()
	}
	if decision.GetSelectedPlan().GetGeneration() != 0 {
		return decision.GetSelectedPlan().GetGeneration()
	}
	for _, result := range decision.GetActionResults() {
		if result.GetObservedGeneration() != 0 {
			return result.GetObservedGeneration()
		}
	}
	return 1
}
