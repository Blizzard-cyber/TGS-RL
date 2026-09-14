package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/internal/protocolmeta"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/api"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/bundleadapter"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/compiler"
	runtimepub "github.com/Blizzard-cyber/TGS-RL/operator-go/runtime"
	"github.com/Blizzard-cyber/TGS-RL/operator-go/statuswatch"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ObservationRegistration contains everything required to resume observation
// after a process restart. BundleKey is the stable identity used to guarantee
// one watcher per materialized bundle.
type ObservationRegistration struct {
	BundleKey               string                  `json:"bundleKey"`
	Bundle                  *api.Bundle             `json:"bundle"`
	Decision                *tgsrlv1.DecisionRecord `json:"decision"`
	JobRun                  *tgsrlv1.JobRun         `json:"jobRun"`
	PublishedEventIDs       map[string]bool         `json:"publishedEventIds,omitempty"`
	LastState               tgsrlv1.RuntimeState    `json:"lastState,omitempty"`
	PendingState            tgsrlv1.RuntimeState    `json:"pendingState,omitempty"`
	Transition              uint64                  `json:"transition,omitempty"`
	PublishedStates         map[string]uint64       `json:"publishedStates,omitempty"`
	PendingObservedAt       time.Time               `json:"pendingObservedAt,omitempty"`
	PendingDetail           string                  `json:"pendingDetail,omitempty"`
	PendingControlKey       string                  `json:"pendingControlKey,omitempty"`
	PendingControlRevision  uint64                  `json:"pendingControlRevision,omitempty"`
	PendingControlRequestID string                  `json:"pendingControlRequestId,omitempty"`
}

// Registrar is the narrow dependency used by Worker.handleDecision. Register
// durably hands off observation before the decision cursor is advanced.
type Registrar interface {
	Register(context.Context, ObservationRegistration) error
}

// BundleSource exposes materialized backend bundles during restart recovery.
type BundleSource interface {
	List(context.Context) ([]*api.Bundle, error)
}

type BundleCleaner interface {
	Cleanup(context.Context, string, uint64) error
}

type ControlMetadataRestorer interface {
	RestoreControlMetadata(context.Context) error
}

// ObservationManager owns independent, reconnecting watchers for registered
// bundles. Registration persistence is intentionally synchronous; backend
// observation and runtime publishing are not.
type ObservationManager struct {
	observer   statuswatch.Observer
	publisher  runtimepub.Publisher
	repository RegistrationRepository
	bundles    BundleSource
	retryBase  time.Duration
	retryMax   time.Duration

	mu        sync.Mutex
	repoMu    sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	running   bool
	accepting bool
	watchers  map[string]*observationWatcher
	wg        sync.WaitGroup
}

type observationWatcher struct {
	cancel context.CancelFunc
}

func (m *ObservationManager) SetBundleSource(source BundleSource) {
	m.mu.Lock()
	m.bundles = source
	m.mu.Unlock()
}

// Active reports how many per-bundle watcher goroutines are currently owned
// by the manager. It is primarily useful for shutdown health checks.
func (m *ObservationManager) Active() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.watchers)
}

func NewObservationManager(observer statuswatch.Observer, publisher runtimepub.Publisher, repository RegistrationRepository) (*ObservationManager, error) {
	if observer == nil {
		return nil, fmt.Errorf("status observer is required")
	}
	if publisher == nil {
		return nil, fmt.Errorf("runtime publisher is required")
	}
	if repository == nil {
		return nil, fmt.Errorf("registration repository is required")
	}
	return &ObservationManager{
		observer:   observer,
		publisher:  publisher,
		repository: repository,
		retryBase:  200 * time.Millisecond,
		retryMax:   5 * time.Second,
		watchers:   make(map[string]*observationWatcher),
	}, nil
}

// Run restores durable registrations and blocks until ctx is canceled. A
// registration made before Run starts is picked up during restoration.
func (m *ObservationManager) Run(ctx context.Context) error {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return fmt.Errorf("observation manager is already running")
	}
	m.ctx, m.cancel = context.WithCancel(ctx)
	m.running = true
	m.accepting = false
	bundleSource := m.bundles
	runCtx := m.ctx
	m.mu.Unlock()

	m.repoMu.Lock()
	registrations, err := m.repository.ListRegistrations()
	if err != nil {
		m.repoMu.Unlock()
		m.stopAfterStartFailure()
		return fmt.Errorf("load observation registrations: %w", err)
	}
	if bundleSource != nil {
		bundles, err := bundleSource.List(runCtx)
		if err != nil {
			m.repoMu.Unlock()
			m.stopAfterStartFailure()
			return fmt.Errorf("list materialized bundles for observation recovery: %w", err)
		}
		if restorer, ok := bundleSource.(ControlMetadataRestorer); ok {
			if err := restorer.RestoreControlMetadata(runCtx); err != nil {
				m.repoMu.Unlock()
				m.stopAfterStartFailure()
				return fmt.Errorf("restore backend control metadata: %w", err)
			}
			bundles, err = bundleSource.List(runCtx)
			if err != nil {
				m.repoMu.Unlock()
				m.stopAfterStartFailure()
				return fmt.Errorf("reload materialized bundles after control recovery: %w", err)
			}
		}
		materialized := make(map[string]*api.Bundle, len(bundles))
		for _, bundle := range bundles {
			if bundle != nil && len(bundle.RuntimeTargets) > 0 {
				materialized[bundle.Key] = bundle
			}
		}
		recovered := make([]ObservationRegistration, 0, len(materialized))
		known := make(map[string]bool, len(materialized))
		for _, registration := range registrations {
			if materialized[registration.BundleKey] != nil {
				recovered = append(recovered, registration)
				known[registration.BundleKey] = true
			}
		}
		for _, bundle := range bundles {
			if bundle == nil || known[bundle.Key] || len(bundle.RuntimeTargets) == 0 {
				continue
			}
			recovered = append(recovered, registrationFromBundle(bundle))
			known[bundle.Key] = true
		}
		registrations = recovered
		if err := m.repository.ReplaceRegistrations(registrations); err != nil {
			m.repoMu.Unlock()
			m.stopAfterStartFailure()
			return fmt.Errorf("replace recovered observation registrations: %w", err)
		}
	}
	m.repoMu.Unlock()

	// Registrations are allowed to persist while recovery runs. Once new
	// registrations may start their own watcher, reload the repository so a
	// handoff racing the earlier snapshot cannot be missed.
	m.mu.Lock()
	m.accepting = true
	m.mu.Unlock()
	m.repoMu.Lock()
	registrations, err = m.repository.ListRegistrations()
	m.repoMu.Unlock()
	if err != nil {
		m.stopAfterStartFailure()
		return fmt.Errorf("reload observation registrations: %w", err)
	}
	m.mu.Lock()
	for _, registration := range registrations {
		m.startLocked(registration.BundleKey)
	}
	m.mu.Unlock()

	<-runCtx.Done()
	m.mu.Lock()
	m.accepting = false
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
	m.mu.Lock()
	m.running = false
	m.ctx = nil
	m.cancel = nil
	m.mu.Unlock()
	return nil
}

func (m *ObservationManager) stopAfterStartFailure() {
	m.mu.Lock()
	m.accepting = false
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Unlock()
	m.wg.Wait()
	m.mu.Lock()
	m.running = false
	m.ctx = nil
	m.cancel = nil
	m.mu.Unlock()
}

func registrationFromBundle(bundle *api.Bundle) ObservationRegistration {
	bindings := make([]*tgsrlv1.Binding, 0, len(bundle.RuntimeTargets))
	actions := make([]*tgsrlv1.Action, 0, len(bundle.RuntimeTargets))
	actionResults := make([]*tgsrlv1.ActionResult, 0, len(bundle.RuntimeTargets))
	decisionID, planID := "", ""
	for _, target := range bundle.RuntimeTargets {
		if decisionID == "" {
			decisionID = target.DecisionID
		}
		if planID == "" {
			planID = target.PlanID
		}
		bindingID := target.BindingID
		if bindingID == "" {
			bindingID = target.RuntimeUnitID
		}
		generation := target.Generation
		if generation == 0 {
			generation = bundle.Generation
		}
		binding := &tgsrlv1.Binding{
			BindingId:     bindingID,
			PendingUnitId: target.RuntimeUnitID,
			RuntimeUnitId: target.RuntimeUnitID,
			SandboxId:     target.SandboxID,
			Generation:    generation,
			DeviceIds:     append([]string(nil), target.DeviceIDs...),
			Resources: &tgsrlv1.ResourceVector{
				CpuMillis:        target.CPUMillis,
				MemoryBytes:      target.MemoryBytes,
				AcceleratorUnits: target.Accelerators,
			},
		}
		bindings = append(bindings, binding)
		if target.ActionID != "" {
			actions = append(actions, &tgsrlv1.Action{
				ActionId:           target.ActionID,
				PlanId:             target.PlanID,
				TargetId:           target.RuntimeUnitID,
				SandboxId:          target.SandboxID,
				ExpectedGeneration: generation,
				Binding:            proto.Clone(binding).(*tgsrlv1.Binding),
			})
			actionResults = append(actionResults, &tgsrlv1.ActionResult{
				ActionId:           target.ActionID,
				PlanId:             target.PlanID,
				Status:             tgsrlv1.ActionResultStatus_ACTION_RESULT_STATUS_SUCCEEDED,
				ObservedGeneration: generation,
			})
		}
	}
	decision := &tgsrlv1.DecisionRecord{
		DecisionId:    decisionID,
		RunId:         bundle.SourceRunID,
		JobId:         bundle.SourceJobID,
		TraceId:       bundle.SourceTraceID,
		Generation:    bundle.Generation,
		ActionResults: actionResults,
		SelectedPlan: &tgsrlv1.PlacementPlan{
			PlanId:     planID,
			DecisionId: decisionID,
			RunId:      bundle.SourceRunID,
			JobId:      bundle.SourceJobID,
			TraceId:    bundle.SourceTraceID,
			Generation: bundle.Generation,
			Bindings:   bindings,
			Actions:    actions,
		},
	}
	run := &tgsrlv1.JobRun{RunId: bundle.SourceRunID, JobId: bundle.SourceJobID, TraceId: bundle.SourceTraceID}
	return ObservationRegistration{BundleKey: bundle.Key, Bundle: bundle, Decision: decision, JobRun: run, PublishedEventIDs: map[string]bool{}}
}

func (m *ObservationManager) Register(ctx context.Context, registration ObservationRegistration) error {
	registration, err := cloneObservationRegistration(registration)
	if err != nil {
		return err
	}
	if registration.BundleKey == "" && registration.Bundle != nil {
		registration.BundleKey = registration.Bundle.Key
	}
	if registration.BundleKey == "" || registration.Bundle == nil || registration.Decision == nil || registration.Decision.GetSelectedPlan() == nil || registration.JobRun == nil {
		return fmt.Errorf("bundle key, bundle, decision, and job run are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m.repoMu.Lock()
	existing, ok, err := m.registrationLocked(registration.BundleKey)
	restart := false
	if err == nil && !ok {
		err = m.repository.SaveRegistration(registration)
	} else if err == nil {
		switch {
		case sameRegistration(existing, registration):
			registration = existing
		case registration.Decision.GetSequence() > existing.Decision.GetSequence():
			registration.PublishedEventIDs = existing.PublishedEventIDs
			if existing.Bundle != nil && registration.Bundle != nil && existing.Bundle.Generation == registration.Bundle.Generation {
				registration.LastState = existing.LastState
				registration.PublishedStates = existing.PublishedStates
				registration.Transition = existing.Transition
			}
			err = m.repository.SaveRegistration(registration)
			restart = err == nil
		default:
			err = fmt.Errorf("registration conflicts with existing bundle identity")
		}
	}
	m.repoMu.Unlock()
	if err != nil {
		return fmt.Errorf("save observation registration %q: %w", registration.BundleKey, err)
	}

	m.mu.Lock()
	if restart {
		if watcher := m.watchers[registration.BundleKey]; watcher != nil {
			watcher.cancel()
			delete(m.watchers, registration.BundleKey)
		}
	}
	if m.running && m.accepting && m.ctx.Err() == nil {
		m.startLocked(registration.BundleKey)
	}
	m.mu.Unlock()
	return nil
}

func (m *ObservationManager) startLocked(bundleKey string) {
	if bundleKey == "" || !m.accepting || m.ctx == nil || m.ctx.Err() != nil {
		return
	}
	if _, exists := m.watchers[bundleKey]; exists {
		return
	}
	watchCtx, cancel := context.WithCancel(m.ctx)
	watcher := &observationWatcher{cancel: cancel}
	m.watchers[bundleKey] = watcher
	m.wg.Add(1)
	go m.watch(watchCtx, bundleKey, watcher)
}

func (m *ObservationManager) watch(ctx context.Context, bundleKey string, watcher *observationWatcher) {
	defer func() {
		m.mu.Lock()
		if m.watchers[bundleKey] == watcher {
			delete(m.watchers, bundleKey)
		}
		m.mu.Unlock()
		m.wg.Done()
	}()

	backoff := m.retryBase
	if backoff <= 0 {
		backoff = 200 * time.Millisecond
	}
	maxBackoff := m.retryMax
	if maxBackoff <= 0 || maxBackoff < backoff {
		maxBackoff = 5 * time.Second
	}
	for {
		if ctx.Err() != nil {
			return
		}
		registration, ok, err := m.registration(bundleKey)
		if err == nil && !ok {
			return
		}
		if err == nil {
			err = m.watchOnce(ctx, &registration)
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, bundleadapter.ErrBundleAbsent) {
			if err := m.deleteRegistration(registration); err == nil {
				return
			}
		}
		if err == nil {
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
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

func (m *ObservationManager) registration(bundleKey string) (ObservationRegistration, bool, error) {
	m.repoMu.Lock()
	defer m.repoMu.Unlock()
	return m.registrationLocked(bundleKey)
}

func (m *ObservationManager) registrationLocked(bundleKey string) (ObservationRegistration, bool, error) {
	registrations, err := m.repository.ListRegistrations()
	if err != nil {
		return ObservationRegistration{}, false, err
	}
	for _, registration := range registrations {
		if registration.BundleKey == bundleKey {
			return registration, true, nil
		}
	}
	return ObservationRegistration{}, false, nil
}

func (m *ObservationManager) watchOnce(ctx context.Context, registration *ObservationRegistration) error {
	stream, err := m.observer.Watch(ctx, statuswatch.Request{
		Bundle:   registration.Bundle,
		Bindings: registration.Decision.GetSelectedPlan().GetBindings(),
	})
	if err != nil {
		return err
	}
	if stream == nil {
		return fmt.Errorf("watch bundle %q returned a nil stream", registration.BundleKey)
	}
	for {
		snapshot, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == io.EOF {
				return io.EOF
			}
			return err
		}
		terminal, err := m.publishSnapshot(ctx, registration, snapshot)
		if err != nil {
			return err
		}
		if terminal {
			return m.deleteRegistration(*registration)
		}
	}
}

func (m *ObservationManager) publishSnapshot(ctx context.Context, registration *ObservationRegistration, snapshot *statuswatch.Snapshot) (bool, error) {
	if snapshot == nil || snapshot.ObservedGeneration < decisionGeneration(registration.Decision) {
		return false, nil
	}
	projection := statuswatch.Project(
		snapshot,
		registration.Bundle.ResourceClaim != nil ||
			registration.Bundle.ResourceClaimTemplate != nil ||
			compiler.IsHAMIGPUProfile(registration.Bundle.GPUProfile),
	)
	if !projection.Ready || !shouldPublishTransition(registration.LastState, registration.PendingState, projection.State) {
		return false, nil
	}
	if registration.PublishedEventIDs == nil {
		registration.PublishedEventIDs = make(map[string]bool)
	}
	if registration.PublishedStates == nil {
		registration.PublishedStates = make(map[string]uint64)
	}
	if registration.PendingState == tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		registration.PendingState = projection.State
		stateKey := projection.State.String()
		registration.Transition = registration.PublishedStates[stateKey] + 1
		registration.PendingObservedAt = snapshot.ObservedAt.UTC()
		if registration.PendingObservedAt.IsZero() {
			registration.PendingObservedAt = time.Now().UTC()
		}
		registration.PendingDetail = projection.Detail
		if snapshot.ControlCommitted && controlMatchesProjection(snapshot.ControlAction, projection.State) {
			registration.PendingControlKey = snapshot.ControlIdempotencyKey
			registration.PendingControlRevision = snapshot.ControlBackendRevision
			registration.PendingControlRequestID = snapshot.ControlRequestID
		}
		if err := m.saveRegistration(*registration); err != nil {
			return false, err
		}
	} else if registration.PendingState != projection.State {
		return false, nil
	} else if registration.PendingObservedAt.IsZero() {
		registration.PendingObservedAt = snapshot.ObservedAt.UTC()
		if registration.PendingObservedAt.IsZero() {
			registration.PendingObservedAt = time.Now().UTC()
		}
		registration.PendingDetail = projection.Detail
		if snapshot.ControlCommitted && controlMatchesProjection(snapshot.ControlAction, projection.State) {
			registration.PendingControlKey = snapshot.ControlIdempotencyKey
			registration.PendingControlRevision = snapshot.ControlBackendRevision
			registration.PendingControlRequestID = snapshot.ControlRequestID
		}
		if err := m.saveRegistration(*registration); err != nil {
			return false, err
		}
	}
	for _, binding := range registration.Decision.GetSelectedPlan().GetBindings() {
		if len(snapshot.AllocatedDeviceIDs) > 0 {
			binding = proto.Clone(binding).(*tgsrlv1.Binding)
			binding.DeviceIds = append([]string(nil), snapshot.AllocatedDeviceIDs...)
		}
		event := runtimepub.BuildSandboxEvent(registration.Decision, registration.JobRun, binding, projection.EventType, projection.State, registration.PendingDetail)
		if registration.PendingControlKey != "" {
			event.IdempotencyKey = registration.PendingControlKey
			event.ProviderRevision = registration.PendingControlRevision
			if registration.PendingControlRequestID != "" {
				event.Detail = registration.PendingDetail + " (request " + registration.PendingControlRequestID + ")"
			}
		}
		if (projection.State == tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED || projection.State == tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED) && snapshot.ControlCommitted {
			protocolmeta.SetRunRetirement(event, snapshot.ControlRetireRun)
		}
		baseEventID := event.GetEventId()
		if registration.Transition > 1 {
			event.EventId = fmt.Sprintf("%s:transition:%d", baseEventID, registration.Transition)
		}
		if registration.PublishedEventIDs[event.GetEventId()] {
			continue
		}
		event.OccurredAt = timestamppb.New(registration.PendingObservedAt)
		if err := m.publisher.Publish(ctx, event); err != nil {
			return false, err
		}
		registration.PublishedEventIDs[event.GetEventId()] = true
		if err := m.saveRegistration(*registration); err != nil {
			return false, err
		}
	}
	terminal := projection.State == tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED || projection.State == tgsrlv1.RuntimeState_RUNTIME_STATE_FAILED
	if terminal {
		if cleaner, ok := m.bundles.(BundleCleaner); ok {
			if err := cleaner.Cleanup(ctx, registration.BundleKey, registration.Bundle.Generation); err != nil {
				return false, fmt.Errorf("cleanup terminal bundle %q: %w", registration.BundleKey, err)
			}
		}
		return true, nil
	}
	registration.LastState = projection.State
	registration.PublishedStates[projection.State.String()] = registration.Transition
	registration.PendingState = tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN
	registration.PendingObservedAt = time.Time{}
	registration.PendingDetail = ""
	registration.PendingControlKey = ""
	registration.PendingControlRevision = 0
	registration.PendingControlRequestID = ""
	if err := m.saveRegistration(*registration); err != nil {
		return false, err
	}
	return false, nil
}

func controlMatchesProjection(action tgsrlv1.JobCommandType, state tgsrlv1.RuntimeState) bool {
	switch action {
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_PAUSE:
		return state == tgsrlv1.RuntimeState_RUNTIME_STATE_PAUSED
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_RESUME:
		return state == tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING
	case tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_STOP, tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_TERMINATE:
		return state == tgsrlv1.RuntimeState_RUNTIME_STATE_TERMINATED
	default:
		return false
	}
}

func shouldPublishTransition(last, pending, next tgsrlv1.RuntimeState) bool {
	if pending != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		return pending == next
	}
	if next == last {
		return false
	}
	// Every newly-created watch may first report admission before it reports
	// the current Job status. Once a bundle has progressed past BOUND, that
	// intermediate read is not a lifecycle regression and must not be emitted.
	if next == tgsrlv1.RuntimeState_RUNTIME_STATE_BOUND && last != tgsrlv1.RuntimeState_RUNTIME_STATE_UNKNOWN {
		return false
	}
	return true
}

func (m *ObservationManager) saveRegistration(registration ObservationRegistration) error {
	m.repoMu.Lock()
	defer m.repoMu.Unlock()
	current, ok, err := m.registrationLocked(registration.BundleKey)
	if err != nil {
		return err
	}
	if !ok || !sameRegistrationVersion(current, registration) {
		return fmt.Errorf("observation registration %q was replaced", registration.BundleKey)
	}
	return m.repository.SaveRegistration(registration)
}

func (m *ObservationManager) deleteRegistration(registration ObservationRegistration) error {
	m.repoMu.Lock()
	defer m.repoMu.Unlock()
	current, ok, err := m.registrationLocked(registration.BundleKey)
	if err != nil || !ok {
		return err
	}
	if !sameRegistrationVersion(current, registration) {
		return fmt.Errorf("observation registration %q was replaced", registration.BundleKey)
	}
	return m.repository.DeleteRegistration(registration.BundleKey)
}

func sameRegistration(left, right ObservationRegistration) bool {
	if left.BundleKey != right.BundleKey || left.Decision == nil || right.Decision == nil {
		return false
	}
	if left.Decision.GetDecisionId() != right.Decision.GetDecisionId() || decisionGeneration(left.Decision) != decisionGeneration(right.Decision) {
		return false
	}
	leftPlan, rightPlan := left.Decision.GetSelectedPlan(), right.Decision.GetSelectedPlan()
	if leftPlan == nil || rightPlan == nil || leftPlan.GetPlanId() != rightPlan.GetPlanId() {
		return false
	}
	leftSequence, rightSequence := left.Decision.GetSequence(), right.Decision.GetSequence()
	if leftSequence != 0 && rightSequence != 0 && leftSequence != rightSequence {
		return false
	}
	if left.Bundle == nil || right.Bundle == nil {
		return left.Bundle == right.Bundle
	}
	return left.Bundle.Fingerprint == right.Bundle.Fingerprint
}

func sameRegistrationVersion(left, right ObservationRegistration) bool {
	return sameRegistration(left, right) && left.Decision.GetSequence() == right.Decision.GetSequence()
}

func cloneObservationRegistration(registration ObservationRegistration) (ObservationRegistration, error) {
	bundle, err := api.CloneBundle(registration.Bundle)
	if err != nil {
		return ObservationRegistration{}, err
	}
	registration.Bundle = bundle
	if registration.Decision != nil {
		registration.Decision = proto.Clone(registration.Decision).(*tgsrlv1.DecisionRecord)
	}
	if registration.JobRun != nil {
		registration.JobRun = proto.Clone(registration.JobRun).(*tgsrlv1.JobRun)
	}
	published := make(map[string]bool, len(registration.PublishedEventIDs))
	for eventID, value := range registration.PublishedEventIDs {
		published[eventID] = value
	}
	registration.PublishedEventIDs = published
	publishedStates := make(map[string]uint64, len(registration.PublishedStates))
	for state, transition := range registration.PublishedStates {
		publishedStates[state] = transition
	}
	registration.PublishedStates = publishedStates
	return registration, nil
}
