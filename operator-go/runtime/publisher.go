package runtime

import (
	"context"
	"fmt"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Publisher interface {
	Publish(ctx context.Context, event *tgsrlv1.SandboxEvent) error
}

type RPCPublisher struct {
	runtime   RuntimeClient
	scheduler SchedulerClient
}

type RuntimeClient interface {
	PublishSandboxEvent(ctx context.Context, in *tgsrlv1.PublishSandboxEventRequest, opts ...grpc.CallOption) (*tgsrlv1.PublishSandboxEventResponse, error)
}

type SchedulerClient interface {
	ObserveSandbox(ctx context.Context, in *tgsrlv1.ObserveSandboxRequest, opts ...grpc.CallOption) (*tgsrlv1.ObserveSandboxResponse, error)
}

func NewRPCPublisher(runtimeClient RuntimeClient, schedulerClient SchedulerClient) (*RPCPublisher, error) {
	if runtimeClient == nil || schedulerClient == nil {
		return nil, fmt.Errorf("runtime and scheduler observation clients are required")
	}
	return &RPCPublisher{runtime: runtimeClient, scheduler: schedulerClient}, nil
}

func (p *RPCPublisher) Publish(ctx context.Context, event *tgsrlv1.SandboxEvent) error {
	if event == nil {
		return fmt.Errorf("sandbox event is required")
	}
	stored, err := p.runtime.PublishSandboxEvent(ctx, &tgsrlv1.PublishSandboxEventRequest{
		Event: proto.Clone(event).(*tgsrlv1.SandboxEvent),
	})
	if err != nil {
		return err
	}
	_, err = p.scheduler.ObserveSandbox(ctx, &tgsrlv1.ObserveSandboxRequest{
		Event: proto.Clone(stored.GetEvent()).(*tgsrlv1.SandboxEvent),
	})
	return err
}

func BuildSandboxEvent(decision *tgsrlv1.DecisionRecord, jobRun *tgsrlv1.JobRun, binding *tgsrlv1.Binding, eventType tgsrlv1.SandboxEventType, state tgsrlv1.RuntimeState, detail string) *tgsrlv1.SandboxEvent {
	bindingClone := proto.Clone(binding).(*tgsrlv1.Binding)
	action, providerRevision := causalAction(decision, binding)
	actionID := ""
	idempotencyKey := ""
	if action != nil {
		actionID = action.GetActionId()
		idempotencyKey = action.GetIdempotencyKey()
	}
	// The binding identifies the concrete runtime incarnation being observed.
	// A decision may carry the generation of the planning transaction, which
	// can differ from a replacement binding during rebind/recreate.
	generation := binding.GetGeneration()
	if generation == 0 {
		generation = decision.GetGeneration()
	}
	if generation == 0 {
		generation = 1
	}
	event := &tgsrlv1.SandboxEvent{
		EventId:          fmt.Sprintf("%s:%s:%d:%s", decision.GetDecisionId(), eventType.String(), generation, binding.GetBindingId()),
		EventType:        eventType,
		SandboxId:        sandboxID(binding),
		RunId:            jobRun.GetRunId(),
		JobId:            jobRun.GetJobId(),
		TraceId:          jobRun.GetTraceId(),
		Generation:       generation,
		State:            state,
		Binding:          bindingClone,
		Detail:           detail,
		OccurredAt:       timestamppb.New(time.Now().UTC()),
		DataKind:         decision.GetDataKind(),
		RuntimeUnitId:    runtimeUnitID(binding),
		DecisionId:       decision.GetDecisionId(),
		PlanId:           decision.GetSelectedPlan().GetPlanId(),
		ActionId:         actionID,
		ProviderRevision: providerRevision,
		IdempotencyKey:   idempotencyKey,
	}
	applyObservedActionState(event, action, binding)
	return event
}

func causalAction(decision *tgsrlv1.DecisionRecord, binding *tgsrlv1.Binding) (*tgsrlv1.Action, uint64) {
	if decision == nil || binding == nil {
		return nil, 0
	}
	var selected *tgsrlv1.Action
	for _, action := range decision.GetSelectedPlan().GetActions() {
		if action == nil {
			continue
		}
		matches := action.GetBinding().GetBindingId() == binding.GetBindingId() || action.GetSandboxId() == binding.GetSandboxId() || action.GetTargetId() == runtimeUnitID(binding)
		if matches {
			selected = action
			break
		}
	}
	var revision uint64
	if selected == nil {
		return nil, 0
	}
	for _, result := range decision.GetActionResults() {
		if result.GetActionId() != selected.GetActionId() {
			continue
		}
		if selected.GetIdempotencyKey() == "" && result.GetIdempotencyKey() != "" {
			selected = proto.Clone(selected).(*tgsrlv1.Action)
			selected.IdempotencyKey = result.GetIdempotencyKey()
		}
		revision = result.GetObservedRevision()
		break
	}
	return selected, revision
}

func applyObservedActionState(event *tgsrlv1.SandboxEvent, action *tgsrlv1.Action, binding *tgsrlv1.Binding) {
	if event == nil {
		return
	}
	switch action.GetActionType() {
	case tgsrlv1.ActionType_ACTION_TYPE_BIND:
		share, priority, offloaded := binding.GetResources().GetAcceleratorUnits(), action.GetPriority(), false
		event.Share, event.Priority, event.Offloaded = &share, &priority, &offloaded
	case tgsrlv1.ActionType_ACTION_TYPE_SET_SHARE:
		share := action.GetShare()
		event.Share = &share
	case tgsrlv1.ActionType_ACTION_TYPE_SET_PRIORITY:
		priority := action.GetPriority()
		event.Priority = &priority
	case tgsrlv1.ActionType_ACTION_TYPE_RESUME:
		offloaded := false
		event.Offloaded = &offloaded
	case tgsrlv1.ActionType_ACTION_TYPE_OFFLOAD:
		offloaded := true
		event.Offloaded = &offloaded
	}
}

func runtimeUnitID(binding *tgsrlv1.Binding) string {
	if binding.GetRuntimeUnitId() != "" {
		return binding.GetRuntimeUnitId()
	}
	return binding.GetPendingUnitId()
}

func sandboxID(binding *tgsrlv1.Binding) string {
	if binding.GetSandboxId() != "" {
		return binding.GetSandboxId()
	}
	if binding.GetBindingId() != "" {
		return "sandbox-" + binding.GetBindingId()
	}
	return "sandbox-unknown"
}
