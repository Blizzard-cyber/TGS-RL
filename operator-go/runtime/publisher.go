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
	client RuntimeClient
}

type RuntimeClient interface {
	PublishSandboxEvent(ctx context.Context, in *tgsrlv1.PublishSandboxEventRequest, opts ...grpc.CallOption) (*tgsrlv1.PublishSandboxEventResponse, error)
}

func NewRPCPublisher(client RuntimeClient) (*RPCPublisher, error) {
	if client == nil {
		return nil, fmt.Errorf("runtime client is required")
	}
	return &RPCPublisher{client: client}, nil
}

func (p *RPCPublisher) Publish(ctx context.Context, event *tgsrlv1.SandboxEvent) error {
	if event == nil {
		return fmt.Errorf("sandbox event is required")
	}
	_, err := p.client.PublishSandboxEvent(ctx, &tgsrlv1.PublishSandboxEventRequest{
		Event: proto.Clone(event).(*tgsrlv1.SandboxEvent),
	})
	return err
}

func BuildSandboxEvent(decision *tgsrlv1.DecisionRecord, jobRun *tgsrlv1.JobRun, binding *tgsrlv1.Binding, eventType tgsrlv1.SandboxEventType, state tgsrlv1.RuntimeState, detail string) *tgsrlv1.SandboxEvent {
	bindingClone := proto.Clone(binding).(*tgsrlv1.Binding)
	actionID, idempotencyKey, providerRevision := causalAction(decision, binding)
	generation := decision.GetGeneration()
	if generation == 0 {
		generation = binding.GetGeneration()
	}
	if generation == 0 {
		generation = 1
	}
	return &tgsrlv1.SandboxEvent{
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
}

func causalAction(decision *tgsrlv1.DecisionRecord, binding *tgsrlv1.Binding) (string, string, uint64) {
	if decision == nil || binding == nil {
		return "", "", 0
	}
	var actionID, key string
	for _, action := range decision.GetSelectedPlan().GetActions() {
		if action == nil {
			continue
		}
		matches := action.GetBinding().GetBindingId() == binding.GetBindingId() || action.GetSandboxId() == binding.GetSandboxId() || action.GetTargetId() == runtimeUnitID(binding)
		if matches {
			actionID, key = action.GetActionId(), action.GetIdempotencyKey()
			break
		}
	}
	var revision uint64
	for _, result := range decision.GetActionResults() {
		if result.GetActionId() != actionID {
			continue
		}
		if key == "" {
			key = result.GetIdempotencyKey()
		}
		revision = result.GetObservedRevision()
		break
	}
	return actionID, key, revision
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
