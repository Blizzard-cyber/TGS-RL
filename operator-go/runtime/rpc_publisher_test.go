package runtime

import (
	"context"
	"errors"
	"testing"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"google.golang.org/grpc"
)

type recordingRuntimeClient struct {
	event *tgsrlv1.SandboxEvent
	err   error
}

type failingSchedulerClient struct{ err error }

func (c failingSchedulerClient) ObserveSandbox(context.Context, *tgsrlv1.ObserveSandboxRequest, ...grpc.CallOption) (*tgsrlv1.ObserveSandboxResponse, error) {
	return nil, c.err
}

func (c *recordingRuntimeClient) PublishSandboxEvent(_ context.Context, request *tgsrlv1.PublishSandboxEventRequest, _ ...grpc.CallOption) (*tgsrlv1.PublishSandboxEventResponse, error) {
	if c.err != nil {
		return nil, c.err
	}
	c.event = request.GetEvent()
	return &tgsrlv1.PublishSandboxEventResponse{Event: request.GetEvent()}, nil
}

func TestRPCPublisherReturnsSchedulerProjectionFailureAfterRuntimePersistence(t *testing.T) {
	runtimeClient := &recordingRuntimeClient{}
	publisher, err := NewRPCPublisher(runtimeClient, failingSchedulerClient{err: errors.New("scheduler unavailable")})
	if err != nil {
		t.Fatal(err)
	}
	event := &tgsrlv1.SandboxEvent{EventId: "event-1", SandboxId: "sandbox-1", Generation: 1, State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING}
	if err := publisher.Publish(context.Background(), event); err == nil {
		t.Fatal("scheduler projection failure must remain retryable")
	}
	if runtimeClient.event.GetEventId() != event.GetEventId() {
		t.Fatal("runtime observation was not persisted before scheduler projection")
	}
}

type recordingSchedulerClient struct {
	event *tgsrlv1.SandboxEvent
}

func (c *recordingSchedulerClient) ObserveSandbox(_ context.Context, request *tgsrlv1.ObserveSandboxRequest, _ ...grpc.CallOption) (*tgsrlv1.ObserveSandboxResponse, error) {
	c.event = request.GetEvent()
	return &tgsrlv1.ObserveSandboxResponse{Event: request.GetEvent()}, nil
}

func TestRPCPublisherPersistsRuntimeObservationBeforeSchedulerProjection(t *testing.T) {
	runtimeClient := &recordingRuntimeClient{}
	schedulerClient := &recordingSchedulerClient{}
	publisher, err := NewRPCPublisher(runtimeClient, schedulerClient)
	if err != nil {
		t.Fatal(err)
	}
	event := &tgsrlv1.SandboxEvent{EventId: "event-1", SandboxId: "sandbox-1", Generation: 1, State: tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING}
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if runtimeClient.event.GetEventId() != "event-1" || schedulerClient.event.GetEventId() != "event-1" {
		t.Fatalf("observation fanout = runtime:%+v scheduler:%+v", runtimeClient.event, schedulerClient.event)
	}

	runtimeClient.err = errors.New("runtime unavailable")
	schedulerClient.event = nil
	if err := publisher.Publish(context.Background(), event); err == nil {
		t.Fatal("runtime failure must stop scheduler projection")
	}
	if schedulerClient.event != nil {
		t.Fatal("scheduler received an observation that Runtime did not persist")
	}
}
