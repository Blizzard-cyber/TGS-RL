package service

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/controller"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/runtimeclient"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testBufferSize = 1 << 20

func TestJobControlServiceBufconn(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 27, 11, 0, 0, 0, time.UTC)
	repository, err := state.NewMemoryRepository(state.WithClock(state.ClockFunc(func() time.Time { return now })))
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	engine, err := controller.New(controller.Config{
		Repository: repository,
		Runtime:    runtimeclient.NewFakeDriver(),
		Clock:      state.ClockFunc(func() time.Time { return now }),
	})
	if err != nil {
		t.Fatalf("controller.New() error = %v", err)
	}
	implementation, err := New(Config{Controller: engine})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client, cleanup := startTestServer(t, implementation)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := client.CreateJob(ctx, &tgsrlv1.CreateJobRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateJob(nil) code = %s, want InvalidArgument", status.Code(err))
	}

	job := serviceJob(now)
	created, err := client.CreateJob(ctx, &tgsrlv1.CreateJobRequest{
		Job:            job,
		RequestId:      "req-create",
		IdempotencyKey: "idem-create",
	})
	if err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	jobID := created.GetJob().GetJobId()
	if jobID == "" {
		t.Fatal("CreateJob() returned empty job id")
	}

	stream, err := client.WatchJobEvents(ctx, &tgsrlv1.WatchJobEventsRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("WatchJobEvents() error = %v", err)
	}

	admitted, err := client.AdmitJob(ctx, &tgsrlv1.AdmitJobRequest{
		JobId:          jobID,
		RequestId:      "req-admit",
		IdempotencyKey: "idem-admit",
	})
	if err != nil {
		t.Fatalf("AdmitJob() error = %v", err)
	}
	if admitted.GetJob().GetState() != tgsrlv1.JobState_JOB_STATE_PENDING {
		t.Fatalf("AdmitJob() state = %s, want PENDING", admitted.GetJob().GetState())
	}

	update, err := stream.Recv()
	if err != nil {
		t.Fatalf("WatchJobEvents().Recv() error = %v", err)
	}
	if update.GetEvent().GetJobId() != jobID {
		t.Fatalf("watch event job_id = %q, want %q", update.GetEvent().GetJobId(), jobID)
	}
	if update.GetCursor() != update.GetEvent().GetCursor() {
		t.Fatalf("watch cursor = %q, want %q", update.GetCursor(), update.GetEvent().GetCursor())
	}

	runs, err := client.ListJobRuns(ctx, &tgsrlv1.ListJobRunsRequest{JobId: jobID})
	if err != nil {
		t.Fatalf("ListJobRuns() error = %v", err)
	}
	if len(runs.GetRuns()) != 1 {
		t.Fatalf("ListJobRuns() len = %d, want 1", len(runs.GetRuns()))
	}
	runID := runs.GetRuns()[0].GetRunId()

	started, err := client.ApplyJobCommand(ctx, &tgsrlv1.ApplyJobCommandRequest{
		JobId:          jobID,
		RunId:          runID,
		Command:        tgsrlv1.JobCommandType_JOB_COMMAND_TYPE_START,
		Actor:          "tester",
		RequestId:      "req-start",
		IdempotencyKey: "idem-start",
	})
	if err != nil {
		t.Fatalf("ApplyJobCommand(start) error = %v", err)
	}
	if started.GetRun().GetRunState() != tgsrlv1.JobRunState_JOB_RUN_STATE_STARTING {
		t.Fatalf("ApplyJobCommand(start) run_state = %s, want STARTING", started.GetRun().GetRunState())
	}
	if started.GetOperation().GetState() != tgsrlv1.OperationState_OPERATION_STATE_RUNNING {
		t.Fatalf("ApplyJobCommand(start) operation state = %s, want RUNNING", started.GetOperation().GetState())
	}

	component, err := client.ReportComponentStatus(ctx, &tgsrlv1.ReportComponentStatusRequest{ComponentStatus: &tgsrlv1.ComponentStatus{
		Component: "runtime",
		Health:    tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY,
		Detail:    "running",
		Source:    "runtime-observation",
		Revision:  1,
		ObservedAt: func() *timestamppb.Timestamp {
			return timestamppb.New(now.Add(time.Second))
		}(),
		Annotations: map[string]string{
			"runtime.state": tgsrlv1.RuntimeState_RUNTIME_STATE_RUNNING.String(),
		},
		JobId:    jobID,
		RunId:    runID,
		TraceId:  started.GetRun().GetTraceId(),
		DataKind: started.GetRun().GetDataKind(),
	}})
	if err != nil {
		t.Fatalf("ReportComponentStatus() error = %v", err)
	}
	if component.GetComponentStatus().GetHealth() != tgsrlv1.ComponentHealth_COMPONENT_HEALTH_HEALTHY {
		t.Fatalf("aggregate health = %s, want HEALTHY", component.GetComponentStatus().GetHealth())
	}

	events, err := client.ListJobEvents(ctx, &tgsrlv1.ListJobEventsRequest{JobId: jobID, Limit: 20})
	if err != nil {
		t.Fatalf("ListJobEvents() error = %v", err)
	}
	if len(events.GetEvents()) < 5 {
		t.Fatalf("ListJobEvents() len = %d, want at least 5", len(events.GetEvents()))
	}
	firstPage, err := client.ListJobEvents(ctx, &tgsrlv1.ListJobEventsRequest{JobId: jobID, Limit: 1})
	if err != nil {
		t.Fatalf("ListJobEvents(first page) error = %v", err)
	}
	if len(firstPage.GetEvents()) != 1 || firstPage.GetNextPageToken() == "" {
		t.Fatalf("ListJobEvents(first page) = %+v, want one event plus next token", firstPage)
	}
	secondPage, err := client.ListJobEvents(ctx, &tgsrlv1.ListJobEventsRequest{
		JobId:     jobID,
		Limit:     1,
		PageToken: firstPage.GetNextPageToken(),
	})
	if err != nil {
		t.Fatalf("ListJobEvents(second page) error = %v", err)
	}
	if len(secondPage.GetEvents()) != 1 {
		t.Fatalf("ListJobEvents(second page) len = %d, want 1", len(secondPage.GetEvents()))
	}
	if firstPage.GetEvents()[0].GetEventId() == secondPage.GetEvents()[0].GetEventId() {
		t.Fatal("ListJobEvents(page_token) returned the same event twice")
	}
	if _, err := client.ListJobEvents(ctx, &tgsrlv1.ListJobEventsRequest{
		JobId:         jobID,
		PageToken:     firstPage.GetNextPageToken(),
		AfterSequence: 1,
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ListJobEvents(page_token+after_sequence) code = %s, want InvalidArgument", status.Code(err))
	}

	gotOperation, err := client.GetOperation(ctx, &tgsrlv1.GetOperationRequest{OperationId: started.GetOperation().GetOperationId()})
	if err != nil {
		t.Fatalf("GetOperation() error = %v", err)
	}
	if gotOperation.GetOperation().GetOperationId() != started.GetOperation().GetOperationId() {
		t.Fatal("GetOperation() returned wrong operation")
	}
}

func TestWatchJobEventsOverflowReturnsRecoverableError(t *testing.T) {
	t.Parallel()

	repository, implementation := newWatchTestServer(t)
	sendStarted := make(chan struct{})
	releaseSend := make(chan struct{})
	handlerDone := make(chan struct{}, 2)
	var firstSend sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSend) }) }
	defer release()
	client, cleanup := startTestServerWithOptions(t, implementation, grpc.StreamInterceptor(func(
		srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		if info.FullMethod != tgsrlv1.JobControlService_WatchJobEvents_FullMethodName {
			return handler(srv, stream)
		}
		err := handler(srv, &sendBlockingServerStream{
			ServerStream: stream,
			beforeFirstSend: func() {
				firstSend.Do(func() { close(sendStarted) })
				<-releaseSend
			},
		})
		handlerDone <- struct{}{}
		return err
	}))
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := client.WatchJobEvents(ctx, &tgsrlv1.WatchJobEventsRequest{JobId: "job-1"})
	if err != nil {
		t.Fatalf("WatchJobEvents() error = %v", err)
	}

	appendServiceEvents(t, repository, 1)
	select {
	case <-sendStarted:
	case <-time.After(time.Second):
		t.Fatal("WatchJobEvents() did not start sending")
	}

	// The first Send is blocked. More than the maximum possible free watch
	// slots forces overflow without relying on gRPC transport buffering.
	updateDone := make(chan struct{})
	updateErrors := make(chan error, 1)
	go func() {
		updateErrors <- appendServiceEventsError(repository, 34)
		close(updateDone)
	}()
	select {
	case <-updateDone:
		if err := <-updateErrors; err != nil {
			t.Fatalf("repository Update error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("repository Update blocked on the slow gRPC consumer")
	}
	select {
	case <-handlerDone:
		t.Fatal("WatchJobEvents() handler terminated while Send was blocked")
	case <-time.After(25 * time.Millisecond):
	}
	release()

	responses := make([]*tgsrlv1.WatchJobEventsResponse, 0, 33)
	var watchErr error
	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			if status.Code(recvErr) != codes.ResourceExhausted {
				t.Fatalf("WatchJobEvents().Recv() code = %s, want ResourceExhausted", status.Code(recvErr))
			}
			watchErr = recvErr
			break
		}
		responses = append(responses, response)
	}
	if len(responses) < 33 || len(responses) > 34 {
		t.Fatalf("WatchJobEvents() sent %d responses, want 33 or 34 before overflow", len(responses))
	}
	for index, response := range responses {
		want := uint64(index + 1)
		if response.GetEvent().GetSequence() != want {
			t.Fatalf("response[%d] sequence = %d, want %d", index, response.GetEvent().GetSequence(), want)
		}
		if response.GetCursor() != strconv.FormatUint(want, 10) {
			t.Fatalf("response[%d] cursor = %q, want %d", index, response.GetCursor(), want)
		}
	}
	lastCursor := responses[len(responses)-1].GetCursor()
	var errorInfo *errdetails.ErrorInfo
	for _, detail := range status.Convert(watchErr).Details() {
		if typed, ok := detail.(*errdetails.ErrorInfo); ok {
			errorInfo = typed
		}
	}
	if errorInfo == nil {
		t.Fatal("WatchJobEvents() error has no ErrorInfo detail")
	}
	if errorInfo.GetReason() != watchConsumerLaggedReason {
		t.Fatalf("ErrorInfo reason = %q, want %q", errorInfo.GetReason(), watchConsumerLaggedReason)
	}
	if errorInfo.GetMetadata()["last_cursor"] != lastCursor {
		t.Fatalf("ErrorInfo last_cursor = %q, want %q", errorInfo.GetMetadata()["last_cursor"], lastCursor)
	}
	if errorInfo.GetMetadata()["retry_after_sequence"] != lastCursor {
		t.Fatalf("ErrorInfo retry_after_sequence = %q, want %q", errorInfo.GetMetadata()["retry_after_sequence"], lastCursor)
	}
	trailer := stream.Trailer()
	if got := trailer.Get(watchTerminationTrailer); len(got) != 1 || got[0] != string(state.WatchTerminationConsumerLagged) {
		t.Fatalf("termination trailer = %v, want %q", got, state.WatchTerminationConsumerLagged)
	}
	if got := trailer.Get(watchLastCursorTrailer); len(got) != 1 || got[0] != lastCursor {
		t.Fatalf("last cursor trailer = %v, want %q", got, lastCursor)
	}
	if got := trailer.Get(watchRetryAfterSequenceTrailer); len(got) != 1 || got[0] != lastCursor {
		t.Fatalf("retry sequence trailer = %v, want %q", got, lastCursor)
	}

	lastSequence, err := strconv.ParseUint(lastCursor, 10, 64)
	if err != nil {
		t.Fatalf("ParseUint(last cursor) error = %v", err)
	}
	resumed, err := client.WatchJobEvents(ctx, &tgsrlv1.WatchJobEventsRequest{
		JobId:         "job-1",
		AfterSequence: lastSequence,
	})
	if err != nil {
		t.Fatalf("WatchJobEvents(resume) error = %v", err)
	}
	recovered, err := resumed.Recv()
	if err != nil {
		t.Fatalf("WatchJobEvents(resume).Recv() error = %v", err)
	}
	if recovered.GetEvent().GetSequence() != lastSequence+1 {
		t.Fatalf("WatchJobEvents(resume) sequence = %d, want %d", recovered.GetEvent().GetSequence(), lastSequence+1)
	}
}

func TestWatchJobEventsContextCancellationIsNotOverflow(t *testing.T) {
	t.Parallel()

	_, implementation := newWatchTestServer(t)
	stream := newBlockingWatchStream()
	stream.blockSend = false
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- implementation.WatchJobEvents(&tgsrlv1.WatchJobEventsRequest{JobId: "job-1"}, stream)
	}()
	stream.cancel()

	select {
	case err := <-watchDone:
		if err != nil {
			t.Fatalf("WatchJobEvents() cancellation error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WatchJobEvents() did not stop after context cancellation")
	}
}

type blockingWatchStream struct {
	grpc.ServerStream

	ctx         context.Context
	cancel      context.CancelFunc
	sendStarted chan struct{}
	releaseSend chan struct{}
	startOnce   sync.Once
	blockSend   bool

	mu        sync.Mutex
	responses []*tgsrlv1.WatchJobEventsResponse
	trailer   metadata.MD
}

func newBlockingWatchStream() *blockingWatchStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &blockingWatchStream{
		ctx:         ctx,
		cancel:      cancel,
		sendStarted: make(chan struct{}),
		releaseSend: make(chan struct{}),
		blockSend:   true,
	}
}

func (s *blockingWatchStream) Context() context.Context {
	return s.ctx
}

func (s *blockingWatchStream) SetTrailer(trailer metadata.MD) {
	s.mu.Lock()
	s.trailer = metadata.Join(s.trailer, trailer)
	s.mu.Unlock()
}

func (s *blockingWatchStream) Send(response *tgsrlv1.WatchJobEventsResponse) error {
	s.startOnce.Do(func() { close(s.sendStarted) })
	if s.blockSend {
		<-s.releaseSend
	}
	s.mu.Lock()
	s.responses = append(s.responses, response)
	s.mu.Unlock()
	return nil
}

type sendBlockingServerStream struct {
	grpc.ServerStream
	beforeFirstSend func()
	once            sync.Once
}

func (s *sendBlockingServerStream) SendMsg(message any) error {
	s.once.Do(s.beforeFirstSend)
	return s.ServerStream.SendMsg(message)
}

func newWatchTestServer(t *testing.T) (*state.MemoryRepository, *Server) {
	t.Helper()
	repository, err := state.NewMemoryRepository()
	if err != nil {
		t.Fatalf("NewMemoryRepository() error = %v", err)
	}
	engine, err := controller.New(controller.Config{
		Repository: repository,
		Runtime:    runtimeclient.NewFakeDriver(),
	})
	if err != nil {
		t.Fatalf("controller.New() error = %v", err)
	}
	implementation, err := New(Config{Controller: engine})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return repository, implementation
}

func appendServiceEvents(t *testing.T, repository *state.MemoryRepository, count int) {
	t.Helper()
	if err := appendServiceEventsError(repository, count); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
}

func appendServiceEventsError(repository *state.MemoryRepository, count int) error {
	return repository.Update(func(store state.Store) error {
		for index := 0; index < count; index++ {
			store.AppendEvent(&tgsrlv1.JobEvent{JobId: "job-1"})
		}
		return nil
	})
}

func startTestServer(t *testing.T, implementation tgsrlv1.JobControlServiceServer) (tgsrlv1.JobControlServiceClient, func()) {
	t.Helper()
	return startTestServerWithOptions(t, implementation)
}

func startTestServerWithOptions(
	t *testing.T,
	implementation tgsrlv1.JobControlServiceServer,
	options ...grpc.ServerOption,
) (tgsrlv1.JobControlServiceClient, func()) {
	t.Helper()
	listener := bufconn.Listen(testBufferSize)
	server := grpc.NewServer(options...)
	tgsrlv1.RegisterJobControlServiceServer(server, implementation)
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.Serve(listener)
	}()
	connection, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	cleanup := func() {
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
		select {
		case err := <-serveErrors:
			if err != nil && err != grpc.ErrServerStopped {
				t.Errorf("server.Serve() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	}
	return tgsrlv1.NewJobControlServiceClient(connection), cleanup
}

func serviceJob(now time.Time) *tgsrlv1.RLTrainingJob {
	return &tgsrlv1.RLTrainingJob{
		DisplayName:     "service-job",
		ProtocolVersion: "v0.3",
		Algorithm:       "ppo",
		Runtime: &tgsrlv1.FrameworkRuntimeSpec{
			Framework:               "pytorch",
			FrameworkVersion:        "2.4.0",
			ExecutionBackend:        "kubernetes",
			ExecutionBackendVersion: "1.30.0",
			Trainer:                 "torchtune",
			TrainerVersion:          "0.5.0",
			RolloutEngine:           "ray",
			RolloutEngineVersion:    "2.20.0",
			ImageDigest:             "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		ExecutionContract: &tgsrlv1.ExecutionContract{
			ContractId: "contract-1",
			Version:    "1.0.0",
			PhaseGraph: &tgsrlv1.PhaseGraph{
				Phases: []*tgsrlv1.Phase{{
					PhaseId:     "phase-1",
					DisplayName: "Train",
					Kind:        tgsrlv1.PhaseKind_PHASE_KIND_ACTOR,
					Parallelism: 1,
					MaxAttempts: 1,
				}},
				EntryPhaseIds: []string{"phase-1"},
			},
		},
		ResourcesPerUnit: &tgsrlv1.ResourceVector{CpuMillis: 1000, MemoryBytes: 4 << 30, AcceleratorUnits: 1},
		DesiredUnits:     1,
		RolloutMode:      tgsrlv1.RolloutMode_ROLLOUT_MODE_SYNC,
		PolicyRef:        "policy-1",
		DataKind:         tgsrlv1.DataKind_DATA_KIND_SYNTHETIC,
	}
}
