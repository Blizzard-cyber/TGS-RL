package controller

import (
	"errors"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/runtimeclient"
	"github.com/Blizzard-cyber/TGS-RL/job-controller-go/state"
)

// Config defines controller dependencies.
type Config struct {
	Repository state.Repository
	Runtime    runtimeclient.Driver
	Clock      state.Clock
}

// Controller owns deterministic in-memory job lifecycle management.
type Controller struct {
	repository state.Repository
	runtime    runtimeclient.Driver
	clock      state.Clock

	inflightMu sync.Mutex
	inflight   map[string]*inflightCall
}

type inflightCall struct {
	done   chan struct{}
	result any
	err    error
}

type prepareResult struct {
	diagnostics []string
	err         error
}

type admitPlan struct {
	job       *tgsrlv1.RLTrainingJob
	run       *tgsrlv1.JobRun
	operation *tgsrlv1.Operation
}

type commandPlan struct {
	job       *tgsrlv1.RLTrainingJob
	run       *tgsrlv1.JobRun
	operation *tgsrlv1.Operation
	eventType tgsrlv1.JobEventType
	command   tgsrlv1.JobCommandType
	isRetry   bool
	stableJob *tgsrlv1.RLTrainingJob
	stableRun *tgsrlv1.JobRun
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// New constructs a Controller.
func New(config Config) (*Controller, error) {
	if config.Repository == nil {
		return nil, errors.New("controller: repository is required")
	}
	if config.Runtime == nil {
		return nil, errors.New("controller: runtime driver is required")
	}
	if config.Clock == nil {
		config.Clock = wallClock{}
	}
	return &Controller{
		repository: config.Repository,
		runtime:    config.Runtime,
		clock:      config.Clock,
		inflight:   make(map[string]*inflightCall),
	}, nil
}
