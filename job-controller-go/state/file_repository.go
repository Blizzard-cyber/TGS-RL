package state

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
)

const (
	snapshotFileName = "snapshot.gob"
	journalFileName  = "journal.gob"
)

type persistedEvent struct {
	Sequence uint64
	Event    *tgsrlv1.JobEvent
}

type persistedState struct {
	Jobs         map[string]*tgsrlv1.RLTrainingJob
	Runs         map[string]map[string]*tgsrlv1.JobRun
	Operations   map[string]*tgsrlv1.Operation
	Idempotency  map[string]*IdempotencyRecord
	Events       []*persistedEvent
	NextSequence uint64
}

type journalRecord struct {
	WrittenAt time.Time
	State     *persistedState
}

// FileRepository provides durable on-disk persistence over MemoryRepository.
type FileRepository struct {
	mu sync.Mutex

	memory       *MemoryRepository
	dir          string
	snapshotPath string
	journalPath  string
}

func init() {
	gob.Register(&tgsrlv1.RLTrainingJob{})
	gob.Register(&tgsrlv1.JobRun{})
	gob.Register(&tgsrlv1.Operation{})
	gob.Register(&tgsrlv1.JobEvent{})
	gob.Register(&IdempotencyRecord{})
	gob.Register(&persistedState{})
	gob.Register(&journalRecord{})
}

// NewFileRepository constructs a durable repository rooted at dir.
func NewFileRepository(dir string, options ...Option) (*FileRepository, error) {
	if dir == "" {
		return nil, errors.New("state: repository dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	memory, err := NewMemoryRepository(options...)
	if err != nil {
		return nil, err
	}
	repository := &FileRepository{
		memory:       memory,
		dir:          dir,
		snapshotPath: filepath.Join(dir, snapshotFileName),
		journalPath:  filepath.Join(dir, journalFileName),
	}
	if err := repository.restore(); err != nil {
		return nil, err
	}
	return repository, nil
}

// View runs fn against the durable repository state.
func (r *FileRepository) View(fn func(Query) error) error {
	return r.memory.View(fn)
}

// Watch streams matching events from durable state.
func (r *FileRepository) Watch(jobID, runID string, afterSequence uint64, afterEventID string) (<-chan *tgsrlv1.JobEvent, func(), error) {
	return r.memory.Watch(jobID, runID, afterSequence, afterEventID)
}

// Update atomically mutates state and persists a snapshot+journal record.
func (r *FileRepository) Update(fn func(Store) error) error {
	if fn == nil {
		return errors.New("state: nil update callback")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Mutate an isolated copy first. Readers keep observing the last durable
	// state while the next state is encoded and flushed to disk.
	r.memory.mu.RLock()
	working := &MemoryRepository{
		clock:         r.memory.clock,
		jobs:          cloneJobMap(r.memory.jobs),
		runs:          cloneRunMap(r.memory.runs),
		operations:    cloneOperationMap(r.memory.operations),
		idempotency:   cloneIdempotencyMap(r.memory.idempotency),
		events:        cloneEventEntries(r.memory.events),
		watchers:      make(map[uint64]*watcher),
		nextSequence:  r.memory.nextSequence,
		nextWatcherID: r.memory.nextWatcherID,
	}
	previousSequence := r.memory.nextSequence
	r.memory.mu.RUnlock()

	store := &memoryStore{memoryQuery: memoryQuery{repository: working}}
	if err := fn(store); err != nil {
		return err
	}
	snapshot := captureRepository(working)
	if err := r.persist(snapshot); err != nil {
		return err
	}

	// Swap only after durable persistence succeeds. Watch subscriptions remain
	// attached to the live repository and receive newly committed events.
	r.memory.mu.Lock()
	r.memory.jobs = working.jobs
	r.memory.runs = working.runs
	r.memory.operations = working.operations
	r.memory.idempotency = working.idempotency
	r.memory.events = working.events
	r.memory.nextSequence = working.nextSequence
	deliveries := make([]delivery, 0)
	for _, entry := range working.events {
		if entry.sequence <= previousSequence {
			continue
		}
		for watcherID, subscriber := range r.memory.watchers {
			if matchesTarget(entry.event, subscriber.jobID, subscriber.runID) {
				deliveries = append(deliveries, delivery{watcherID: watcherID, event: cloneEvent(entry.event)})
			}
		}
	}
	for _, item := range deliveries {
		r.memory.deliverLocked(item)
	}
	r.memory.mu.Unlock()
	return nil
}

func (r *FileRepository) restore() error {
	stateValue, err := r.loadLatestState()
	if err != nil {
		return err
	}
	if stateValue == nil {
		return nil
	}

	r.memory.mu.Lock()
	defer r.memory.mu.Unlock()
	r.memory.jobs = cloneJobMap(stateValue.Jobs)
	r.memory.runs = cloneRunMap(stateValue.Runs)
	r.memory.operations = cloneOperationMap(stateValue.Operations)
	r.memory.idempotency = cloneIdempotencyMap(stateValue.Idempotency)
	r.memory.events = make([]*eventEntry, 0, len(stateValue.Events))
	for _, entry := range stateValue.Events {
		r.memory.events = append(r.memory.events, &eventEntry{
			sequence: entry.Sequence,
			event:    cloneEvent(entry.Event),
		})
	}
	r.memory.nextSequence = stateValue.NextSequence
	return nil
}

func (r *FileRepository) loadLatestState() (*persistedState, error) {
	journalState, err := readJournalFile(r.journalPath)
	if err != nil {
		return nil, err
	}
	if journalState != nil {
		return journalState, nil
	}
	return readSnapshotFile(r.snapshotPath)
}

func (r *FileRepository) captureLocked() *persistedState {
	return captureRepository(r.memory)
}

func captureRepository(repository *MemoryRepository) *persistedState {
	stateValue := &persistedState{
		Jobs:         cloneJobMap(repository.jobs),
		Runs:         cloneRunMap(repository.runs),
		Operations:   cloneOperationMap(repository.operations),
		Idempotency:  cloneIdempotencyMap(repository.idempotency),
		Events:       make([]*persistedEvent, 0, len(repository.events)),
		NextSequence: repository.nextSequence,
	}
	for _, entry := range repository.events {
		stateValue.Events = append(stateValue.Events, &persistedEvent{
			Sequence: entry.sequence,
			Event:    cloneEvent(entry.event),
		})
	}
	return stateValue
}

func (r *FileRepository) persist(stateValue *persistedState) error {
	// The journal is the authority. Replacing one full-state record atomically
	// avoids torn append tails while retaining the snapshot as a read fallback.
	if err := writeJournalFile(r.journalPath, &journalRecord{
		WrittenAt: time.Now().UTC(),
		State:     stateValue,
	}); err != nil {
		return err
	}
	_ = writeSnapshotFile(r.snapshotPath, stateValue)
	return nil
}

func writeSnapshotFile(path string, stateValue *persistedState) error {
	return atomicWriteGob(path, stateValue, 0o644)
}

func writeJournalFile(path string, record *journalRecord) error {
	return atomicWriteGob(path, record, 0o644)
}

func atomicWriteGob(path string, value any, mode os.FileMode) error {
	if err := ensureParentDir(path); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("state: create temp file for %s: %w", path, err)
	}
	tempPath := temp.Name()
	cleanup := true
	defer func() {
		_ = temp.Close()
		if cleanup {
			_ = os.Remove(tempPath)
		}
	}()

	if err := temp.Chmod(mode); err != nil {
		return fmt.Errorf("state: chmod temp file for %s: %w", path, err)
	}
	if err := gob.NewEncoder(temp).Encode(value); err != nil {
		return fmt.Errorf("state: encode temp file for %s: %w", path, err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("state: sync temp file for %s: %w", path, err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("state: close temp file for %s: %w", path, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("state: rename temp file into %s: %w", path, err)
	}
	cleanup = false
	if err := syncDir(dir); err != nil {
		return err
	}
	return nil
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("state: create dir %s: %w", dir, err)
	}
	return syncDir(dir)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("state: open dir %s: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("state: sync dir %s: %w", path, err)
	}
	return nil
}

func readSnapshotFile(path string) (*persistedState, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var stateValue persistedState
	if err := gob.NewDecoder(file).Decode(&stateValue); err != nil {
		return nil, err
	}
	return &stateValue, nil
}

func readJournalFile(path string) (*persistedState, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var latest *persistedState
	decoder := gob.NewDecoder(file)
	for {
		var record journalRecord
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			return latest, nil
		}
		if err != nil {
			return nil, err
		}
		latest = record.State
	}
}

func cloneJobMap(input map[string]*tgsrlv1.RLTrainingJob) map[string]*tgsrlv1.RLTrainingJob {
	output := make(map[string]*tgsrlv1.RLTrainingJob, len(input))
	for key, value := range input {
		output[key] = cloneJob(value)
	}
	return output
}

func cloneRunMap(input map[string]map[string]*tgsrlv1.JobRun) map[string]map[string]*tgsrlv1.JobRun {
	output := make(map[string]map[string]*tgsrlv1.JobRun, len(input))
	for jobID, runs := range input {
		output[jobID] = make(map[string]*tgsrlv1.JobRun, len(runs))
		for runID, run := range runs {
			output[jobID][runID] = cloneRun(run)
		}
	}
	return output
}

func cloneOperationMap(input map[string]*tgsrlv1.Operation) map[string]*tgsrlv1.Operation {
	output := make(map[string]*tgsrlv1.Operation, len(input))
	for key, value := range input {
		output[key] = cloneOperation(value)
	}
	return output
}

func cloneIdempotencyMap(input map[string]*IdempotencyRecord) map[string]*IdempotencyRecord {
	output := make(map[string]*IdempotencyRecord, len(input))
	for key, value := range input {
		output[key] = cloneIdempotency(value)
	}
	return output
}

func cloneEventEntries(input []*eventEntry) []*eventEntry {
	output := make([]*eventEntry, 0, len(input))
	for _, entry := range input {
		output = append(output, &eventEntry{sequence: entry.sequence, event: cloneEvent(entry.event)})
	}
	return output
}
