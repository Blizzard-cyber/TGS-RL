package worker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type DeliveryRecord struct {
	DecisionID        string          `json:"decisionId"`
	Sequence          uint64          `json:"sequence"`
	Cursor            string          `json:"cursor,omitempty"`
	Phase             string          `json:"phase,omitempty"`
	PublishedEventIDs map[string]bool `json:"publishedEventIds,omitempty"`
}

type DeliveryRepository interface {
	Load() (DeliveryRecord, error)
	Save(DeliveryRecord) error
	Clear() error
}

// RegistrationRepository is the durable handoff between decision processing
// and background observation. Implementations must support more than one
// bundle at a time; a decision cursor may advance as soon as its registration
// has been saved.
type RegistrationRepository interface {
	ListRegistrations() ([]ObservationRegistration, error)
	SaveRegistration(ObservationRegistration) error
	DeleteRegistration(bundleKey string) error
}

type deliveryLedgerState struct {
	Delivery      *DeliveryRecord                    `json:"delivery,omitempty"`
	Registrations map[string]ObservationRegistration `json:"registrations,omitempty"`
}

type FileDeliveryRepository struct {
	path          string
	mu            sync.Mutex
	syncDirectory func(string) error
}

func NewFileDeliveryRepository(path string) *FileDeliveryRepository {
	return &FileDeliveryRepository{path: path, syncDirectory: syncLedgerDirectory}
}

func (r *FileDeliveryRepository) Load() (DeliveryRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.readStateLocked()
	if err != nil || state.Delivery == nil {
		return DeliveryRecord{}, err
	}
	record := *state.Delivery
	if record.PublishedEventIDs == nil {
		record.PublishedEventIDs = map[string]bool{}
	}
	return record, nil
}

func (r *FileDeliveryRepository) Save(record DeliveryRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.readStateLocked()
	if err != nil {
		return err
	}
	if record.PublishedEventIDs == nil {
		record.PublishedEventIDs = map[string]bool{}
	}
	state.Delivery = &record
	return r.writeStateLocked(state)
}

func (r *FileDeliveryRepository) Clear() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.readStateLocked()
	if err != nil {
		return err
	}
	state.Delivery = nil
	return r.writeStateLocked(state)
}

func (r *FileDeliveryRepository) ListRegistrations() ([]ObservationRegistration, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.readStateLocked()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(state.Registrations))
	for key := range state.Registrations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	registrations := make([]ObservationRegistration, 0, len(keys))
	for _, key := range keys {
		registrations = append(registrations, state.Registrations[key])
	}
	return registrations, nil
}

func (r *FileDeliveryRepository) SaveRegistration(registration ObservationRegistration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.readStateLocked()
	if err != nil {
		return err
	}
	if state.Registrations == nil {
		state.Registrations = make(map[string]ObservationRegistration)
	}
	state.Registrations[registration.BundleKey] = registration
	return r.writeStateLocked(state)
}

func (r *FileDeliveryRepository) DeleteRegistration(bundleKey string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	state, err := r.readStateLocked()
	if err != nil {
		return err
	}
	delete(state.Registrations, bundleKey)
	return r.writeStateLocked(state)
}

func (r *FileDeliveryRepository) readStateLocked() (deliveryLedgerState, error) {
	payload, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return deliveryLedgerState{}, nil
		}
		return deliveryLedgerState{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return deliveryLedgerState{}, err
	}
	if _, hasDelivery := fields["delivery"]; hasDelivery || fields["registrations"] != nil {
		var persisted persistedDeliveryLedgerState
		if err := json.Unmarshal(payload, &persisted); err != nil {
			return deliveryLedgerState{}, err
		}
		return decodeDeliveryLedgerState(persisted)
	}

	// Files written before observation registrations were introduced contain
	// a DeliveryRecord at the top level. Continue to accept that representation.
	var legacy DeliveryRecord
	if err := json.Unmarshal(payload, &legacy); err != nil {
		return deliveryLedgerState{}, err
	}
	return deliveryLedgerState{Delivery: &legacy}, nil
}

func (r *FileDeliveryRepository) writeStateLocked(state deliveryLedgerState) error {
	if state.Delivery == nil && len(state.Registrations) == 0 {
		if err := os.Remove(r.path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		return r.syncParentDirectory()
	}
	if err := os.MkdirAll(filepath.Dir(r.path), 0o755); err != nil {
		return err
	}
	persisted, err := encodeDeliveryLedgerState(state)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(persisted)
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.path); err != nil {
		return fmt.Errorf("rename delivery file: %w", err)
	}
	return r.syncParentDirectory()
}

func (r *FileDeliveryRepository) syncParentDirectory() error {
	syncDirectory := r.syncDirectory
	if syncDirectory == nil {
		syncDirectory = syncLedgerDirectory
	}
	return syncDirectory(filepath.Dir(r.path))
}

func syncLedgerDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
