package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var ErrCheckpointNotFound = errors.New("storage: checkpoint not found")

type CheckpointStore struct {
	path string
}

func OpenCheckpointStore(root, name string) (*CheckpointStore, error) {
	if name == "" {
		return nil, errors.New("storage: checkpoint name is required")
	}
	path := filepath.Join(root, name+".checkpoint")
	if err := ensureDir(path); err != nil {
		return nil, err
	}
	return &CheckpointStore{path: path}, nil
}

func (s *CheckpointStore) Path() string { return s.path }

func (s *CheckpointStore) Save(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("storage: empty checkpoint payload")
	}
	return atomicWriteFile(s.path, payload, 0o600)
}

func (s *CheckpointStore) Load() ([]byte, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrCheckpointNotFound
		}
		return nil, fmt.Errorf("storage: read checkpoint %s: %w", s.path, err)
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: empty checkpoint", ErrCorruptJournal)
	}
	return data, nil
}
