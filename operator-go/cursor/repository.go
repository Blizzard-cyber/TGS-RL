package cursor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Cursor struct {
	DecisionID string `json:"decisionId"`
	Sequence   uint64 `json:"sequence"`
	Cursor     string `json:"cursor,omitempty"`
}

type Repository interface {
	Load() (Cursor, error)
	Save(Cursor) error
}

type FileRepository struct {
	path string
	mu   sync.Mutex
}

func NewFileRepository(path string) *FileRepository {
	return &FileRepository{path: path}
}

func (r *FileRepository) Load() (Cursor, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	payload, err := os.ReadFile(r.path)
	if err != nil {
		if os.IsNotExist(err) {
			return Cursor{}, nil
		}
		return Cursor{}, err
	}
	var cursor Cursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return Cursor{}, err
	}
	return cursor, nil
}

func (r *FileRepository) Save(cursor Cursor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	directoryPath := filepath.Dir(r.path)
	if err := os.MkdirAll(directoryPath, 0o700); err != nil {
		return err
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directoryPath, filepath.Base(r.path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, r.path); err != nil {
		return fmt.Errorf("rename cursor file: %w", err)
	}
	committed = true
	directory, err := os.Open(directoryPath)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
