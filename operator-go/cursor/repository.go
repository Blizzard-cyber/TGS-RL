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
	if err := os.MkdirAll(filepath.Dir(r.path), 0o755); err != nil {
		return err
	}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, r.path); err != nil {
		return fmt.Errorf("rename cursor file: %w", err)
	}
	return nil
}
