// Package helperstate provides process-safe atomic JSON storage for local
// NVIDIA helper executables. Domain packages retain all schema validation.
package helperstate

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type Store[T any] struct {
	path     string
	empty    func() T
	validate func(T) error
}

func New[T any](path string, empty func() T, validate func(T) error) (*Store[T], error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("helper state path is required")
	}
	if empty == nil || validate == nil {
		return nil, errors.New("helper state constructor and validator are required")
	}
	return &Store[T]{path: path, empty: empty, validate: validate}, nil
}

func (s *Store[T]) Snapshot() (T, error) {
	var snapshot T
	err := s.withLock(false, func(state *T) error {
		snapshot = *state
		return nil
	})
	return snapshot, err
}

func (s *Store[T]) Update(fn func(*T) error) error {
	return s.withLock(true, fn)
}

func (s *Store[T]) withLock(write bool, fn func(*T) error) error {
	if fn == nil {
		return errors.New("helper state callback is required")
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create helper state directory: %w", err)
	}
	if err := os.Chmod(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("restrict helper state directory: %w", err)
	}
	lock, err := os.OpenFile(s.path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open helper state lock: %w", err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock helper state: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) //nolint:errcheck
	state, err := s.load()
	if err != nil {
		return err
	}
	if err := fn(&state); err != nil || !write {
		return err
	}
	return s.save(state)
}

func (s *Store[T]) load() (T, error) {
	state := s.empty()
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return state, fmt.Errorf("read helper state: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode helper state: %w", err)
	}
	if err := s.validate(state); err != nil {
		return state, err
	}
	return state, nil
}

func (s *Store[T]) save(state T) error {
	if err := s.validate(state); err != nil {
		return err
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode helper state: %w", err)
	}
	temp, err := os.CreateTemp(filepath.Dir(s.path), ".helper-state-*")
	if err != nil {
		return fmt.Errorf("create helper state temp file: %w", err)
	}
	tempPath := temp.Name()
	ok := false
	defer func() {
		if !ok {
			_ = os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(append(data, '\n')); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write helper state: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync helper state: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close helper state: %w", err)
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("replace helper state: %w", err)
	}
	directory, err := os.Open(filepath.Dir(s.path))
	if err != nil {
		return fmt.Errorf("open helper state directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync helper state directory: %w", err)
	}
	ok = true
	return nil
}
