package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

func ensureDir(path string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("storage: create dir %s: %w", dir, err)
	}
	return syncDir(dir)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("storage: open dir %s: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("storage: sync dir %s: %w", path, err)
	}
	return nil
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	if err := ensureDir(path); err != nil {
		return err
	}
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("storage: create temp file for %s: %w", path, err)
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
		return fmt.Errorf("storage: chmod temp file for %s: %w", path, err)
	}
	if _, err := temp.Write(data); err != nil {
		return fmt.Errorf("storage: write temp file for %s: %w", path, err)
	}
	if err := temp.Sync(); err != nil {
		return fmt.Errorf("storage: sync temp file for %s: %w", path, err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("storage: close temp file for %s: %w", path, err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("storage: rename temp file into %s: %w", path, err)
	}
	cleanup = false
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	return nil
}
