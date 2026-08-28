package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestJournalAppendReplayAndReplace(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	first := JournalRecord{Kind: "intent", Key: "k1", Payload: []byte("one")}
	second := JournalRecord{Kind: "intent", Key: "k2", Payload: []byte("two")}
	if err := journal.Append(first); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if err := journal.Append(second); err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	var records []JournalRecord
	if err := journal.Replay(func(record JournalRecord) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(records) != 2 || !sameRecord(records[0], first) || !sameRecord(records[1], second) {
		t.Fatalf("records = %+v", records)
	}
	if err := journal.Replace([]JournalRecord{second}); err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	records = records[:0]
	if err := journal.Replay(func(record JournalRecord) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatalf("Replay(after replace) error = %v", err)
	}
	if len(records) != 1 || !sameRecord(records[0], second) {
		t.Fatalf("records after replace = %+v", records)
	}
}

func TestJournalDetectsCorruption(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	if err := journal.Append(JournalRecord{Kind: "intent", Key: "k1", Payload: []byte("one")}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	file, err := os.OpenFile(journal.Path(), os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close()
	if _, err := file.WriteAt([]byte{0, 1, 2, 3}, 0); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	err = journal.Replay(func(JournalRecord) error { return nil })
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Replay() error = %v, want ErrCorruptRecord", err)
	}
}

func TestJournalRejectsOversizedBodyBeforeAllocation(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	header := make([]byte, 12)
	binary.BigEndian.PutUint32(header[0:4], journalMagic)
	binary.BigEndian.PutUint32(header[4:8], maxJournalBodyBytes+1)
	if err := os.WriteFile(journal.Path(), header, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	err = journal.Replay(func(JournalRecord) error { return nil })
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Replay() error = %v, want ErrCorruptRecord", err)
	}
}

func TestJournalRejectsBodyShorterThanLengths(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	header := make([]byte, 12)
	binary.BigEndian.PutUint32(header[0:4], journalMagic)
	binary.BigEndian.PutUint32(header[4:8], 3)
	if err := os.WriteFile(journal.Path(), header, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	err = journal.Replay(func(JournalRecord) error { return nil })
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("Replay() error = %v, want ErrCorruptRecord", err)
	}
}

func TestJournalRejectsOversizedEncodedRecord(t *testing.T) {
	record := JournalRecord{
		Kind:    "intent",
		Key:     "oversized",
		Payload: make([]byte, int(maxJournalBodyBytes)),
	}
	if _, err := encodeJournalRecord(record); err == nil {
		t.Fatal("encodeJournalRecord() error = nil, want size limit error")
	}
}

func TestCheckpointStoreSaveLoadAndMissing(t *testing.T) {
	root := t.TempDir()
	store, err := OpenCheckpointStore(root, "state")
	if err != nil {
		t.Fatalf("OpenCheckpointStore() error = %v", err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrCheckpointNotFound) {
		t.Fatalf("Load(missing) error = %v, want ErrCheckpointNotFound", err)
	}
	payload := []byte("checkpoint")
	if err := store.Save(payload); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if string(loaded) != string(payload) {
		t.Fatalf("loaded = %q, want %q", loaded, payload)
	}
	if _, err := os.Stat(filepath.Join(root, "state.checkpoint")); err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
}

func sameRecord(left, right JournalRecord) bool {
	return left.Kind == right.Kind &&
		left.Key == right.Key &&
		bytes.Equal(left.Payload, right.Payload)
}
