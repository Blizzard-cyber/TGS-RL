package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
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
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Replay() error = %v, want ErrRecordTooLarge", err)
	}
}

func TestJournalRejectsDeclaredBodyShorterThanFieldLengths(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	body := []byte{
		0, 5,
		'a', 'b',
	}
	record := encodedRawJournalRecord(body)
	if err := os.WriteFile(journal.Path(), record, 0o644); err != nil {
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
	if _, err := encodeJournalRecord(record); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("encodeJournalRecord() error = %v, want ErrRecordTooLarge", err)
	}
}

func TestJournalAppendRejectsOversizedRecord(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	if err := journal.Append(JournalRecord{Kind: "intent", Key: "k1", Payload: []byte("ok")}); err != nil {
		t.Fatalf("Append(valid) error = %v", err)
	}
	before, err := os.ReadFile(journal.Path())
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}

	err = journal.Append(JournalRecord{
		Kind:    "intent",
		Key:     "oversized",
		Payload: make([]byte, int(maxJournalBodyBytes)),
	})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Append(oversized) error = %v, want ErrRecordTooLarge", err)
	}

	after, err := os.ReadFile(journal.Path())
	if err != nil {
		t.Fatalf("ReadFile(after) error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("Append(oversized) modified journal contents")
	}
}

func TestJournalReplaceRejectsOversizedRecordWithoutReplacingFile(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	existing := JournalRecord{Kind: "intent", Key: "k1", Payload: []byte("one")}
	if err := journal.Append(existing); err != nil {
		t.Fatalf("Append(existing) error = %v", err)
	}
	before, err := os.ReadFile(journal.Path())
	if err != nil {
		t.Fatalf("ReadFile(before) error = %v", err)
	}

	err = journal.Replace([]JournalRecord{{
		Kind:    "intent",
		Key:     "oversized",
		Payload: make([]byte, int(maxJournalBodyBytes)),
	}})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Replace(oversized) error = %v, want ErrRecordTooLarge", err)
	}

	after, err := os.ReadFile(journal.Path())
	if err != nil {
		t.Fatalf("ReadFile(after) error = %v", err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("Replace(oversized) replaced journal contents")
	}

	var records []JournalRecord
	if err := journal.Replay(func(record JournalRecord) error {
		records = append(records, record)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(records) != 1 || !sameRecord(records[0], existing) {
		t.Fatalf("records = %+v", records)
	}
}

func TestJournalBoundarySizedRecordRoundTrips(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	record := JournalRecord{
		Kind:    "i",
		Key:     "k",
		Payload: bytes.Repeat([]byte{'x'}, int(maxJournalBodyBytes)-(2+len("i")+2+len("k"))),
	}
	if _, err := encodeJournalRecord(record); err != nil {
		t.Fatalf("encodeJournalRecord() error = %v", err)
	}
	if err := journal.Append(record); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	count := 0
	if err := journal.Replay(func(got JournalRecord) error {
		count++
		if !sameRecord(got, record) {
			t.Fatalf("record mismatch: got kind=%q key=%q payload=%d want kind=%q key=%q payload=%d",
				got.Kind, got.Key, len(got.Payload), record.Kind, record.Key, len(record.Payload))
		}
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("Replay() count = %d, want 1", count)
	}
}

func TestJournalReplaceRejectsOversizedAggregateWithoutReplacingFile(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, "events")
	if err != nil {
		t.Fatal(err)
	}
	existing := JournalRecord{Kind: "intent", Key: "existing", Payload: []byte("value")}
	if err := journal.Append(existing); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(journal.Path())
	if err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte{'x'}, int(maxJournalBodyBytes/2))
	err = journal.Replace([]JournalRecord{
		{Kind: "intent", Key: "one", Payload: payload},
		{Kind: "intent", Key: "two", Payload: payload},
	})
	if !errors.Is(err, ErrJournalTooLarge) {
		t.Fatalf("Replace() error = %v, want ErrJournalTooLarge", err)
	}
	after, err := os.ReadFile(journal.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("oversized aggregate replaced the existing journal")
	}
}

func TestValidateJournalReplacementRejectsOversizedRecord(t *testing.T) {
	err := ValidateJournalReplacement([]JournalRecord{{
		Kind:    "intent",
		Key:     "oversized",
		Payload: make([]byte, int(maxJournalBodyBytes)),
	}})
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("ValidateJournalReplacement() error = %v, want ErrRecordTooLarge", err)
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

func encodedRawJournalRecord(body []byte) []byte {
	record := make([]byte, 12+len(body))
	binary.BigEndian.PutUint32(record[0:4], journalMagic)
	binary.BigEndian.PutUint32(record[4:8], uint32(len(body)))
	copy(record[12:], body)
	binary.BigEndian.PutUint32(record[8:12], crc32.ChecksumIEEE(body))
	return record
}
