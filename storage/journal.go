package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

var (
	ErrCorruptJournal  = errors.New("storage: corrupt journal")
	ErrCorruptRecord   = errors.New("storage: corrupt journal record")
	ErrRecordTooLarge  = errors.New("storage: journal record too large")
	ErrJournalTooLarge = errors.New("storage: replacement journal too large")
)

const (
	journalMagic        uint32 = 0x5447534a
	maxJournalBodyBytes uint32 = 64 << 20
)

type JournalRecord struct {
	Kind    string
	Key     string
	Payload []byte
}

type Journal struct {
	path string
}

func OpenJournal(root, name string) (*Journal, error) {
	if name == "" {
		return nil, errors.New("storage: journal name is required")
	}
	path := filepath.Join(root, name+".journal")
	if err := ensureDir(path); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("storage: create journal %s: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("storage: close journal %s: %w", path, err)
	}
	return &Journal{path: path}, nil
}

func (j *Journal) Path() string { return j.path }

func (j *Journal) Append(record JournalRecord) error {
	if record.Kind == "" {
		return errors.New("storage: journal record kind is required")
	}
	if record.Key == "" {
		return errors.New("storage: journal record key is required")
	}
	wire, err := encodeJournalRecord(record)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(j.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("storage: open journal %s: %w", j.path, err)
	}
	defer file.Close()
	if _, err := file.Write(wire); err != nil {
		return fmt.Errorf("storage: append journal %s: %w", j.path, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("storage: sync journal %s: %w", j.path, err)
	}
	return nil
}

func (j *Journal) Replay(visitor func(JournalRecord) error) error {
	file, err := os.Open(j.path)
	if err != nil {
		return fmt.Errorf("storage: open journal %s: %w", j.path, err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		record, err := decodeJournalRecord(reader)
		switch {
		case err == nil:
			if visitErr := visitor(record); visitErr != nil {
				return visitErr
			}
		case errors.Is(err, io.EOF):
			return nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			return fmt.Errorf("%w: truncated tail in %s", ErrCorruptJournal, j.path)
		default:
			return err
		}
	}
}

func (j *Journal) Replace(records []JournalRecord) error {
	return writeJournalRecordsAtomically(j.path, records, 0o644)
}

func ValidateJournalReplacement(records []JournalRecord) error {
	var total uint64
	for _, record := range records {
		bodyLen, err := journalRecordBodyLength(record)
		if err != nil {
			return err
		}
		total += 12 + bodyLen
		if total > uint64(maxJournalBodyBytes) {
			return fmt.Errorf("%w: encoded size %d exceeds limit %d", ErrJournalTooLarge, total, maxJournalBodyBytes)
		}
	}
	return nil
}

func journalRecordBodyLength(record JournalRecord) (uint64, error) {
	if len(record.Kind) > 1<<16-1 {
		return 0, errors.New("storage: journal record kind is too long")
	}
	if len(record.Key) > 1<<16-1 {
		return 0, errors.New("storage: journal record key is too long")
	}
	bodyLen := uint64(2) + uint64(len(record.Kind)) + 2 + uint64(len(record.Key)) + uint64(len(record.Payload))
	if bodyLen > uint64(maxJournalBodyBytes) {
		return 0, fmt.Errorf(
			"%w: body length %d exceeds limit %d",
			ErrRecordTooLarge,
			bodyLen,
			maxJournalBodyBytes,
		)
	}
	return bodyLen, nil
}

func encodeJournalRecord(record JournalRecord) ([]byte, error) {
	bodyLen64, err := journalRecordBodyLength(record)
	if err != nil {
		return nil, err
	}
	bodyLen := int(bodyLen64)

	wire := make([]byte, 12+bodyLen)
	binary.BigEndian.PutUint32(wire[0:4], journalMagic)
	binary.BigEndian.PutUint32(wire[4:8], uint32(bodyLen))

	offset := 12
	binary.BigEndian.PutUint16(wire[offset:offset+2], uint16(len(record.Kind)))
	offset += 2
	offset += copy(wire[offset:], record.Kind)
	binary.BigEndian.PutUint16(wire[offset:offset+2], uint16(len(record.Key)))
	offset += 2
	offset += copy(wire[offset:], record.Key)
	copy(wire[offset:], record.Payload)

	binary.BigEndian.PutUint32(wire[8:12], crc32.ChecksumIEEE(wire[12:]))
	return wire, nil
}

func decodeJournalRecord(reader *bufio.Reader) (JournalRecord, error) {
	header := make([]byte, 12)
	if _, err := io.ReadFull(reader, header); err != nil {
		return JournalRecord{}, err
	}
	if binary.BigEndian.Uint32(header[0:4]) != journalMagic {
		return JournalRecord{}, fmt.Errorf("%w: bad magic", ErrCorruptRecord)
	}
	bodyLen := binary.BigEndian.Uint32(header[4:8])
	if bodyLen > maxJournalBodyBytes {
		return JournalRecord{}, fmt.Errorf(
			"%w: body length %d exceeds limit %d",
			ErrRecordTooLarge,
			bodyLen,
			maxJournalBodyBytes,
		)
	}
	if bodyLen < 4 {
		return JournalRecord{}, fmt.Errorf("%w: invalid body length %d", ErrCorruptRecord, bodyLen)
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(reader, body); err != nil {
		return JournalRecord{}, err
	}
	if want, got := binary.BigEndian.Uint32(header[8:12]), crc32.ChecksumIEEE(body); want != got {
		return JournalRecord{}, fmt.Errorf("%w: checksum mismatch", ErrCorruptRecord)
	}
	bodyReader := bytes.NewReader(body)
	var kindLen uint16
	if err := binary.Read(bodyReader, binary.BigEndian, &kindLen); err != nil {
		return JournalRecord{}, fmt.Errorf("%w: kind length", ErrCorruptRecord)
	}
	kind := make([]byte, kindLen)
	if _, err := io.ReadFull(bodyReader, kind); err != nil {
		return JournalRecord{}, fmt.Errorf("%w: kind", ErrCorruptRecord)
	}
	var keyLen uint16
	if err := binary.Read(bodyReader, binary.BigEndian, &keyLen); err != nil {
		return JournalRecord{}, fmt.Errorf("%w: key length", ErrCorruptRecord)
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(bodyReader, key); err != nil {
		return JournalRecord{}, fmt.Errorf("%w: key", ErrCorruptRecord)
	}
	payload, err := io.ReadAll(bodyReader)
	if err != nil {
		return JournalRecord{}, fmt.Errorf("%w: payload", ErrCorruptRecord)
	}
	return JournalRecord{Kind: string(kind), Key: string(key), Payload: payload}, nil
}

func writeJournalRecordsAtomically(path string, records []JournalRecord, mode os.FileMode) error {
	if err := ValidateJournalReplacement(records); err != nil {
		return err
	}
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

	writer := bufio.NewWriter(temp)
	for _, record := range records {
		wire, err := encodeJournalRecord(record)
		if err != nil {
			return err
		}
		if _, err := writer.Write(wire); err != nil {
			return fmt.Errorf("storage: write temp file for %s: %w", path, err)
		}
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("storage: flush temp file for %s: %w", path, err)
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
