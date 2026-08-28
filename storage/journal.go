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
	ErrCorruptJournal = errors.New("storage: corrupt journal")
	ErrCorruptRecord  = errors.New("storage: corrupt journal record")
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
	buffer, err := encodeJournalRecords(records)
	if err != nil {
		return err
	}
	return atomicWriteFile(j.path, buffer.Bytes(), 0o644)
}

func ValidateJournalReplacement(records []JournalRecord) error {
	_, err := encodeJournalRecords(records)
	return err
}

func encodeJournalRecords(records []JournalRecord) (*bytes.Buffer, error) {
	var buffer bytes.Buffer
	for _, record := range records {
		wire, err := encodeJournalRecord(record)
		if err != nil {
			return nil, err
		}
		if uint64(buffer.Len())+uint64(len(wire)) > uint64(maxJournalBodyBytes) {
			return nil, fmt.Errorf(
				"storage: replacement journal is too large: exceeds %d bytes",
				maxJournalBodyBytes,
			)
		}
		buffer.Write(wire)
	}
	return &buffer, nil
}

func encodeJournalRecord(record JournalRecord) ([]byte, error) {
	if len(record.Kind) > 1<<16-1 {
		return nil, errors.New("storage: journal record kind is too long")
	}
	if len(record.Key) > 1<<16-1 {
		return nil, errors.New("storage: journal record key is too long")
	}
	payloadLen := len(record.Payload)
	bodyLen := 2 + len(record.Kind) + 2 + len(record.Key) + payloadLen
	if uint64(bodyLen) > uint64(maxJournalBodyBytes) {
		return nil, fmt.Errorf("storage: journal record body is too large: %d bytes (maximum %d)", bodyLen, maxJournalBodyBytes)
	}
	buffer := bytes.NewBuffer(make([]byte, 0, 12+bodyLen))
	header := make([]byte, 12)
	binary.BigEndian.PutUint32(header[0:4], journalMagic)
	binary.BigEndian.PutUint32(header[4:8], uint32(bodyLen))
	body := bytes.NewBuffer(make([]byte, 0, bodyLen))
	_ = binary.Write(body, binary.BigEndian, uint16(len(record.Kind)))
	body.WriteString(record.Kind)
	_ = binary.Write(body, binary.BigEndian, uint16(len(record.Key)))
	body.WriteString(record.Key)
	body.Write(record.Payload)
	binary.BigEndian.PutUint32(header[8:12], crc32.ChecksumIEEE(body.Bytes()))
	buffer.Write(header)
	buffer.Write(body.Bytes())
	return buffer.Bytes(), nil
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
	if bodyLen < 4 || bodyLen > maxJournalBodyBytes {
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
