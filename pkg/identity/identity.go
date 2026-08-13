package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/hi2shark/santaizi-agent/pkg/util"
)

const identityFileName = "identity"

type ID [16]byte

func New() (ID, error) {
	var id ID
	if _, err := io.ReadFull(rand.Reader, id[:]); err != nil {
		return ID{}, fmt.Errorf("generate identity: %w", err)
	}
	// RFC 4122 variant and version bits make diagnostic UUID rendering familiar.
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id, nil
}

func LoadOrCreate(dataDir string) (ID, error) {
	if dataDir == "" {
		return ID{}, errors.New("identity data directory is empty")
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return ID{}, fmt.Errorf("create identity directory: %w", err)
	}
	path := filepath.Join(dataDir, identityFileName)
	if id, err := read(path); err == nil {
		return id, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return ID{}, err
	}

	id, err := New()
	if err != nil {
		return ID{}, err
	}
	if err := writeAtomic(path, id[:]); err != nil {
		return ID{}, err
	}
	return id, nil
}

func read(path string) (ID, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ID{}, err
	}
	if len(data) != len(ID{}) {
		return ID{}, fmt.Errorf("identity file %s has invalid length %d", path, len(data))
	}
	var id ID
	copy(id[:], data)
	if id == (ID{}) {
		return ID{}, fmt.Errorf("identity file %s contains an empty identity", path)
	}
	return id, nil
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".identity-*.tmp")
	if err != nil {
		return fmt.Errorf("create identity temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("install identity file: %w", err)
	}
	return util.SyncDir(dir)
}

func EventID(nodeID, sessionID ID, sequence uint64) ID {
	var input [40]byte
	copy(input[0:16], nodeID[:])
	copy(input[16:32], sessionID[:])
	binary.BigEndian.PutUint64(input[32:40], sequence)
	sum := sha256.Sum256(input[:])
	var eventID ID
	copy(eventID[:], sum[:16])
	return eventID
}

type Session struct {
	ID       ID
	sequence atomic.Uint64
}

func NewSession() (*Session, error) {
	id, err := New()
	if err != nil {
		return nil, err
	}
	return &Session{ID: id}, nil
}

func (s *Session) Next(nodeID ID) (uint64, ID) {
	sequence := s.sequence.Add(1)
	return sequence, EventID(nodeID, s.ID, sequence)
}
