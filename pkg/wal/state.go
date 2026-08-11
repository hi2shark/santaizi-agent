package wal

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type EndpointState struct {
	EndpointID         string            `json:"endpoint_id"`
	Generation         uint64            `json:"generation"`
	Reliable           bool              `json:"reliable"`
	ActivationSession  string            `json:"activation_session,omitempty"`
	ActivationSequence uint64            `json:"activation_sequence,omitempty"`
	Cursors            map[string]uint64 `json:"cursors"`
	Activations        map[string]uint64 `json:"activations,omitempty"`
}

type CursorState struct {
	ConfigVersion uint64                    `json:"config_version"`
	Endpoints     map[string]*EndpointState `json:"endpoints"`
}

type StateStore struct {
	mu   sync.Mutex
	path string
	data CursorState
}

func OpenStateStore(dir string) (*StateStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	s := &StateStore{path: filepath.Join(dir, "state.json")}
	data, err := os.ReadFile(s.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &s.data); err != nil {
			return nil, fmt.Errorf("decode wal state: %w", err)
		}
	}
	if s.data.Endpoints == nil {
		s.data.Endpoints = make(map[string]*EndpointState)
	}
	return s, nil
}

func (s *StateStore) UpsertEndpoint(endpoint EndpointState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.data.Endpoints[endpoint.EndpointID]
	if current != nil && current.Generation == endpoint.Generation && endpoint.Cursors == nil {
		endpoint.Cursors = current.Cursors
		endpoint.Activations = current.Activations
	}
	if endpoint.Cursors == nil {
		endpoint.Cursors = make(map[string]uint64)
	}
	if endpoint.Activations == nil {
		endpoint.Activations = make(map[string]uint64)
	}
	if endpoint.ActivationSession != "" {
		activation := endpoint.ActivationSequence
		if activation == 0 {
			activation = 1
		}
		endpoint.Activations[endpoint.ActivationSession] = activation
		if _, ok := endpoint.Cursors[endpoint.ActivationSession]; !ok {
			endpoint.Cursors[endpoint.ActivationSession] = activation - 1
		}
	}
	s.data.Endpoints[endpoint.EndpointID] = &endpoint
	return s.saveLocked()
}

func (s *StateStore) SetConfigVersion(version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version <= s.data.ConfigVersion {
		return nil
	}
	s.data.ConfigVersion = version
	return s.saveLocked()
}

func (s *StateStore) RemoveEndpoint(endpointID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data.Endpoints, endpointID)
	return s.saveLocked()
}

func (s *StateStore) UpdateAck(endpointID string, generation uint64, sessionID []byte, ackThrough uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	endpoint := s.data.Endpoints[endpointID]
	if endpoint == nil || endpoint.Generation != generation {
		return fmt.Errorf("unknown endpoint generation %s/%d", endpointID, generation)
	}
	key := hex.EncodeToString(sessionID)
	if endpoint.Cursors[key] >= ackThrough {
		return nil
	}
	endpoint.Cursors[key] = ackThrough
	return s.saveLocked()
}

func (s *StateStore) Ack(endpointID string, sessionID []byte) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	endpoint := s.data.Endpoints[endpointID]
	if endpoint == nil {
		return 0
	}
	return endpoint.Cursors[hex.EncodeToString(sessionID)]
}

func (s *StateStore) Snapshot() CursorState {
	s.mu.Lock()
	defer s.mu.Unlock()
	encoded, _ := json.Marshal(s.data)
	var clone CursorState
	_ = json.Unmarshal(encoded, &clone)
	return clone
}

func (s *StateStore) saveLocked() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".state-*.tmp")
	if err != nil {
		return err
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
	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(s.path))
	if err == nil {
		err = dir.Sync()
		_ = dir.Close()
	}
	return err
}
