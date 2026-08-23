// Package memstorage is an in-memory implementation of storage.ArtifactStorage
// for smoke tests. Drop-in replacement for S3CompatibleArtifactStorage
// when no real S3 is available — every object lives in a map under the
// key {sessionID}/{filename}.
package memstorage

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/storage"
)

// InMemoryStorage satisfies storage.ArtifactStorage. Concurrent-safe via
// a single mutex — sufficient for smoke tests.
type InMemoryStorage struct {
	mu      sync.Mutex
	objects map[string][]byte
}

// NewInMemoryStorage creates an empty store.
func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{objects: make(map[string][]byte)}
}

func key(sessionID, filename string) string {
	return sessionID + "/" + filename
}

func (s *InMemoryStorage) GenerateUploadURL(sessionID, filename string) (storage.UploadURLResponse, error) {
	return storage.UploadURLResponse{
		ArtifactID: "", // populated by service after INSERT
		UploadURL:  "memstorage://" + key(sessionID, filename),
		S3Key:      key(sessionID, filename),
		ExpiresAt:  time.Now().UTC().Add(15 * time.Minute),
	}, nil
}

func (s *InMemoryStorage) GenerateDownloadURL(sessionID, filename string) (storage.DownloadURLResponse, error) {
	return storage.DownloadURLResponse{
		DownloadURL: "memstorage://" + key(sessionID, filename),
		ExpiresAt:   time.Now().UTC().Add(15 * time.Minute),
	}, nil
}

func (s *InMemoryStorage) DeleteObject(sessionID, filename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key(sessionID, filename))
	return nil
}

func (s *InMemoryStorage) DeleteSessionPrefix(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := sessionID + "/"
	for k := range s.objects {
		if strings.HasPrefix(k, prefix) {
			delete(s.objects, k)
		}
	}
	return nil
}

func (s *InMemoryStorage) GetObjectSize(sessionID, filename string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.objects[key(sessionID, filename)]
	if !ok {
		return 0, fmt.Errorf("not found: %s", key(sessionID, filename))
	}
	return int64(len(body)), nil
}

func (s *InMemoryStorage) ObjectExists(sessionID, filename string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key(sessionID, filename)]
	return ok, nil
}

// Put is a test-only helper: the upload URL is a memstorage:// scheme, so
// tests can directly inject the bytes without going through HTTP. The
// Complete step uses GetObjectSize to verify.
func (s *InMemoryStorage) Put(sessionID, filename string, body []byte) error {
	if len(body) == 0 {
		return errors.New("empty body")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key(sessionID, filename)] = body
	return nil
}

// Has returns true if the named object is present.
func (s *InMemoryStorage) Has(sessionID, filename string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[key(sessionID, filename)]
	return ok
}