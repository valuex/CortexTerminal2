package signalr

import (
	"sync"
)

// InMemoryWorkerRegistry is the minimum WorkerRegistry implementation that
// satisfies the worker-hub dispatch path. Persistence (Workers table +
// metadata + metrics) lands in the Sessions/Workers subsystem port. Until
// then this gives RegisterWorker / FindByConnectionID / Unregister real
// semantics so the C# Worker daemon can connect, register, and disconnect
// without losing its connection handle.
type InMemoryWorkerRegistry struct {
	mu       sync.RWMutex
	byConn   map[string]WorkerHandle // connectionID → handle
	byWorker map[string]string       // workerID → connectionID
	metadata map[string]WorkerMetadataWire
	metrics  map[string]WorkerMetricsWire
}

func NewInMemoryWorkerRegistry() *InMemoryWorkerRegistry {
	return &InMemoryWorkerRegistry{
		byConn:   make(map[string]WorkerHandle),
		byWorker: make(map[string]string),
		metadata: make(map[string]WorkerMetadataWire),
		metrics:  make(map[string]WorkerMetricsWire),
	}
}

func (r *InMemoryWorkerRegistry) Register(workerID, connectionID, ownerUserID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// A worker can only be registered on one connection at a time. If a
	// stale handle exists for this workerId, drop it so the new connection
	// wins — matches the C# DbWorkerRegistry.Register behaviour.
	if prev, ok := r.byWorker[workerID]; ok {
		delete(r.byConn, prev)
	}
	r.byConn[connectionID] = WorkerHandle{WorkerID: workerID, OwnerUserID: ownerUserID}
	r.byWorker[workerID] = connectionID
}

// FindByWorkerID returns the connection id currently mapped to workerID.
func (r *InMemoryWorkerRegistry) FindByWorkerID(workerID string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn, ok := r.byWorker[workerID]
	return conn, ok
}

// LookupOwner returns the user id currently bound to workerID (empty if
// none). Used by the sessions.WorkerResolver to gate CreateSession.
func (r *InMemoryWorkerRegistry) LookupOwner(workerID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	conn, ok := r.byWorker[workerID]
	if !ok {
		return ""
	}
	h, ok := r.byConn[conn]
	if !ok {
		return ""
	}
	return h.OwnerUserID
}

// SetOwner rebinds workerID to ownerUserID. Used by the session launch
// coordinator when allocating a worker to a new session.
func (r *InMemoryWorkerRegistry) SetOwner(workerID, ownerUserID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conn, ok := r.byWorker[workerID]
	if !ok {
		return
	}
	h := r.byConn[conn]
	h.OwnerUserID = ownerUserID
	r.byConn[conn] = h
}

// ListForUser returns the worker ids currently owned by userID.
func (r *InMemoryWorkerRegistry) ListForUser(userID string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for _, h := range r.byConn {
		if h.OwnerUserID == userID {
			out = append(out, h.WorkerID)
		}
	}
	return out
}

func (r *InMemoryWorkerRegistry) Unregister(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if conn, ok := r.byWorker[workerID]; ok {
		delete(r.byConn, conn)
		delete(r.byWorker, workerID)
		delete(r.metadata, workerID)
		delete(r.metrics, workerID)
	}
}

func (r *InMemoryWorkerRegistry) FindByConnectionID(connectionID string) (WorkerHandle, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.byConn[connectionID]
	return h, ok
}

func (r *InMemoryWorkerRegistry) PersistMetadata(workerID string, meta WorkerMetadataWire) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metadata[workerID] = meta
}

func (r *InMemoryWorkerRegistry) UpdateMetrics(workerID string, metrics WorkerMetricsWire) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics[workerID] = metrics
}

// LookupMetadata is a small accessor used by /api/me/workers when the
// read-side ports land. Not yet used by the hubs.
func (r *InMemoryWorkerRegistry) LookupMetadata(workerID string) (WorkerMetadataWire, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.metadata[workerID]
	return m, ok
}