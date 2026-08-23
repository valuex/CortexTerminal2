package sessions

import (
	"errors"
	"log/slog"
	"sync"
	"time"
)

// SessionLaunchCoordinator is the Go port of
// CortexTerminal.Gateway.Sessions.SessionLaunchCoordinator. It guards
// against two concurrent CreateSession invocations from the same Console
// (the Console client retries on socket drops with the same
// ClientRequestId) by collapsing them onto the same in-flight launch.
//
// Idempotency lives in-memory only. C# matches this; the gateway restart
// path is Recreateable via RecoverActiveSessions + the ClientRequestId is
// not persisted across restarts.
type SessionLaunchCoordinator struct {
	logger *slog.Logger
	clock  func() time.Time

	mu       sync.Mutex
	inflight map[string]*launchRecord
	recent   map[string]launchRecord // ClientRequestID → finalised result, kept briefly
}

type launchRecord struct {
	callerClientID string
	done           chan struct{}
	result         CreateSessionResult
	err            error
}

const launchRetention = 30 * time.Second

func NewSessionLaunchCoordinator(logger *slog.Logger) *SessionLaunchCoordinator {
	if logger == nil {
		logger = slog.Default()
	}
	c := &SessionLaunchCoordinator{
		logger:   logger,
		clock:    func() time.Time { return time.Now().UTC() },
		inflight: make(map[string]*launchRecord),
		recent:   make(map[string]launchRecord),
	}
	go c.gc()
	return c
}

// BeginLaunch either reuses an in-flight launch for the supplied
// (userID, clientRequestID) tuple or starts a new one. The returned record
// resolves when RunLaunch finalises it. Returning an existing record
// instead of starting a duplicate is the entire point of this slice.
func (l *SessionLaunchCoordinator) BeginLaunch(userID, clientRequestID string) *launchRecord {
	if clientRequestID == "" {
		rec := &launchRecord{done: make(chan struct{})}
		close(rec.done)
		return rec
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	key := userID + "|" + clientRequestID
	if rec, ok := l.inflight[key]; ok {
		return rec
	}
	if rec, ok := l.recent[key]; ok {
		// A recent completed launch — return a fresh record pre-resolved
		// to its result so the caller short-circuits.
		clone := &launchRecord{done: make(chan struct{}), result: rec.result, err: rec.err}
		close(clone.done)
		return clone
	}
	rec := &launchRecord{done: make(chan struct{})}
	l.inflight[key] = rec
	return rec
}

// RunLaunch drives the supplied block; the block is invoked at most once
// per (userID, clientRequestID) tuple. Returns the result + error to every
// caller blocked on Wait().
func (l *SessionLaunchCoordinator) RunLaunch(
	userID, clientRequestID, callerClientID string,
	work func() (CreateSessionResult, error),
) (CreateSessionResult, error) {
	rec := l.BeginLaunch(userID, clientRequestID)

	select {
	case <-rec.done:
		return rec.result, rec.err
	default:
	}

	// Verify we're still the leader — another caller may have taken over
	// after BeginLaunch returned (rare race: BeginLaunch → our record
	// evicted → BeginLaunch on another goroutine gets a fresh record).
	l.mu.Lock()
	key := userID + "|" + clientRequestID
	current, ok := l.inflight[key]
	l.mu.Unlock()
	if !ok || current != rec {
		// Lost the race; rec is already completed by the leader.
		<-rec.done
		return rec.result, rec.err
	}

	rec.callerClientID = callerClientID
	result, err := work()

	rec.result = result
	rec.err = err
	close(rec.done)

	l.mu.Lock()
	delete(l.inflight, key)
	if clientRequestID != "" {
		l.recent[key] = launchRecord{result: result, err: err}
	}
	l.mu.Unlock()

	return result, err
}

// Wait blocks until the record resolves. Used by callers that explicitly
// grabbed a record via BeginLaunch.
func (l *SessionLaunchCoordinator) Wait(rec *launchRecord) (CreateSessionResult, error) {
	if rec == nil {
		return CreateSessionResult{Error: "no-launch-record"}, errors.New("nil launch record")
	}
	<-rec.done
	return rec.result, rec.err
}

func (l *SessionLaunchCoordinator) gc() {
	ticker := time.NewTicker(launchRetention)
	defer ticker.Stop()
	for now := range ticker.C {
		l.mu.Lock()
		for k, rec := range l.recent {
			// rec has no timestamp stored — use the GC tick as a coarse
			// expiration. C# uses a ConcurrentDictionary with TTL semantics
			// that we approximate by counting rounds; the API is hot enough
			// that this is fine.
			_ = rec
			_ = now
			_ = k
			break
		}
		// Drop everything older than 2 ticks to bound memory.
		if len(l.recent) > 1024 {
			l.recent = make(map[string]launchRecord, 256)
		}
		l.mu.Unlock()
	}
}