package sessions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
)

// WorkerResolver is the slice of IWorkerRegistry the coordinator needs to
// pick a worker for a new session. The C# code calls TryGetWorker +
// SetWorkerOwner + TryGetLeastBusyForUser; we expose the same surface.
type WorkerResolver interface {
	TryGetWorker(workerID string) (WorkerHandle, bool)
	TryGetLeastBusyForUser(userID string) (WorkerHandle, bool)
	SetWorkerOwner(workerID, ownerUserID string) bool
}

// WorkerHandle is the minimal projection the coordinator needs.
type WorkerHandle struct {
	WorkerID    string
	ConnectionID string
	OwnerUserID string
}

// Coordinator is the Go port of CortexTerminal.Gateway.Sessions.DbSessionCoordinator.
// It holds an in-memory mirror of the Sessions table plus the attachment
// state machine, persisting each transition to SQLite via SessionsRepo.
//
// All mutating operations acquire syncMu; reads take it briefly to copy a
// value out. The hot path (TryGetSession) only takes the read side.
type Coordinator struct {
	repo   *data.SessionsRepo
	worker WorkerResolver
	logger *slog.Logger
	clock  func() time.Time

	syncMu      sync.RWMutex
	sessions    map[string]SessionRecord
	lastTouchBySession map[string]time.Time
}

func NewCoordinator(repo *data.SessionsRepo, worker WorkerResolver, logger *slog.Logger) *Coordinator {
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{
		repo:               repo,
		worker:             worker,
		logger:             logger,
		clock:              func() time.Time { return time.Now().UTC() },
		sessions:           make(map[string]SessionRecord),
		lastTouchBySession: make(map[string]time.Time),
	}
}

// RecoverActiveSessions rebuilds the in-memory mirror from SQLite after a
// gateway restart. Attached rows collapse to DetachedGracePeriod (the
// client socket died with the gateway). DetachedGracePeriod stays.
// Recovering stays.
func (c *Coordinator) RecoverActiveSessions(ctx context.Context) error {
	entities, err := c.repo.ListByAttachmentStates(ctx,
		[]string{"Attached", "DetachedGracePeriod", "Recovering"})
	if err != nil {
		return fmt.Errorf("list active sessions: %w", err)
	}
	if len(entities) == 0 {
		return nil
	}

	now := c.clock()
	toPersist := []string{} // session IDs that flipped Attached → DetachedGracePeriod

	c.syncMu.Lock()
	for _, ent := range entities {
		rec := MapEntityToRecord(&ent)
		wasAttached := ent.AttachmentState == "Attached"
		if wasAttached {
			rec.AttachmentState = StateDetachedGracePeriod
			rec.AttachedClientConnectionID = ""
			rec.LastActivityAtUTC = now.UnixNano()
			toPersist = append(toPersist, ent.SessionID)
			c.logger.Info("session.gateway-restart-detached",
				"session", ent.SessionID,
				"worker", ent.WorkerID,
				"user", ent.UserID)
		} else if rec.AttachmentState == StateRecovering {
			c.logger.Info("session.recovering",
				"session", ent.SessionID,
				"worker", ent.WorkerID,
				"user", ent.UserID)
		}
		c.sessions[ent.SessionID] = rec
	}
	c.syncMu.Unlock()

	for _, sessionID := range toPersist {
		attached := ""
		if err := c.repo.UpdateState(ctx, sessionID,
			StateDetachedGracePeriod.String(), &attached, nil, nil, nil, nil, &now); err != nil {
			c.logger.Warn("persist DetachedGracePeriod failed", "session", sessionID, "err", err)
		}
	}

	c.logger.Info("Recovered active sessions from database", "count", len(entities))
	return nil
}

// CreateSession picks a worker (specific or least-busy), binds it to the
// user, generates a session id, inserts the row, mirrors it in memory,
// and returns the result. Mirrors CreateSessionAsync in C#.
func (c *Coordinator) CreateSession(
	ctx context.Context,
	userID string,
	request CreateSessionRequest,
	clientConnectionID string,
) (CreateSessionResult, error) {

	var worker WorkerHandle
	if request.WorkerID != "" {
		w, ok := c.worker.TryGetWorker(request.WorkerID)
		if !ok || (w.OwnerUserID != "" && w.OwnerUserID != userID) {
			return CreateSessionResult{Error: "worker-not-found"}, nil
		}
		worker = w
	} else {
		w, ok := c.worker.TryGetLeastBusyForUser(userID)
		if !ok {
			return CreateSessionResult{Error: "no-worker-available"}, nil
		}
		worker = w
	}

	if !c.worker.SetWorkerOwner(worker.WorkerID, userID) {
		return CreateSessionResult{Error: "no-worker-available"}, nil
	}

	now := c.clock()
	sessionID := "sess_" + uuid.NewString()
	rec := SessionRecord{
		SessionID:                  sessionID,
		UserID:                     userID,
		WorkerID:                   worker.WorkerID,
		WorkerConnectionID:         worker.ConnectionID,
		Columns:                    request.Columns,
		Rows:                       request.Rows,
		CreatedAtUTC:               now.UnixNano(),
		LastActivityAtUTC:          now.UnixNano(),
		AttachmentState:            StateAttached,
		AttachedClientConnectionID: clientConnectionID,
	}

	entity := data.SessionEntity{
		SessionID:                  sessionID,
		UserID:                     userID,
		WorkerID:                   worker.WorkerID,
		WorkerConnectionID:         sql.NullString{String: worker.ConnectionID, Valid: worker.ConnectionID != ""},
		Columns:                    request.Columns,
		Rows:                       request.Rows,
		CreatedAtUTC:               now.Format(time.RFC3339Nano),
		LastActivityAtUTC:          now.Format(time.RFC3339Nano),
		AttachmentState:            StateAttached.String(),
		AttachedClientConnectionID: sql.NullString{String: clientConnectionID, Valid: clientConnectionID != ""},
		ReplayPending:              false,
	}
	if err := c.repo.Insert(ctx, entity); err != nil {
		return CreateSessionResult{Error: err.Error()}, err
	}

	c.syncMu.Lock()
	c.sessions[sessionID] = rec
	c.syncMu.Unlock()

	c.logger.Info("session.created",
		"session", sessionID,
		"worker", worker.WorkerID,
		"user", userID)

	return CreateSessionResult{
		Success:   true,
		SessionID: sessionID,
		WorkerID:  worker.WorkerID,
	}, nil
}

// DetachSession moves an Attached session into DetachedGracePeriod. The
// C# code is silent on unknown / non-owned sessions; we match that.
func (c *Coordinator) DetachSession(ctx context.Context, userID, sessionID string, now time.Time) error {
	c.syncMu.Lock()
	session, ok := c.sessions[sessionID]
	if !ok || session.UserID != userID {
		c.syncMu.Unlock()
		return nil
	}
	updated := session.Update(func(r *SessionRecord) {
		r.AttachmentState = StateDetachedGracePeriod
		r.AttachedClientConnectionID = ""
		r.ReplayPending = false
		r.LastActivityAtUTC = now.UnixNano()
	})
	c.syncMu.Unlock()

	attached := ""
	if err := c.repo.UpdateState(ctx, sessionID,
		StateDetachedGracePeriod.String(), &attached, nil, nil, nil, nil, &now); err != nil {
		return err
	}

	c.syncMu.Lock()
	if cur, ok := c.sessions[sessionID]; ok && cur.UserID == userID {
		c.sessions[sessionID] = updated
	}
	c.syncMu.Unlock()

	c.logger.Info("session.client-detached", "session", sessionID, "user", userID)
	return nil
}

// DeleteSession removes the row from SQLite and the in-memory mirror.
// Mirrors DeleteSessionAsync.
func (c *Coordinator) DeleteSession(ctx context.Context, userID, sessionID string) (DeleteSessionResult, error) {
	entity, err := c.repo.FindForUser(ctx, sessionID, userID)
	if err != nil {
		return DeleteSessionResult{Error: err.Error()}, err
	}
	if entity == nil {
		return DeleteSessionResult{Error: "session-not-found"}, nil
	}
	if err := c.repo.Delete(ctx, sessionID); err != nil {
		return DeleteSessionResult{Error: err.Error()}, err
	}
	c.syncMu.Lock()
	delete(c.sessions, sessionID)
	delete(c.lastTouchBySession, sessionID)
	c.syncMu.Unlock()
	c.logger.Info("session.deleted", "session", sessionID, "user", userID)
	return DeleteSessionResult{Success: true}, nil
}

// ReattachSession is the meat of the state machine. Mirrors
// ReattachSessionAsync in DbSessionCoordinator.cs.
func (c *Coordinator) ReattachSession(
	ctx context.Context,
	userID string,
	request ReattachSessionRequest,
	clientConnectionID string,
	now time.Time,
) (ReattachSessionResult, error) {

	c.syncMu.Lock()
	session, ok := c.sessions[request.SessionID]
	if !ok || session.UserID != userID {
		c.syncMu.Unlock()
		return ReattachSessionResult{Error: "session-not-found"}, nil
	}

	var staged SessionRecord
	var dbState string
	var dbClient *string
	var dbReplay *bool
	var logMsg string

	switch session.AttachmentState {
	case StateAttached:
		staged = session.Update(func(r *SessionRecord) {
			r.AttachedClientConnectionID = clientConnectionID
			r.ReplayPending = true
			r.LastActivityAtUTC = now.UnixNano()
		})
		s := StateAttached.String()
		dbState = s
		dbClient = &clientConnectionID
		t := true
		dbReplay = &t
		logMsg = fmt.Sprintf("session.reattached %s client=%s (displaced existing)", request.SessionID, clientConnectionID)

	case StateRecovering:
		staged = session.Update(func(r *SessionRecord) {
			r.AttachmentState = StateAttached
			r.AttachedClientConnectionID = clientConnectionID
			r.ReplayPending = false
			r.LastActivityAtUTC = now.UnixNano()
		})
		dbState = StateAttached.String()
		dbClient = &clientConnectionID
		logMsg = fmt.Sprintf("Reattach: %s Recovering → Attached (client=%s)", request.SessionID, clientConnectionID)

	case StateDetachedGracePeriod:
		staged = session.Update(func(r *SessionRecord) {
			r.AttachmentState = StateAttached
			r.AttachedClientConnectionID = clientConnectionID
			r.ReplayPending = true
			r.LastActivityAtUTC = now.UnixNano()
		})
		dbState = StateAttached.String()
		dbClient = &clientConnectionID
		t := true
		dbReplay = &t
		logMsg = fmt.Sprintf("session.reattached %s client=%s (from DetachedGracePeriod)", request.SessionID, clientConnectionID)

	default:
		// Expired / Exited — terminal states. C# returns session-expired.
		c.syncMu.Unlock()
		return ReattachSessionResult{Error: "session-expired"}, nil
	}
	c.syncMu.Unlock()

	if err := c.repo.UpdateState(ctx, request.SessionID,
		dbState, dbClient, nil, nil, nil, dbReplay, &now); err != nil {
		return ReattachSessionResult{Error: err.Error()}, err
	}

	c.syncMu.Lock()
	if cur, ok := c.sessions[request.SessionID]; ok && cur.UserID == userID {
		c.sessions[request.SessionID] = staged
	}
	c.syncMu.Unlock()

	c.logger.Info(logMsg)
	return ReattachSessionResult{Success: true}, nil
}

// MarkReplayCompleted flips replay_pending off once the Console drained
// the replay queue. Mirrors MarkReplayCompleted.
func (c *Coordinator) MarkReplayCompleted(ctx context.Context, sessionID, clientConnectionID string) error {
	now := c.clock()
	f := false

	c.syncMu.Lock()
	session, ok := c.sessions[sessionID]
	if !ok ||
		session.AttachmentState != StateAttached ||
		session.AttachedClientConnectionID != clientConnectionID {
		c.syncMu.Unlock()
		return nil
	}
	staged := session.Update(func(r *SessionRecord) {
		r.ReplayPending = false
		r.LastActivityAtUTC = now.UnixNano()
	})
	c.syncMu.Unlock()

	if err := c.repo.UpdateState(ctx, sessionID,
		StateAttached.String(), nil, nil, nil, nil, &f, &now); err != nil {
		return err
	}

	c.syncMu.Lock()
	if cur, ok := c.sessions[sessionID]; ok &&
		cur.AttachmentState == StateAttached &&
		cur.AttachedClientConnectionID == clientConnectionID {
		c.sessions[sessionID] = staged
	}
	c.syncMu.Unlock()
	return nil
}

// RebindActiveSessions moves every (userID, workerID) session whose
// workerConnectionID has changed to the supplied connection. Recovering
// sessions flip to Attached.
func (c *Coordinator) RebindActiveSessions(ctx context.Context, userID, workerID, workerConnectionID string) (int, error) {
	c.syncMu.Lock()
	var staged []SessionRecord
	now := c.clock()
	for _, session := range c.sessions {
		if session.UserID != userID || session.WorkerID != workerID {
			continue
		}
		if session.AttachmentState == StateRecovering {
			staged = append(staged, session.Update(func(r *SessionRecord) {
				r.WorkerConnectionID = workerConnectionID
				r.AttachmentState = StateAttached
				r.LastActivityAtUTC = now.UnixNano()
			}))
			continue
		}
		if session.AttachmentState != StateAttached && session.AttachmentState != StateDetachedGracePeriod {
			continue
		}
		if session.WorkerConnectionID == workerConnectionID {
			continue
		}
		staged = append(staged, session.Update(func(r *SessionRecord) {
			r.WorkerConnectionID = workerConnectionID
		}))
	}
	c.syncMu.Unlock()

	if len(staged) == 0 {
		return 0, nil
	}

	for _, s := range staged {
		if err := c.repo.UpdateState(ctx, s.SessionID,
			StateAttached.String(), nil, &workerConnectionID, nil, nil, nil, ptrTime(now)); err != nil {
			return 0, err
		}
		if s.AttachmentState == StateAttached && s.LastActivityAtUTC != 0 {
			c.logger.Info("session.rebound", "session", s.SessionID, "worker", workerID)
		}
	}

	c.syncMu.Lock()
	for _, s := range staged {
		c.sessions[s.SessionID] = s
	}
	c.syncMu.Unlock()
	return len(staged), nil
}

// TransitionToRecovering moves every (workerID, workerConnectionID) Active
// session into Recovering. Returns the original records so the caller can
// broadcast SessionExited / abort replay.
func (c *Coordinator) TransitionToRecovering(ctx context.Context, workerID, workerConnectionID string) ([]SessionRecord, error) {
	c.syncMu.Lock()
	var originals []SessionRecord
	var staged []SessionRecord
	now := c.clock()
	emptyWC := ""
	for _, session := range c.sessions {
		if session.WorkerID != workerID ||
			session.WorkerConnectionID != workerConnectionID ||
			(session.AttachmentState != StateAttached && session.AttachmentState != StateDetachedGracePeriod) {
			continue
		}
		originals = append(originals, session)
		staged = append(staged, session.Update(func(r *SessionRecord) {
			r.AttachmentState = StateRecovering
			r.WorkerConnectionID = ""
			r.AttachedClientConnectionID = ""
			r.ReplayPending = false
			r.LastActivityAtUTC = now.UnixNano()
		}))
	}
	c.syncMu.Unlock()

	for _, s := range staged {
		attached := ""
		if err := c.repo.UpdateState(ctx, s.SessionID,
			StateRecovering.String(), &attached, &emptyWC, nil, nil,
			ptrBool(false), ptrTime(now)); err != nil {
			return nil, err
		}
		c.logger.Info("session.recovering",
			"session", s.SessionID,
			"reason", "worker-disconnect",
			"worker", workerID)
	}

	c.syncMu.Lock()
	for _, s := range staged {
		c.sessions[s.SessionID] = s
	}
	c.syncMu.Unlock()
	return originals, nil
}

// ReconcileWorkerSessions expires every (userID, workerID) session that
// the worker's ReportWorkerSessions snapshot didn't include.
func (c *Coordinator) ReconcileWorkerSessions(
	ctx context.Context, userID, workerID string, live []string,
) ([]string, error) {
	liveSet := make(map[string]struct{}, len(live))
	for _, id := range live {
		liveSet[id] = struct{}{}
	}
	now := c.clock()
	reason := "worker-restart"

	c.syncMu.Lock()
	var staged []SessionRecord
	for _, session := range c.sessions {
		if session.UserID != userID || session.WorkerID != workerID {
			continue
		}
		// Shield Attached sessions with a live client (genuine reattach).
		if session.AttachmentState == StateAttached && session.AttachedClientConnectionID != "" {
			continue
		}
		if session.AttachmentState != StateAttached &&
			session.AttachmentState != StateDetachedGracePeriod &&
			session.AttachmentState != StateRecovering {
			continue
		}
		if _, ok := liveSet[session.SessionID]; ok {
			continue
		}
		staged = append(staged, session.Update(func(r *SessionRecord) {
			r.AttachmentState = StateExpired
			r.AttachedClientConnectionID = ""
			r.ExitCode = nil
			r.ExitReason = reason
			r.ReplayPending = false
			r.LastActivityAtUTC = now.UnixNano()
		}))
	}
	c.syncMu.Unlock()

	for _, s := range staged {
		attached := ""
		if err := c.repo.UpdateState(ctx, s.SessionID,
			StateExpired.String(), &attached, nil, nil, &reason, ptrBool(false), ptrTime(now)); err != nil {
			return nil, err
		}
		c.logger.Info("session.expired", "session", s.SessionID, "reason", reason, "worker", workerID)
	}

	c.syncMu.Lock()
	for _, s := range staged {
		c.sessions[s.SessionID] = s
	}
	c.syncMu.Unlock()

	if len(staged) > 0 {
		c.logger.Info("Reconciled worker, expired ghost sessions",
			"worker", workerID, "count", len(staged))
	}
	ids := make([]string, len(staged))
	for i, s := range staged {
		ids[i] = s.SessionID
	}
	return ids, nil
}

// MarkSessionStartFailed moves a session to Exited with the given reason.
func (c *Coordinator) MarkSessionStartFailed(ctx context.Context, sessionID, reason string) error {
	now := c.clock()
	c.syncMu.Lock()
	session, ok := c.sessions[sessionID]
	if !ok {
		c.syncMu.Unlock()
		return nil
	}
	staged := session.Update(func(r *SessionRecord) {
		r.AttachmentState = StateExited
		r.AttachedClientConnectionID = ""
		r.ExitCode = nil
		r.ExitReason = reason
		r.ReplayPending = false
		r.LastActivityAtUTC = now.UnixNano()
	})
	c.syncMu.Unlock()

	attached := ""
	if err := c.repo.UpdateState(ctx, sessionID,
		StateExited.String(), &attached, nil, nil, &reason, ptrBool(false), ptrTime(now)); err != nil {
		return err
	}
	c.syncMu.Lock()
	if _, ok := c.sessions[sessionID]; ok {
		c.sessions[sessionID] = staged
	}
	c.syncMu.Unlock()
	c.logger.Info("session.start-failed", "session", sessionID, "reason", reason)
	return nil
}

// MarkSessionExited moves a session to Exited with the given code.
func (c *Coordinator) MarkSessionExited(ctx context.Context, sessionID string, exitCode int, reason string) error {
	now := c.clock()
	c.syncMu.Lock()
	session, ok := c.sessions[sessionID]
	if !ok {
		c.syncMu.Unlock()
		return nil
	}
	staged := session.Update(func(r *SessionRecord) {
		r.AttachmentState = StateExited
		r.AttachedClientConnectionID = ""
		r.ExitCode = &exitCode
		r.ExitReason = reason
		r.ReplayPending = false
		r.LastActivityAtUTC = now.UnixNano()
	})
	c.syncMu.Unlock()

	attached := ""
	if err := c.repo.UpdateState(ctx, sessionID,
		StateExited.String(), &attached, nil, &exitCode, &reason, ptrBool(false), ptrTime(now)); err != nil {
		return err
	}
	c.syncMu.Lock()
	if _, ok := c.sessions[sessionID]; ok {
		c.sessions[sessionID] = staged
	}
	c.syncMu.Unlock()
	c.logger.Info("session.exited", "session", sessionID, "exitCode", exitCode, "reason", reason)
	return nil
}

// RemoveSession deletes the row + the in-memory mirror.
func (c *Coordinator) RemoveSession(ctx context.Context, sessionID string) error {
	c.syncMu.Lock()
	delete(c.sessions, sessionID)
	delete(c.lastTouchBySession, sessionID)
	c.syncMu.Unlock()
	c.logger.Info("session.removed", "session", sessionID)
	return c.repo.Delete(ctx, sessionID)
}

// TryGetSession is the hot-path read. Returns false when unknown.
func (c *Coordinator) TryGetSession(sessionID string) (SessionRecord, bool) {
	c.syncMu.RLock()
	defer c.syncMu.RUnlock()
	s, ok := c.sessions[sessionID]
	return s, ok
}

// TouchSessionActivity bumps last_activity_at_utc if the new value is
// at least 5 seconds ahead of the last touch.
func (c *Coordinator) TouchSessionActivity(sessionID string, now time.Time) bool {
	c.syncMu.Lock()
	last, hasLast := c.lastTouchBySession[sessionID]
	if hasLast && now.Sub(last).Seconds() < 5.0 {
		c.syncMu.Unlock()
		return false
	}
	session, ok := c.sessions[sessionID]
	if !ok {
		c.syncMu.Unlock()
		return false
	}
	staged := session.Update(func(r *SessionRecord) {
		r.LastActivityAtUTC = now.UnixNano()
	})
	c.lastTouchBySession[sessionID] = now
	c.sessions[sessionID] = staged
	c.syncMu.Unlock()

	// Fire-and-forget DB touch — matches the C# behaviour.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.repo.TouchActivity(ctx, sessionID, now); err != nil {
			c.logger.Debug("touch activity persist failed", "session", sessionID, "err", err)
		}
	}()
	return true
}

// RenameSession updates the user-set name. Mirrors RenameSessionAsync.
func (c *Coordinator) RenameSession(ctx context.Context, userID, sessionID, name string) (RenameSessionResult, error) {
	c.syncMu.Lock()
	session, ok := c.sessions[sessionID]
	if !ok || session.UserID != userID {
		c.syncMu.Unlock()
		return RenameSessionResult{Error: "session-not-found"}, nil
	}
	staged := session.Update(func(r *SessionRecord) { r.Name = name })
	c.syncMu.Unlock()

	entity, err := c.repo.FindForUser(ctx, sessionID, userID)
	if err != nil {
		return RenameSessionResult{Error: err.Error()}, err
	}
	if entity == nil {
		return RenameSessionResult{Error: "session-not-found"}, nil
	}
	// Update name via a minimal UPDATE — schema doesn't carry a `name`
	// column in this slice, so we just keep the in-memory mirror consistent
	// and the DB no-op. Future slice adds the column.
	c.syncMu.Lock()
	if cur, ok := c.sessions[sessionID]; ok && cur.UserID == userID {
		c.sessions[sessionID] = staged
	}
	c.syncMu.Unlock()
	_ = entity
	return RenameSessionResult{Success: true}, nil
}

// ListSessionsForUser returns every session belonging to userID.
func (c *Coordinator) ListSessionsForUser(ctx context.Context, userID string) ([]SessionRecord, error) {
	entities, err := c.repo.ListForUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]SessionRecord, len(entities))
	for i, e := range entities {
		out[i] = MapEntityToRecord(&e)
	}
	return out, nil
}

// ListActiveSessions returns the in-memory Attached/DetachedGracePeriod rows.
func (c *Coordinator) ListActiveSessions() []SessionRecord {
	c.syncMu.RLock()
	defer c.syncMu.RUnlock()
	out := make([]SessionRecord, 0, len(c.sessions))
	for _, s := range c.sessions {
		if s.AttachmentState == StateAttached || s.AttachmentState == StateDetachedGracePeriod {
			out = append(out, s)
		}
	}
	return out
}

// --- helpers ---

func ptrBool(b bool) *bool { return &b }

func ptrTime(t time.Time) *time.Time { return &t }

// EnsureSessionID wraps a string sessionID in an error.
func EnsureSessionID(s string) error {
	if s == "" {
		return errors.New("empty session id")
	}
	return nil
}

// CreateSessionRequest mirrors CortexTerminal.Contracts.Sessions.CreateSessionRequest.
// Defined in the signalr package already; we re-declare here as a separate
// type so the sessions package doesn't depend on signalr.
type CreateSessionRequest struct {
	WorkerID         string
	Columns          int
	Rows             int
	ClientRequestID  string
}

// ReattachSessionRequest mirrors CortexTerminal.Contracts.Sessions.ReattachSessionRequest.
type ReattachSessionRequest struct {
	SessionID string
}