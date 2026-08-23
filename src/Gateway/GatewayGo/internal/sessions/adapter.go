// Package sessions — adapter.go bridges the rich sessions.Coordinator
// surface (CreateSession / DetachSession / ReattachSession / TryGetSession /
// MarkReplayCompleted / etc.) into the narrow signalr.SessionCoordinator
// interface TerminalHub depends on. It also bridges signalr.TerminalHub's
// CreateSessionRequest/Result into the sessions-package types so neither
// package needs to import the other.
package sessions

import (
	"context"
	"sync"
	"time"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/signalr"
)

// SignalRSessionCoordinator adapts sessions.Coordinator into the
// signalr.SessionCoordinator interface so TerminalHub can call into it
// without depending on the sessions package types.
type SignalRSessionCoordinator struct {
	c *Coordinator
}

func NewSignalRSessionCoordinator(c *Coordinator) *SignalRSessionCoordinator {
	return &SignalRSessionCoordinator{c: c}
}

func (a *SignalRSessionCoordinator) DetachSession(ctx context.Context, userID, sessionID string, now signalrTime) error {
	return a.c.DetachSession(ctx, userID, sessionID, time.Unix(0, now.UnixNano))
}

func (a *SignalRSessionCoordinator) ReattachSession(
	ctx context.Context, userID string, request signalr.ReattachSessionRequest,
	connectionID string, now signalrTime,
) (signalr.ReattachSessionResult, error) {
	result, err := a.c.ReattachSession(ctx, userID, ReattachSessionRequest{
		SessionID: request.SessionID,
	}, connectionID, time.Unix(0, now.UnixNano))
	if result.Error != "" && !result.Success {
		return signalr.ReattachSessionResult{Error: result.Error}, err
	}
	return signalr.ReattachSessionResult{Success: true}, err
}

func (a *SignalRSessionCoordinator) TryGetSession(sessionID string) (signalr.SessionRecord, bool) {
	rec, ok := a.c.TryGetSession(sessionID)
	if !ok {
		return signalr.SessionRecord{}, false
	}
	return signalr.SessionRecord{
		SessionID:                  rec.SessionID,
		UserID:                     rec.UserID,
		WorkerConnectionID:         rec.WorkerConnectionID,
		AttachedClientConnectionID: rec.AttachedClientConnectionID,
	}, true
}

func (a *SignalRSessionCoordinator) MarkReplayCompleted(ctx context.Context, sessionID, connectionID string) error {
	return a.c.MarkReplayCompleted(ctx, sessionID, connectionID)
}

// signalrTime matches the timeNow alias in the signalr package — kept as
// a structural type so the signalr package does not need to import the
// sessions package or time.
type signalrTime = struct{ UnixNano int64 }

// SignalRWorkerSessionCoordinator bridges Coordinator into the
// signalr.WorkerSessionCoordinator interface WorkerHub depends on.
type SignalRWorkerSessionCoordinator struct {
	c *Coordinator
}

func NewSignalRWorkerSessionCoordinator(c *Coordinator) *SignalRWorkerSessionCoordinator {
	return &SignalRWorkerSessionCoordinator{c: c}
}

func (a *SignalRWorkerSessionCoordinator) RebindActiveSessions(ctx context.Context, userID, workerID, connectionID string) (int, error) {
	return a.c.RebindActiveSessions(ctx, userID, workerID, connectionID)
}

func (a *SignalRWorkerSessionCoordinator) ReconcileWorkerSessions(ctx context.Context, userID, workerID string, live []string) ([]string, error) {
	return a.c.ReconcileWorkerSessions(ctx, userID, workerID, live)
}

func (a *SignalRWorkerSessionCoordinator) TryGetSession(sessionID string) (signalr.SessionRecord, bool) {
	rec, ok := a.c.TryGetSession(sessionID)
	if !ok {
		return signalr.SessionRecord{}, false
	}
	return signalr.SessionRecord{
		SessionID:                  rec.SessionID,
		UserID:                     rec.UserID,
		WorkerConnectionID:         rec.WorkerConnectionID,
		AttachedClientConnectionID: rec.AttachedClientConnectionID,
	}, true
}

func (a *SignalRWorkerSessionCoordinator) TouchSessionActivity(sessionID string, ts int64) {
	a.c.TouchSessionActivity(sessionID, time.Unix(0, ts))
}

func (a *SignalRWorkerSessionCoordinator) TransitionToRecovering(workerID, connectionID string) ([]signalr.SessionRecord, error) {
	records, err := a.c.TransitionToRecovering(context.Background(), workerID, connectionID)
	if err != nil {
		return nil, err
	}
	out := make([]signalr.SessionRecord, len(records))
	for i, r := range records {
		out[i] = signalr.SessionRecord{
			SessionID:                  r.SessionID,
			UserID:                     r.UserID,
			WorkerConnectionID:         r.WorkerConnectionID,
			AttachedClientConnectionID: r.AttachedClientConnectionID,
		}
	}
	return out, nil
}

func (a *SignalRWorkerSessionCoordinator) MarkSessionStartFailed(sessionID, reason string) error {
	return a.c.MarkSessionStartFailed(context.Background(), sessionID, reason)
}

func (a *SignalRWorkerSessionCoordinator) RemoveSession(sessionID string) error {
	return a.c.RemoveSession(context.Background(), sessionID)
}

// SignalRLaunchCoordinator bridges SessionLaunchCoordinator into the
// signalr.SessionLaunchCoordinator interface TerminalHub depends on.
type SignalRLaunchCoordinator struct {
	l *SessionLaunchCoordinator
	c *Coordinator
}

func NewSignalRLaunchCoordinator(l *SessionLaunchCoordinator, c *Coordinator) *SignalRLaunchCoordinator {
	return &SignalRLaunchCoordinator{l: l, c: c}
}

func (a *SignalRLaunchCoordinator) CreateSession(
	ctx context.Context, userID string, req signalr.CreateSessionRequest, connectionID string,
) (signalr.CreateSessionResult, error) {
	result, err := a.l.RunLaunch(userID, req.ClientRequestID, connectionID, func() (CreateSessionResult, error) {
		return a.c.CreateSession(ctx, userID, CreateSessionRequest{
			WorkerID:        req.WorkerID,
			Columns:         80, // default — user-pref lookup lands in the read-side slice
			Rows:            24,
			ClientRequestID: req.ClientRequestID,
		}, connectionID)
	})
	return signalr.CreateSessionResult{
		Success:   result.Success,
		SessionID: result.SessionID,
		Error:     result.Error,
	}, err
}

// SignalRReplayCoordinator bridges ReplayCoordinator into the
// signalr.ReplayCoordinator interface.
type SignalRReplayCoordinator struct {
	r *ReplayCoordinator
}

func NewSignalRReplayCoordinator(r *ReplayCoordinator) *SignalRReplayCoordinator {
	return &SignalRReplayCoordinator{r: r}
}

func (a *SignalRReplayCoordinator) BeginReplay(sessionID, connectionID string) {
	a.r.OpenBuffer(sessionID)
}

func (a *SignalRReplayCoordinator) AbortReplay(sessionID string) {
	a.r.AbortReplay(sessionID)
}

func (a *SignalRReplayCoordinator) FlushPending(ctx context.Context, sessionID, connectionID string, send func(any) error) error {
	bundle := a.r.CloseBuffer(sessionID)
	for _, c := range bundle.Stdout {
		if err := send(c); err != nil {
			return err
		}
	}
	for _, c := range bundle.Stderr {
		if err := send(c); err != nil {
			return err
		}
	}
	for _, p := range bundle.Probes {
		if err := send(p); err != nil {
			return err
		}
	}
	for _, e := range bundle.StartFailures {
		if err := send(e); err != nil {
			return err
		}
	}
	for _, e := range bundle.ExitedEvents {
		if err := send(e); err != nil {
			return err
		}
	}
	return nil
}

// WorkerResolverAdapter bridges signalr.InMemoryWorkerRegistry into
// sessions.WorkerResolver. The C# DbWorkerRegistry surfaces session counts
// per worker; until that lands, the resolver picks workers round-robin
// from the owner's pool — good enough to make the slice exercisable.
type WorkerResolverAdapter struct {
	r *signalr.InMemoryWorkerRegistry

	mu        sync.Mutex
	rrCounter int
}

func NewWorkerResolverAdapter(r *signalr.InMemoryWorkerRegistry) *WorkerResolverAdapter {
	return &WorkerResolverAdapter{r: r}
}

func (a *WorkerResolverAdapter) TryGetWorker(workerID string) (WorkerHandle, bool) {
	conn, ok := a.r.FindByWorkerID(workerID)
	if !ok {
		return WorkerHandle{}, false
	}
	return WorkerHandle{
		WorkerID:     workerID,
		ConnectionID: conn,
		OwnerUserID:  a.r.LookupOwner(workerID),
	}, true
}

func (a *WorkerResolverAdapter) TryGetLeastBusyForUser(userID string) (WorkerHandle, bool) {
	candidates := a.r.ListForUser(userID)
	if len(candidates) == 0 {
		return WorkerHandle{}, false
	}
	a.mu.Lock()
	a.rrCounter++
	idx := a.rrCounter % len(candidates)
	a.mu.Unlock()
	pick := candidates[idx]
	conn, _ := a.r.FindByWorkerID(pick)
	return WorkerHandle{
		WorkerID:     pick,
		ConnectionID: conn,
		OwnerUserID:  userID,
	}, true
}

func (a *WorkerResolverAdapter) SetWorkerOwner(workerID, ownerUserID string) bool {
	a.r.SetOwner(workerID, ownerUserID)
	return true
}