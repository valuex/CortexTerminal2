package signalr

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// WorkerHub is the Worker-side hub. Method names match the C# WorkerHub
// verbatim because the existing C# Worker daemon calls them by name on the
// wire:
//
//	RegisterWorker, UpdateWorkerInfo, ReportWorkerSessions,
//	ForwardStdout, ForwardStderr, ForwardLatencyProbe,
//	SessionStartFailed, SessionExited,
//	ForwardAgentStarted, ForwardAgentPromptSubmitted, ForwardAgentToolCall,
//	ForwardAgentStopped, ForwardAgentSessionEnded, ForwardAgentSubagentStopped,
//	ForwardAgentNotified, ForwardAgentCompacting, ForwardAgentTitleUpdated,
//	RequestArtifactUploadUrl, CompleteArtifactUpload, ReportArtifactDeleted
//
// Each method delegates to a subsystem interface. Real implementations land
// in the Sessions/Workers/AgentActivity porting slices; for now only
// RegisterWorker is exercised end-to-end (see cmd/signalr-msgpack-smoke).
type WorkerHub struct {
	logger *slog.Logger

	Workers       WorkerRegistry
	Sessions      WorkerSessionCoordinator
	Replay        ReplayCoordinator
	AuditLog      AuditLogRecorder
	Terminal      WorkerHubCaller       // sends messages to Console clients on /hubs/terminal
	Stats         WorkerStatsLifecycle
	SessionStats  SessionStatsRecorder
	Artifacts     ArtifactService
	AgentActivity AgentActivityService
}

// WorkerRegistry mirrors IWorkerRegistry from the C# Gateway. Only the
// methods WorkerHub needs today are listed; the rest land with the registry
// port.
type WorkerRegistry interface {
	Register(workerID, connectionID, ownerUserID string)
	Unregister(workerID string)
	FindByConnectionID(connectionID string) (WorkerHandle, bool)
	PersistMetadata(workerID string, meta WorkerMetadataWire)
	UpdateMetrics(workerID string, metrics WorkerMetricsWire)
}

// WorkerHandle mirrors the C# RegisteredWorker struct.
type WorkerHandle struct {
	WorkerID    string
	OwnerUserID string
}

// WorkerMetadataWire matches CortexTerminal.Contracts.Workers.WorkerMetadata.
type WorkerMetadataWire struct {
	Hostname       string `json:"hostname" msgpack:"hostname"`
	OperatingSystem string `json:"operatingSystem" msgpack:"operatingSystem"`
	Architecture   string `json:"architecture" msgpack:"architecture"`
	MachineName    string `json:"machineName" msgpack:"machineName"`
	Version        string `json:"version" msgpack:"version"`
}

// WorkerMetricsWire matches CortexTerminal.Contracts.Workers.WorkerMetrics.
type WorkerMetricsWire struct {
	CpuUsagePercent    float64 `json:"cpuUsagePercent" msgpack:"cpuUsagePercent"`
	MemoryUsagePercent float64 `json:"memoryUsagePercent" msgpack:"memoryUsagePercent"`
}

// WorkerSessionCoordinator is the slice of ISessionCoordinator that
// WorkerHub uses.
type WorkerSessionCoordinator interface {
	RebindActiveSessions(ctx context.Context, userID, workerID, connectionID string) (int, error)
	ReconcileWorkerSessions(ctx context.Context, userID, workerID string, live []string) ([]string, error)
	TryGetSession(sessionID string) (SessionRecord, bool)
	TouchSessionActivity(sessionID string, ts int64)
	TransitionToRecovering(workerID, connectionID string) ([]SessionRecord, error)
	MarkSessionStartFailed(sessionID, reason string) error
	RemoveSession(sessionID string) error
}

// AuditLogRecorder is the minimal subset of IAuditLogStore WorkerHub uses.
type AuditLogRecorder interface {
	RecordWorkerConnect(userID, workerID string)
}

// WorkerHubCaller is the slice of IHubContext<TerminalHub> WorkerHub uses
// to push frames to Console clients.
type WorkerHubCaller interface {
	SendStdoutChunk(connectionID string, chunk TerminalChunkWire) error
	SendStderrChunk(connectionID string, chunk TerminalChunkWire) error
	SendLatencyProbeAck(connectionID string, ack LatencyAckWire) error
	SendSessionStartFailed(connectionID string, evt SessionStartFailedWire) error
	SendSessionExited(connectionID string, evt SessionExitedWire) error
}

// LatencyAckWire matches the WsLatencyAckFrame the C# Worker receives.
type LatencyAckWire struct {
	ProbeID    string `json:"probeId" msgpack:"probeId"`
	ClientTime int64  `json:"clientTime" msgpack:"clientTime"`
	ServerTime int64  `json:"serverTime" msgpack:"serverTime"`
}

// SessionStartFailedWire matches CortexTerminal.Contracts.Sessions.SessionStartFailedEvent.
type SessionStartFailedWire struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Reason    string `json:"reason" msgpack:"reason"`
}

// SessionExitedWire matches CortexTerminal.Contracts.Sessions.SessionExited.
type SessionExitedWire struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	ExitCode  int    `json:"exitCode" msgpack:"exitCode"`
	Reason    string `json:"reason,omitempty" msgpack:"reason,omitempty"`
}

// WorkerStatsLifecycle is the slice of IGatewayStatsService WorkerHub uses.
type WorkerStatsLifecycle interface {
	RecordBytesTransferred(bytes int64)
}

// SessionStatsRecorder is the slice of ISessionStatsService WorkerHub uses.
type SessionStatsRecorder interface {
	RecordBytes(sessionID, userID string, bytes int64)
}

// ArtifactService mirrors IArtifactService (worker-side entry points only).
type ArtifactService interface {
	CreateForWorkerUpload(ctx context.Context, connectionID, userID string, req CreateArtifactRequestWire) (UploadUrlResponseWire, error)
	CompleteWorkerUpload(ctx context.Context, connectionID, userID, artifactID, contentSHA256 string) error
	DeleteByWorker(ctx context.Context, sessionID, filename string) error
}

// CreateArtifactRequestWire matches CortexTerminal.Contracts.Artifacts.CreateArtifactRequest.
type CreateArtifactRequestWire struct {
	SessionID       string `json:"sessionId" msgpack:"sessionId"`
	Filename        string `json:"filename" msgpack:"filename"`
	ContentType      string `json:"contentType" msgpack:"contentType"`
	ContentLength   int64  `json:"contentLength" msgpack:"contentLength"`
	ContentSHA256   string `json:"contentSha256" msgpack:"contentSha256"`
}

// UploadUrlResponseWire matches CortexTerminal.Contracts.Artifacts.UploadUrlResponse.
type UploadUrlResponseWire struct {
	ArtifactID string `json:"artifactId" msgpack:"artifactId"`
	UploadURL  string `json:"uploadUrl" msgpack:"uploadUrl"`
	UploadMethod string `json:"uploadMethod" msgpack:"uploadMethod"`
	UploadHeaders map[string]string `json:"uploadHeaders,omitempty" msgpack:"uploadHeaders,omitempty"`
}

// AgentActivityService mirrors IAgentActivityService (worker-side entry points).
type AgentActivityService interface {
	HandleAgentStarted(ctx context.Context, sessionID, connectionID string, frame AgentStartedWire) error
	HandleAgentPromptSubmitted(ctx context.Context, sessionID, connectionID string, frame AgentPromptWire) error
	HandleAgentToolCall(ctx context.Context, sessionID, connectionID string, frame AgentToolCallWire) error
	HandleAgentStopped(ctx context.Context, sessionID, connectionID string, frame AgentStoppedWire) error
	HandleAgentSessionEnded(ctx context.Context, sessionID, connectionID string, frame AgentSessionEndedWire) error
	HandleAgentSubagentStopped(ctx context.Context, sessionID, connectionID string, frame AgentSubagentStoppedWire) error
	HandleAgentNotified(ctx context.Context, sessionID, connectionID string, frame AgentNotifiedWire) error
	HandleAgentCompacting(ctx context.Context, sessionID, connectionID string, frame AgentCompactingWire) error
	HandleAgentTitleUpdated(ctx context.Context, sessionID, connectionID string, frame AgentTitleUpdatedWire) error
}

// AgentStartedWire matches CortexTerminal.Contracts.Streaming.AgentStartedFrame.
type AgentStartedWire struct {
	SessionID      string `json:"sessionId" msgpack:"sessionId"`
	Kind           string `json:"kind" msgpack:"kind"`
	AgentSessionID string `json:"agentSessionId" msgpack:"agentSessionId"`
}

// AgentPromptWire matches CortexTerminal.Contracts.Streaming.AgentPromptSubmittedFrame.
type AgentPromptWire struct {
	SessionID  string `json:"sessionId" msgpack:"sessionId"`
	PromptText string `json:"promptText" msgpack:"promptText"`
}

// AgentToolCallWire matches CortexTerminal.Contracts.Streaming.AgentToolCallFrame.
type AgentToolCallWire struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	ToolName  string `json:"toolName" msgpack:"toolName"`
	IsError   bool   `json:"isError" msgpack:"isError"`
}

// AgentStoppedWire matches CortexTerminal.Contracts.Streaming.AgentStoppedFrame.
type AgentStoppedWire struct {
	SessionID    string  `json:"sessionId" msgpack:"sessionId"`
	TotalCostUsd float64 `json:"totalCostUsd" msgpack:"totalCostUsd"`
}

// AgentSessionEndedWire matches CortexTerminal.Contracts.Streaming.AgentSessionEndedFrame.
type AgentSessionEndedWire struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Reason    string `json:"reason" msgpack:"reason"`
}

// AgentSubagentStoppedWire matches CortexTerminal.Contracts.Streaming.AgentSubagentStoppedFrame.
type AgentSubagentStoppedWire struct {
	SessionID  string `json:"sessionId" msgpack:"sessionId"`
	SubagentID string `json:"subagentId" msgpack:"subagentId"`
}

// AgentNotifiedWire matches CortexTerminal.Contracts.Streaming.AgentNotifiedFrame.
type AgentNotifiedWire struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Title     string `json:"title" msgpack:"title"`
}

// AgentCompactingWire matches CortexTerminal.Contracts.Streaming.AgentCompactingFrame.
type AgentCompactingWire struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Trigger   string `json:"trigger" msgpack:"trigger"`
}

// AgentTitleUpdatedWire matches CortexTerminal.Contracts.Streaming.AgentTitleUpdatedFrame.
type AgentTitleUpdatedWire struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Title     string `json:"title" msgpack:"title"`
}

// WorkerInfoFrame matches CortexTerminal.Contracts.Workers.WorkerInfoFrame.
type WorkerInfoFrame struct {
	Hostname             string  `json:"hostname" msgpack:"hostname"`
	OperatingSystem      string  `json:"operatingSystem" msgpack:"operatingSystem"`
	Architecture         string  `json:"architecture" msgpack:"architecture"`
	MachineName          string  `json:"machineName" msgpack:"machineName"`
	Version              string  `json:"version" msgpack:"version"`
	CpuUsagePercent      float64 `json:"cpuUsagePercent" msgpack:"cpuUsagePercent"`
	MemoryUsagePercent   float64 `json:"memoryUsagePercent" msgpack:"memoryUsagePercent"`
}

// WorkerSessionsSnapshot matches CortexTerminal.Contracts.Workers.WorkerSessionsSnapshot.
type WorkerSessionsSnapshot struct {
	WorkerID        string   `json:"workerId" msgpack:"workerId"`
	LiveSessionIDs  []string `json:"liveSessionIds,omitempty" msgpack:"liveSessionIds,omitempty"`
}

// ReportArtifactDeletedFrame matches CortexTerminal.Contracts.Artifacts.ReportArtifactDeletedFrame.
type ReportArtifactDeletedFrame struct {
	SessionID string `json:"sessionId" msgpack:"sessionId"`
	Filename  string `json:"filename" msgpack:"filename"`
}

// CompleteArtifactRequestWire matches CortexTerminal.Contracts.Artifacts.CompleteArtifactRequest.
type CompleteArtifactRequestWire struct {
	ArtifactID    string `json:"artifactId" msgpack:"artifactId"`
	ContentSHA256 string `json:"contentSha256" msgpack:"contentSha256"`
}

// CompleteArtifactAckWire matches CortexTerminal.Contracts.Artifacts.CompleteArtifactAck.
type CompleteArtifactAckWire struct {
	Success bool   `json:"success" msgpack:"success"`
	Error   string `json:"error,omitempty" msgpack:"error,omitempty"`
}

// NewWorkerHub builds a WorkerHub. Subsystems may be nil; the matching
// method returns ErrNotImplemented until they're wired.
func NewWorkerHub(opts ...func(*WorkerHub)) *WorkerHub {
	h := &WorkerHub{logger: slog.Default()}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (*WorkerHub) Name() string { return "worker" }

func (h *WorkerHub) OnConnected(_ context.Context, _ *Connection) {
	// No-op — registration happens via RegisterWorker.
}

func (h *WorkerHub) OnDisconnected(ctx context.Context, conn *Connection, _ error) {
	if h.Workers == nil || h.Sessions == nil || h.Replay == nil {
		return
	}
	worker, ok := h.Workers.FindByConnectionID(conn.ID)
	if !ok {
		return
	}
	h.Workers.Unregister(worker.WorkerID)
	transitioned, err := h.Sessions.TransitionToRecovering(worker.WorkerID, conn.ID)
	if err != nil {
		h.logger.Warn("transition to recovering failed", "worker", worker.WorkerID, "err", err)
		return
	}
	for _, s := range transitioned {
		h.Replay.AbortReplay(s.SessionID)
	}
}

func (*WorkerHub) OnPing(_ context.Context, _ *Connection) {}

// OnInvocation dispatches by the target method name. Method names match
// the C# WorkerHub 1:1.
func (h *WorkerHub) OnInvocation(ctx context.Context, conn *Connection, inv Invocation) (any, error) {
	if conn.UserID == "" {
		return nil, fmt.Errorf("unauthenticated")
	}
	switch inv.Target {
	case "RegisterWorker":
		workerID, err := bindFirstStringArg(inv.Arguments)
		if err != nil {
			return nil, err
		}
		return nil, h.registerWorker(ctx, conn, workerID)
	case "UpdateWorkerInfo":
		var info WorkerInfoFrame
		if err := bindFirstArg(inv.Arguments, &info); err != nil {
			return nil, err
		}
		return nil, h.updateWorkerInfo(conn, info)
	case "ReportWorkerSessions":
		var snap WorkerSessionsSnapshot
		if err := bindFirstArg(inv.Arguments, &snap); err != nil {
			return nil, err
		}
		return nil, h.reportWorkerSessions(ctx, conn, snap)
	case "ForwardStdout":
		var chunk TerminalChunkWire
		if err := bindFirstArg(inv.Arguments, &chunk); err != nil {
			return nil, err
		}
		return nil, h.forwardStdout(ctx, conn, chunk)
	case "ForwardStderr":
		var chunk TerminalChunkWire
		if err := bindFirstArg(inv.Arguments, &chunk); err != nil {
			return nil, err
		}
		return nil, h.forwardStderr(ctx, conn, chunk)
	case "ForwardLatencyProbe":
		var frame ProbeLatencyFrame
		if err := bindFirstArg(inv.Arguments, &frame); err != nil {
			return nil, err
		}
		return nil, h.forwardLatencyProbe(ctx, conn, frame)
	case "SessionStartFailed":
		var evt SessionStartFailedWire
		if err := bindFirstArg(inv.Arguments, &evt); err != nil {
			return nil, err
		}
		return nil, h.sessionStartFailed(ctx, conn, evt)
	case "SessionExited":
		var evt SessionExitedWire
		if err := bindFirstArg(inv.Arguments, &evt); err != nil {
			return nil, err
		}
		return nil, h.sessionExited(ctx, conn, evt)
	default:
		return nil, fmt.Errorf("worker hub: unknown method %q", inv.Target)
	}
}

func (h *WorkerHub) registerWorker(ctx context.Context, conn *Connection, workerID string) error {
	if h.Workers == nil {
		return ErrNotImplemented
	}
	h.Workers.Register(workerID, conn.ID, conn.UserID)
	if h.Sessions != nil {
		if _, err := h.Sessions.RebindActiveSessions(ctx, conn.UserID, workerID, conn.ID); err != nil {
			h.logger.Warn("rebind active sessions failed", "worker", workerID, "err", err)
		}
	}
	if h.AuditLog != nil {
		h.AuditLog.RecordWorkerConnect(conn.UserID, workerID)
	}
	h.logger.Info("worker registered",
		"worker", workerID,
		"connection", conn.ID,
		"user", conn.UserID)
	return nil
}

func (h *WorkerHub) updateWorkerInfo(conn *Connection, info WorkerInfoFrame) error {
	if h.Workers == nil {
		return ErrNotImplemented
	}
	worker, ok := h.Workers.FindByConnectionID(conn.ID)
	if !ok {
		return nil // unknown connection — silently drop, matches C#
	}
	h.Workers.PersistMetadata(worker.WorkerID, WorkerMetadataWire{
		Hostname:        info.Hostname,
		OperatingSystem: info.OperatingSystem,
		Architecture:    info.Architecture,
		MachineName:     info.MachineName,
		Version:         info.Version,
	})
	h.Workers.UpdateMetrics(worker.WorkerID, WorkerMetricsWire{
		CpuUsagePercent:    info.CpuUsagePercent,
		MemoryUsagePercent: info.MemoryUsagePercent,
	})
	return nil
}

func (h *WorkerHub) reportWorkerSessions(ctx context.Context, conn *Connection, snap WorkerSessionsSnapshot) error {
	if h.Workers == nil || h.Sessions == nil {
		return ErrNotImplemented
	}
	worker, ok := h.Workers.FindByConnectionID(conn.ID)
	if !ok {
		return nil
	}
	live := snap.LiveSessionIDs
	if live == nil {
		live = []string{}
	}
	if _, err := h.Sessions.ReconcileWorkerSessions(ctx, conn.UserID, worker.WorkerID, live); err != nil {
		return err
	}
	return nil
}

func (h *WorkerHub) forwardStdout(ctx context.Context, conn *Connection, chunk TerminalChunkWire) error {
	if h.Sessions == nil {
		return ErrNotImplemented
	}
	session, ok := h.Sessions.TryGetSession(chunk.SessionID)
	if !ok {
		return nil
	}
	if session.WorkerConnectionID != conn.ID {
		return nil
	}
	h.Sessions.TouchSessionActivity(chunk.SessionID, nowUnixNano())
	if h.Stats != nil {
		h.Stats.RecordBytesTransferred(int64(len(chunk.Payload)))
	}
	if h.SessionStats != nil {
		h.SessionStats.RecordBytes(chunk.SessionID, session.UserID, int64(len(chunk.Payload)))
	}
	if h.Terminal != nil && session.AttachedClientConnectionID != "" {
		_ = h.Terminal.SendStdoutChunk(session.AttachedClientConnectionID, chunk)
	}
	return nil
}

func (h *WorkerHub) forwardStderr(ctx context.Context, conn *Connection, chunk TerminalChunkWire) error {
	if h.Sessions == nil {
		return ErrNotImplemented
	}
	session, ok := h.Sessions.TryGetSession(chunk.SessionID)
	if !ok {
		return nil
	}
	if session.WorkerConnectionID != conn.ID {
		return nil
	}
	if h.Stats != nil {
		h.Stats.RecordBytesTransferred(int64(len(chunk.Payload)))
	}
	if h.SessionStats != nil {
		h.SessionStats.RecordBytes(chunk.SessionID, session.UserID, int64(len(chunk.Payload)))
	}
	if h.Terminal != nil && session.AttachedClientConnectionID != "" {
		_ = h.Terminal.SendStderrChunk(session.AttachedClientConnectionID, chunk)
	}
	return nil
}

func (h *WorkerHub) forwardLatencyProbe(ctx context.Context, conn *Connection, frame ProbeLatencyFrame) error {
	if h.Sessions == nil {
		return ErrNotImplemented
	}
	session, ok := h.Sessions.TryGetSession(frame.SessionID)
	if !ok {
		return nil
	}
	if session.WorkerConnectionID != conn.ID {
		return nil
	}
	if h.Terminal != nil && session.AttachedClientConnectionID != "" {
		_ = h.Terminal.SendLatencyProbeAck(session.AttachedClientConnectionID, LatencyAckWire{
			ProbeID:    frame.ProbeID,
			ClientTime: frame.ClientTime,
			ServerTime: nowUnixNano() / int64(time.Millisecond),
		})
	}
	return nil
}

func (h *WorkerHub) sessionStartFailed(ctx context.Context, conn *Connection, evt SessionStartFailedWire) error {
	if h.Sessions == nil || h.Replay == nil {
		return ErrNotImplemented
	}
	session, ok := h.Sessions.TryGetSession(evt.SessionID)
	if !ok || session.WorkerConnectionID != conn.ID {
		return nil
	}
	if err := h.Sessions.MarkSessionStartFailed(evt.SessionID, evt.Reason); err != nil {
		return err
	}
	h.Replay.AbortReplay(evt.SessionID)
	if h.Terminal != nil && session.AttachedClientConnectionID != "" {
		_ = h.Terminal.SendSessionStartFailed(session.AttachedClientConnectionID, evt)
	}
	return nil
}

func (h *WorkerHub) sessionExited(ctx context.Context, conn *Connection, evt SessionExitedWire) error {
	if h.Sessions == nil || h.Replay == nil {
		return ErrNotImplemented
	}
	session, ok := h.Sessions.TryGetSession(evt.SessionID)
	if !ok || session.WorkerConnectionID != conn.ID {
		return nil
	}
	h.Replay.AbortReplay(evt.SessionID)
	if err := h.Sessions.RemoveSession(evt.SessionID); err != nil {
		return err
	}
	if h.Terminal != nil && session.AttachedClientConnectionID != "" {
		_ = h.Terminal.SendSessionExited(session.AttachedClientConnectionID, evt)
	}
	return nil
}