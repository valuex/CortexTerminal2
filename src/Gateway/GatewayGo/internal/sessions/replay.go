package sessions

import (
	"log/slog"
	"sync"
	"time"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/signalr"
)

// ReplayCoordinator is the Go port of CortexTerminal.Gateway.Sessions.ReplayCoordinator.
// It holds an in-memory ring buffer per session of frames that arrived while
// the Console was detached, then drains them on Reattach.
//
// Per-session ring caps at 4096 entries to bound memory when a Console stays
// away for hours. Older entries are evicted FIFO when the cap is hit.
type ReplayCoordinator struct {
	logger *slog.Logger

	mu      sync.Mutex
	buffers map[string]*replayBuffer
}

const replayCap = 4096

type replayBuffer struct {
	items []replayEntry
	open  bool // true while the session is detached; false after a successful drain
}

type replayEntry struct {
	enqueuedAt int64
	kind       replayKind
	stdout     signalr.TerminalChunkWire
	stderr     signalr.TerminalChunkWire
	probe      signalr.ProbeLatencyFrame
	startFail  signalr.SessionStartFailedWire
	exited     signalr.SessionExitedWire
}

type replayKind int

const (
	replayStdout replayKind = iota + 1
	replayStderr
	replayProbe
	replayStartFailed
	replayExited
)

func NewReplayCoordinator(logger *slog.Logger) *ReplayCoordinator {
	if logger == nil {
		logger = slog.Default()
	}
	return &ReplayCoordinator{
		logger:  logger,
		buffers: make(map[string]*replayBuffer),
	}
}

// OpenBuffer (re-)creates the buffer for sessionID and marks it open for
// writes. Called on DetachSession / TransitionToRecovering + after the
// Console disconnects.
func (r *ReplayCoordinator) OpenBuffer(sessionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	buf, ok := r.buffers[sessionID]
	if !ok {
		buf = &replayBuffer{open: true}
		r.buffers[sessionID] = buf
	} else {
		buf.open = true
	}
}

// CloseBuffer flips the buffer to read-only mode and drains it. Returns the
// buffered frames to the caller so it can replay them through the
// TerminalHub caller to the Console that just reattached. Mirrors
// ReplayCoordinator.ReplayAsync in C#.
func (r *ReplayCoordinator) CloseBuffer(sessionID string) ReplayBundle {
	r.mu.Lock()
	defer r.mu.Unlock()
	buf, ok := r.buffers[sessionID]
	if !ok {
		return ReplayBundle{}
	}
	buf.open = false
	bundle := ReplayBundle{
		Stdout:        append([]signalr.TerminalChunkWire(nil), buf.stdoutSlice()...),
		Stderr:        append([]signalr.TerminalChunkWire(nil), buf.stderrSlice()...),
		Probes:        append([]signalr.ProbeLatencyFrame(nil), buf.probeSlice()...),
		StartFailures: append([]signalr.SessionStartFailedWire(nil), buf.startFailSlice()...),
		ExitedEvents:  append([]signalr.SessionExitedWire(nil), buf.exitSlice()...),
	}
	// Drain and remove the buffer; future replays start fresh.
	delete(r.buffers, sessionID)
	return bundle
}

// AbortReplay clears any buffered entries for sessionID. Called when the
// session exits permanently so we don't replay to a ghost Console.
func (r *ReplayCoordinator) AbortReplay(sessionID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.buffers, sessionID)
}

// BufferStdout / BufferStderr / etc. are fire-and-forget: if no buffer is
// open (Console attached) the entry is silently dropped — the live delivery
// path through TerminalHub already sent it.
func (r *ReplayCoordinator) BufferStdout(sessionID string, chunk signalr.TerminalChunkWire) {
	r.push(sessionID, replayEntry{kind: replayStdout, stdout: chunk, enqueuedAt: time.Now().UnixNano()})
}

func (r *ReplayCoordinator) BufferStderr(sessionID string, chunk signalr.TerminalChunkWire) {
	r.push(sessionID, replayEntry{kind: replayStderr, stderr: chunk, enqueuedAt: time.Now().UnixNano()})
}

func (r *ReplayCoordinator) BufferProbe(sessionID string, frame signalr.ProbeLatencyFrame) {
	r.push(sessionID, replayEntry{kind: replayProbe, probe: frame, enqueuedAt: time.Now().UnixNano()})
}

func (r *ReplayCoordinator) BufferStartFailed(sessionID string, evt signalr.SessionStartFailedWire) {
	r.push(sessionID, replayEntry{kind: replayStartFailed, startFail: evt, enqueuedAt: time.Now().UnixNano()})
}

func (r *ReplayCoordinator) BufferExited(sessionID string, evt signalr.SessionExitedWire) {
	r.push(sessionID, replayEntry{kind: replayExited, exited: evt, enqueuedAt: time.Now().UnixNano()})
}

func (r *ReplayCoordinator) push(sessionID string, entry replayEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	buf, ok := r.buffers[sessionID]
	if !ok || !buf.open {
		return
	}
	if len(buf.items) >= replayCap {
		// Evict the oldest entry — the Console will see a gap, but bounded
		// memory wins. C# drops the oldest chunk too.
		buf.items = buf.items[1:]
	}
	buf.items = append(buf.items, entry)
}

// Size returns the current buffer depth — useful for /api/admin endpoints.
func (r *ReplayCoordinator) Size(sessionID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if buf, ok := r.buffers[sessionID]; ok {
		return len(buf.items)
	}
	return 0
}

// --- bundle accessors ---

func (b *replayBuffer) stdoutSlice() []signalr.TerminalChunkWire {
	out := make([]signalr.TerminalChunkWire, 0)
	for _, e := range b.items {
		if e.kind == replayStdout {
			out = append(out, e.stdout)
		}
	}
	return out
}

func (b *replayBuffer) stderrSlice() []signalr.TerminalChunkWire {
	out := make([]signalr.TerminalChunkWire, 0)
	for _, e := range b.items {
		if e.kind == replayStderr {
			out = append(out, e.stderr)
		}
	}
	return out
}

func (b *replayBuffer) probeSlice() []signalr.ProbeLatencyFrame {
	out := make([]signalr.ProbeLatencyFrame, 0)
	for _, e := range b.items {
		if e.kind == replayProbe {
			out = append(out, e.probe)
		}
	}
	return out
}

func (b *replayBuffer) startFailSlice() []signalr.SessionStartFailedWire {
	out := make([]signalr.SessionStartFailedWire, 0)
	for _, e := range b.items {
		if e.kind == replayStartFailed {
			out = append(out, e.startFail)
		}
	}
	return out
}

func (b *replayBuffer) exitSlice() []signalr.SessionExitedWire {
	out := make([]signalr.SessionExitedWire, 0)
	for _, e := range b.items {
		if e.kind == replayExited {
			out = append(out, e.exited)
		}
	}
	return out
}

// ReplayBundle is the drained payload returned from CloseBuffer. The caller
// is responsible for fanning it back out through the TerminalHub caller.
type ReplayBundle struct {
	Stdout        []signalr.TerminalChunkWire
	Stderr        []signalr.TerminalChunkWire
	Probes        []signalr.ProbeLatencyFrame
	StartFailures []signalr.SessionStartFailedWire
	ExitedEvents  []signalr.SessionExitedWire
}