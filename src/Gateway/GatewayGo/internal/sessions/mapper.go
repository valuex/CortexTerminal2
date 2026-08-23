package sessions

import (
	"time"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
)

// MapEntityToRecord converts a SessionEntity row from the data layer into
// the in-memory SessionRecord the coordinator uses. Mirrors
// DbSessionCoordinator.MapEntityToRecord.
func MapEntityToRecord(e *data.SessionEntity) SessionRecord {
	rec := SessionRecord{
		SessionID:          e.SessionID,
		UserID:             e.UserID,
		WorkerID:           e.WorkerID,
		WorkerConnectionID: e.WorkerConnectionID.String,
		Columns:            e.Columns,
		Rows:               e.Rows,
		AttachmentState:    ParseAttachmentState(e.AttachmentState),
		ReplayPending:      e.ReplayPending,
	}
	if e.AttachedClientConnectionID.Valid {
		rec.AttachedClientConnectionID = e.AttachedClientConnectionID.String
	}
	if e.ExitCode.Valid {
		v := int(e.ExitCode.Int64)
		rec.ExitCode = &v
	}
	if e.ExitReason.Valid {
		rec.ExitReason = e.ExitReason.String
	}
	if t, err := time.Parse(time.RFC3339Nano, e.CreatedAtUTC); err == nil {
		rec.CreatedAtUTC = t.UnixNano()
	}
	if t, err := time.Parse(time.RFC3339Nano, e.LastActivityAtUTC); err == nil {
		rec.LastActivityAtUTC = t.UnixNano()
	}
	return rec
}