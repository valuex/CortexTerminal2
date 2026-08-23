package tunnels

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
)

// Registry mirrors CortexTerminal.Gateway.Tunnels.TunnelRegistry.
// All queries automatically exclude revoked rows.
type Registry struct {
	repo  *data.TunnelsRepo
	clock func() time.Time
}

func NewRegistry(repo *data.TunnelsRepo, clock func() time.Time) *Registry {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Registry{repo: repo, clock: clock}
}

// CreateAsync-style method name preserved for the porting convention.
// Generates the row id, picks transport type = http, and stamps created /
// expires timestamps. Returns the entity + the plaintext secret the
// caller returns to the visitor.
func (r *Registry) Create(
	ctx context.Context,
	tunnelKey, secretHash, ownerUserID,
	workerID, workerConnectionID, sessionID string,
	port int, ttl time.Duration,
) (data.TunnelEntity, error) {
	now := r.clock()
	e := data.TunnelEntity{
		ID:                 uuid.NewString(),
		TunnelKey:          tunnelKey,
		OwnerUserID:        ownerUserID,
		WorkerID:           workerID,
		WorkerConnectionID: workerConnectionID,
		SessionID:          sessionID,
		Port:               port,
		SecretHash:         secretHash,
		TransportType:      data.TunnelTransportHTTP,
		CreatedAtUTC:       now.Format(time.RFC3339Nano),
		ExpiresAtUTC:       now.Add(ttl).Format(time.RFC3339Nano),
	}
	if err := r.repo.Insert(ctx, e); err != nil {
		return data.TunnelEntity{}, err
	}
	return e, nil
}

// FindByKey returns the active tunnel (revoked rows excluded).
func (r *Registry) FindByKey(ctx context.Context, tunnelKey string) (*data.TunnelEntity, error) {
	return r.repo.FindByKey(ctx, tunnelKey)
}

// ListForSession returns active tunnels for the (sessionID, ownerUserID)
// pair, oldest first.
func (r *Registry) ListForSession(ctx context.Context, sessionID, ownerUserID string) ([]data.TunnelEntity, error) {
	return r.repo.ListForSession(ctx, sessionID, ownerUserID)
}

// Revoke flips revoked_at_utc iff the tunnel is owned by ownerUserID
// and not already revoked. Returns false when the row is missing.
func (r *Registry) Revoke(ctx context.Context, tunnelID, ownerUserID string) (bool, error) {
	return r.repo.Revoke(ctx, tunnelID, ownerUserID, r.clock())
}

// CountActiveForSession returns the count of non-revoked tunnels for
// sessionID.
func (r *Registry) CountActiveForSession(ctx context.Context, sessionID string) (int, error) {
	return r.repo.CountActiveForSession(ctx, sessionID)
}

// ErrQuotaExceeded is returned by Create when the session already has
// MaxTunnelsPerSession tunnels. The middleware maps it to HTTP 429.
var ErrQuotaExceeded = errors.New("session tunnel quota exceeded")