package sessions

import (
	"context"
	"log/slog"
	"time"
)

// ArtifactCleanupInterval is the C# default — 5 minutes. The C#
// implementation runs as an IHostedService that fires every 5 minutes
// against ArtifactService.CleanExpiredAsync.
const ArtifactCleanupInterval = 5 * time.Minute

// ArtifactCleanupRunner is the Go port of
// CortexTerminal.Gateway.Sessions.ArtifactCleanupHostedService.
// Run it in a goroutine from main; cancel ctx to stop.
type ArtifactCleanupRunner struct {
	service *ArtifactService
	logger  *slog.Logger
}

func NewArtifactCleanupRunner(service *ArtifactService, logger *slog.Logger) *ArtifactCleanupRunner {
	if logger == nil {
		logger = slog.Default()
	}
	return &ArtifactCleanupRunner{service: service, logger: logger}
}

// Run blocks until ctx is cancelled, calling CleanExpired every
// ArtifactCleanupInterval. Errors are logged but don't stop the loop —
// matches C# behaviour where transient S3 outages don't kill the service.
func (r *ArtifactCleanupRunner) Run(ctx context.Context) {
	r.logger.Info("artifact cleanup started",
		"interval", ArtifactCleanupInterval.String())
	ticker := time.NewTicker(ArtifactCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("artifact cleanup stopping")
			return
		case <-ticker.C:
			n, err := r.service.CleanExpired(ctx)
			if err != nil {
				r.logger.Warn("artifact cleanup pass failed", "err", err)
				continue
			}
			if n > 0 {
				r.logger.Info("artifact cleanup pass", "expired", n)
			}
		}
	}
}