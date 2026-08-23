// Command gateway is the entry point for GatewayGo — the Go port of the
// CortexTerminal2 backend. Wires config → logger → DB → router and serves
// HTTP on the configured listen address.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/config"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/server"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/sessions"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/signalr"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/storage"
)

func main() {
	configPath := flag.String("config", "config.example.yaml", "path to YAML config")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("load config", "err", err)
		os.Exit(1)
	}

	db, err := data.Open(cfg.Database.SQLitePath)
	if err != nil {
		logger.Error("open database", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	logger.Info("database ready", "path", cfg.Database.SQLitePath)

	if err := data.SeedDevUser(context.Background(), db); err != nil {
		logger.Error("seed dev user", "err", err)
		os.Exit(1)
	}

	terminalHub := signalr.NewTerminalHub()
	workerRegistry := signalr.NewInMemoryWorkerRegistry()
	workerHub := signalr.NewWorkerHub(func(h *signalr.WorkerHub) {
		h.Workers = workerRegistry
	})

	// Graceful shutdown: drain in-flight requests on SIGINT/SIGTERM.
	// Declared up front so background goroutines (artifact cleanup ticker)
	// can observe the same ctx.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Sessions subsystem — coordinator + launch (idempotent) + replay.
	sessionsRepo := data.NewSessionsRepo(db)
	sessionCoordinator := sessions.NewCoordinator(
		sessionsRepo,
		sessions.NewWorkerResolverAdapter(workerRegistry),
		logger,
	)
	sessionLaunch := sessions.NewSessionLaunchCoordinator(logger)
	sessionReplay := sessions.NewReplayCoordinator(logger)

	// Recover any sessions left Attached/DetachedGracePeriod/Recovering from
	// a previous gateway run. After restart they're all DetachedGracePeriod
	// until a Console reattaches.
	if err := sessionCoordinator.RecoverActiveSessions(context.Background()); err != nil {
		logger.Error("recover active sessions", "err", err)
		os.Exit(1)
	}

	// Wire sessions into both hubs.
	signalrCoordinator := sessions.NewSignalRSessionCoordinator(sessionCoordinator)
	signalrLaunch := sessions.NewSignalRLaunchCoordinator(sessionLaunch, sessionCoordinator)
	signalrReplay := sessions.NewSignalRReplayCoordinator(sessionReplay)
	signalrWorkerSessions := sessions.NewSignalRWorkerSessionCoordinator(sessionCoordinator)

	terminalHub.LaunchCoordinator = signalrLaunch
	terminalHub.Sessions = signalrCoordinator
	terminalHub.Replay = signalrReplay
	workerHub.Sessions = signalrWorkerSessions
	workerHub.Replay = signalrReplay

	// Artifact subsystem — S3 storage + service + cleanup ticker. Only
	// instantiated when storage is configured (matches C# startup
	// behaviour: missing S3 config logs a warning and leaves the artifact
	// endpoints returning 503).
	storageOpts := storage.ArtifactStorageOptions{
		Endpoint:               cfg.Storage.Endpoint,
		Bucket:                 cfg.Storage.Bucket,
		Region:                 cfg.Storage.Region,
		AccessKey:              cfg.Storage.AccessKey,
		SecretKey:              cfg.Storage.SecretKey,
		ForcePathStyle:         cfg.Storage.ForcePathStyle,
		PresignedUrlTtl:        cfg.Storage.PresignedURLTTL,
		MaxArtifactSizeBytes:   cfg.Storage.MaxArtifactSizeBytes,
		MaxArtifactAgeDays:     cfg.Storage.MaxArtifactAgeDays,
		GracePeriodHours:       cfg.Storage.GracePeriodHours,
		MaxArtifactsPerSession: cfg.Storage.MaxArtifactsPerSession,
	}.WithDefaults()

	var artifactService *sessions.ArtifactService
	if storageOpts.IsConfigured() {
		artifactStorage, err := storage.NewS3CompatibleArtifactStorage(context.Background(), storageOpts)
		if err != nil {
			logger.Error("create artifact storage", "err", err)
			os.Exit(1)
		}
		artifactsRepo := data.NewArtifactsRepo(db)
		artifactService = sessions.NewArtifactService(
			artifactsRepo, artifactStorage,
			sessionCoordinator, nil, nil, nil,
			storageOpts, logger,
		)
		workerHub.Artifacts = sessions.NewSignalRArtifactService(artifactService)
		cleanupRunner := sessions.NewArtifactCleanupRunner(artifactService, logger)
		go cleanupRunner.Run(ctx)
		logger.Info("artifact service ready",
			"bucket", storageOpts.Bucket,
			"endpoint", storageOpts.Endpoint)
	} else {
		logger.Warn("artifact storage not configured; artifact endpoints disabled",
			"hint", "set storage.bucket + storage.access_key + storage.secret_key in config.yaml")
	}

	router := server.New(
		signalr.NewServer([]signalr.Hub{terminalHub, workerHub}, signalr.ConnectionOptions{}),
		cfg.Auth.SigningKey,
		db,
	)
	_ = terminalHub
	_ = workerHub
	_ = workerRegistry

	srv := &http.Server{
		Addr:              cfg.Server.Listen,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		// Match C# Kestrel defaults — long enough for tunnel forward + S3 PUT.
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Graceful shutdown: drain in-flight requests on SIGINT/SIGTERM.

	go func() {
		logger.Info("listening", "addr", cfg.Server.Listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("listen", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown", "err", err)
	}
}
