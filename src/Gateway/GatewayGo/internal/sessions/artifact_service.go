package sessions

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/storage"
)

// SessionLookup is the slice of SessionCoordinator ArtifactService needs
// for ownership checks.
type SessionLookup interface {
	TryGetSession(sessionID string) (SessionRecord, bool)
}

// ArtifactBroadcaster fans out ArtifactChanged events to every
// connection owned by a user. The C# implementation calls
// IHubContext<TerminalHub>.Clients.User(userId).SendAsync("ArtifactChanged", …)
// plus the WebSocket equivalent for legacy clients.
type ArtifactBroadcaster interface {
	BroadcastArtifactChanged(userID string, evt ArtifactChangedEvent) error
}

// ArtifactWorkerNotifier is the slice of IArtifactCommandDispatcher the
// service uses to tell the Worker to mirror a file locally.
type ArtifactWorkerNotifier interface {
	NotifyArtifactUploaded(workerConnectionID string, frame NotifyArtifactUploadedFrame) error
}

// AuditLogger is the slice of IAuditLogStore the service uses.
type AuditLogger interface {
	Record(userID, action, targetEntity, targetID string)
}

// ArtifactService orchestrates the artifact lifecycle: ownership
// checks, DB state machine, S3 storage brokering, TTL bookkeeping,
// audit logging, and SignalR fan-out to every connection owned by the
// artifact's user. Mirrors C# ArtifactService.cs 1:1.
type ArtifactService struct {
	repo        *data.ArtifactsRepo
	storage     storage.ArtifactStorage
	sessions    SessionLookup
	audit       AuditLogger
	broadcaster ArtifactBroadcaster
	worker      ArtifactWorkerNotifier
	options     storage.ArtifactStorageOptions
	logger      *slog.Logger
	clock       func() time.Time
}

func NewArtifactService(
	repo *data.ArtifactsRepo,
	storage storage.ArtifactStorage,
	sessions SessionLookup,
	audit AuditLogger,
	broadcaster ArtifactBroadcaster,
	worker ArtifactWorkerNotifier,
	options storage.ArtifactStorageOptions,
	logger *slog.Logger,
) *ArtifactService {
	if logger == nil {
		logger = slog.Default()
	}
	return &ArtifactService{
		repo:        repo,
		storage:     storage,
		sessions:    sessions,
		audit:       audit,
		broadcaster: broadcaster,
		worker:      worker,
		options:     options,
		logger:      logger,
		clock:       func() time.Time { return time.Now().UTC() },
	}
}

// CreateForConsoleUpload is step 1 of the Console upload flow.
// Validates the request, INSERTs a Pending artifact row, and returns a
// presigned PUT URL the client uses to upload directly to S3. Mirrors
// CreateForConsoleUploadAsync.
func (s *ArtifactService) CreateForConsoleUpload(
	ctx context.Context, userID string, request CreateArtifactRequest,
) (storage.UploadURLResponse, error) {
	if !validOrigin(request.Origin) {
		return storage.UploadURLResponse{}, fmt.Errorf("invalid origin: %s", request.Origin)
	}
	filename, err := url.PathUnescape(request.Filename)
	if err != nil {
		filename = request.Filename
	}
	if reason := ValidateFilename(filename); reason != "" {
		return storage.UploadURLResponse{}, fmt.Errorf("invalid filename: %s", reason)
	}
	if request.SizeBytes <= 0 || request.SizeBytes > s.options.MaxArtifactSizeBytes {
		return storage.UploadURLResponse{}, fmt.Errorf("size must be between 1 and %d bytes", s.options.MaxArtifactSizeBytes)
	}

	if err := s.ensureSessionOwnedByUser(request.SessionID, userID); err != nil {
		return storage.UploadURLResponse{}, err
	}
	quota, err := s.repo.CountBySession(ctx, request.SessionID)
	if err != nil {
		return storage.UploadURLResponse{}, fmt.Errorf("quota check: %w", err)
	}
	if quota >= s.options.MaxArtifactsPerSession {
		return storage.UploadURLResponse{}, errors.New("session artifact quota exceeded")
	}
	// Console uploads reject duplicate filenames.
	dup, err := s.repo.FindBySessionFilename(ctx, request.SessionID, filename)
	if err != nil {
		return storage.UploadURLResponse{}, err
	}
	if dup != nil && dup.Status != ArtifactStatusDeleted {
		return storage.UploadURLResponse{}, errors.New("duplicate filename")
	}

	now := s.clock()
	expiresAt := s.computeExpiresAt(now, false, nil)
	entity := data.ArtifactEntity{
		ID:             uuid.NewString(),
		SessionID:      request.SessionID,
		Filename:       filename,
		SizeBytes:      request.SizeBytes,
		Status:         ArtifactStatusPending,
		Origin:         ArtifactOriginConsole,
		OwnerUserID:    userID,
		ContentSHA256:  toNullString(request.ContentSHA256),
		FileCategory:   DetectFileCategory(filename),
		CreatedAtUTC:   now.Format(time.RFC3339Nano),
		ExpiresAtUTC:   expiresAt.Format(time.RFC3339Nano),
	}
	if err := s.repo.Insert(ctx, entity); err != nil {
		return storage.UploadURLResponse{}, fmt.Errorf("insert: %w", err)
	}

	upload, err := s.storage.GenerateUploadURL(request.SessionID, filename)
	if err != nil {
		return storage.UploadURLResponse{}, fmt.Errorf("presign: %w", err)
	}
	upload.ArtifactID = entity.ID
	upload.ExpiresAt = expiresAt

	s.recordAudit(userID, "artifact.create", entity.ID)
	s.logger.Info("artifact created",
		"artifact", entity.ID,
		"session", entity.SessionID,
		"filename", entity.Filename,
		"size", entity.SizeBytes,
		"origin", entity.Origin)
	return upload, nil
}

// CompleteConsoleUpload is step 2 of the Console upload flow. Verifies
// the object landed in S3 with the expected size, flips status to Ready,
// fans out ArtifactChanged(created), and tells the owning Worker to
// mirror the file locally. Mirrors CompleteConsoleUploadAsync.
func (s *ArtifactService) CompleteConsoleUpload(
	ctx context.Context, userID, artifactID, contentSHA256 string,
) error {
	entity, err := s.repo.Find(ctx, artifactID)
	if err != nil {
		return err
	}
	if entity == nil {
		return errors.New("artifact not found")
	}
	if entity.OwnerUserID != userID {
		return errors.New("artifact belongs to another user")
	}
	if entity.Status != ArtifactStatusPending {
		return fmt.Errorf("artifact not pending: %s", entity.Status)
	}

	exists, err := s.storage.ObjectExists(entity.SessionID, entity.Filename)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("object missing from storage")
	}
	actualSize, err := s.storage.GetObjectSize(entity.SessionID, entity.Filename)
	if err != nil {
		return err
	}
	if actualSize != entity.SizeBytes {
		// Size mismatch — clean up the rogue S3 object + row so the user
		// can retry cleanly.
		_ = s.storage.DeleteObject(entity.SessionID, entity.Filename)
		if err := s.repo.UpdateStatus(ctx, entity.ID, ArtifactStatusDeleted, nil, nil); err != nil {
			return fmt.Errorf("size mismatch + delete: %w", err)
		}
		return fmt.Errorf("size mismatch: expected %d, actual %d", entity.SizeBytes, actualSize)
	}

	now := s.clock()
	sha := contentSHA256
	if err := s.repo.UpdateStatus(ctx, entity.ID, ArtifactStatusReady, &sha, &now); err != nil {
		return err
	}

	s.recordAudit(userID, "artifact.complete", entity.ID)
	s.logger.Info("artifact upload complete; notifying worker",
		"artifact", entity.ID,
		"session", entity.SessionID)

	dto := s.mapToDTO(entity, now)
	evt := ArtifactChangedEvent{
		SessionID:  entity.SessionID,
		ArtifactID: entity.ID,
		ChangeType: ArtifactChangeCreated,
		Artifact:   &dto,
	}
	if s.broadcaster != nil {
		if err := s.broadcaster.BroadcastArtifactChanged(userID, evt); err != nil {
			s.logger.Warn("broadcast artifact changed failed",
				"artifact", entity.ID, "err", err)
		}
	}
	s.notifyWorkerToMirror(entity)
	return nil
}

// CreateForWorkerUpload is the worker-side create path. Re-uses the same
// Pending → Ready state machine but with Origin=Worker and last-write-wins:
// an existing Pending row is reused; a Ready row is overwritten
// (status reset to Pending). Mirrors CreateForWorkerUploadAsync.
func (s *ArtifactService) CreateForWorkerUpload(
	ctx context.Context, workerConnectionID, workerOwnerUserID string,
	request CreateArtifactRequest,
) (storage.UploadURLResponse, error) {
	filename, err := url.PathUnescape(request.Filename)
	if err != nil {
		filename = request.Filename
	}
	if reason := ValidateFilename(filename); reason != "" {
		return storage.UploadURLResponse{}, fmt.Errorf("invalid filename: %s", reason)
	}
	if request.SizeBytes <= 0 || request.SizeBytes > s.options.MaxArtifactSizeBytes {
		return storage.UploadURLResponse{}, fmt.Errorf("size must be between 1 and %d bytes", s.options.MaxArtifactSizeBytes)
	}

	if err := s.ensureSessionOwnedByWorker(request.SessionID, workerConnectionID, workerOwnerUserID); err != nil {
		return storage.UploadURLResponse{}, err
	}

	existing, err := s.repo.FindBySessionFilename(ctx, request.SessionID, filename)
	if err != nil {
		return storage.UploadURLResponse{}, err
	}
	var loadedFromDB *data.ArtifactEntity
	if existing == nil {
		quota, err := s.repo.CountBySession(ctx, request.SessionID)
		if err != nil {
			return storage.UploadURLResponse{}, fmt.Errorf("quota check: %w", err)
		}
		if quota >= s.options.MaxArtifactsPerSession {
			return storage.UploadURLResponse{}, errors.New("session artifact quota exceeded")
		}
		now := s.clock()
		existing = &data.ArtifactEntity{
			ID:            uuid.NewString(),
			SessionID:     request.SessionID,
			Filename:      filename,
			Origin:        ArtifactOriginWorker,
			OwnerUserID:   workerOwnerUserID,
			FileCategory:  DetectFileCategory(filename),
			CreatedAtUTC:  now.Format(time.RFC3339Nano),
		}
		if err := s.repo.Insert(ctx, *existing); err != nil {
			return storage.UploadURLResponse{}, fmt.Errorf("insert: %w", err)
		}
	} else {
		loadedFromDB = existing
		if existing.Status == ArtifactStatusDeleted {
			existing.CreatedAtUTC = s.clock().Format(time.RFC3339Nano)
		}
	}
	now := s.clock()
	existing.SizeBytes = request.SizeBytes
	existing.Status = ArtifactStatusPending
	existing.ContentSHA256 = toNullString(request.ContentSHA256)
	existing.ExpiresAtUTC = s.computeExpiresAt(now, false, loadedFromDB).Format(time.RFC3339Nano)
	if err := s.repo.UpdateStatus(ctx, existing.ID, existing.Status, ptrString(request.ContentSHA256), nil); err != nil {
		return storage.UploadURLResponse{}, err
	}

	upload, err := s.storage.GenerateUploadURL(request.SessionID, filename)
	if err != nil {
		return storage.UploadURLResponse{}, fmt.Errorf("presign: %w", err)
	}
	upload.ArtifactID = existing.ID
	expAt, perr := time.Parse(time.RFC3339Nano, existing.ExpiresAtUTC)
	if perr == nil {
		upload.ExpiresAt = expAt
	}

	s.recordAudit(workerOwnerUserID, "artifact.workerCreate", existing.ID)
	s.logger.Info("worker artifact created",
		"artifact", existing.ID,
		"session", existing.SessionID,
		"filename", existing.Filename)
	return upload, nil
}

// CompleteWorkerUpload is the worker-side complete path. Mirrors
// CompleteWorkerUploadAsync.
func (s *ArtifactService) CompleteWorkerUpload(
	ctx context.Context, workerConnectionID, workerOwnerUserID, artifactID, contentSHA256 string,
) error {
	entity, err := s.repo.Find(ctx, artifactID)
	if err != nil {
		return err
	}
	if entity == nil {
		return errors.New("artifact not found")
	}
	if err := s.ensureSessionOwnedByWorker(entity.SessionID, workerConnectionID, workerOwnerUserID); err != nil {
		return err
	}
	if entity.Status != ArtifactStatusPending {
		return fmt.Errorf("artifact not pending: %s", entity.Status)
	}
	exists, err := s.storage.ObjectExists(entity.SessionID, entity.Filename)
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("object missing from storage")
	}

	now := s.clock()
	sha := contentSHA256
	if err := s.repo.UpdateStatus(ctx, entity.ID, ArtifactStatusReady, &sha, &now); err != nil {
		return err
	}

	s.recordAudit(workerOwnerUserID, "artifact.workerComplete", entity.ID)
	s.logger.Info("worker artifact upload complete", "artifact", entity.ID)

	if s.broadcaster != nil {
		dto := s.mapToDTO(entity, now)
		if err := s.broadcaster.BroadcastArtifactChanged(entity.OwnerUserID, ArtifactChangedEvent{
			SessionID:  entity.SessionID,
			ArtifactID: entity.ID,
			ChangeType: ArtifactChangeCreated,
			Artifact:   &dto,
		}); err != nil {
			s.logger.Warn("broadcast artifact changed failed",
				"artifact", entity.ID, "err", err)
		}
	}
	return nil
}

// List returns every non-deleted artifact for a session, oldest first.
// Mirrors ListAsync.
func (s *ArtifactService) List(ctx context.Context, userID, sessionID string) ([]ArtifactInfo, error) {
	if err := s.ensureSessionOwnedByUser(sessionID, userID); err != nil {
		return nil, err
	}
	rows, err := s.repo.ListBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	out := make([]ArtifactInfo, len(rows))
	for i, r := range rows {
		uploadedAt := r.CreatedAtUTC
		if r.CompletedAtUTC.Valid {
			uploadedAt = r.CompletedAtUTC.String
		}
		uploaded, _ := time.Parse(time.RFC3339Nano, uploadedAt)
		expires, _ := time.Parse(time.RFC3339Nano, r.ExpiresAtUTC)
		out[i] = ArtifactInfo{
			ID:           r.ID,
			SessionID:    r.SessionID,
			Filename:     r.Filename,
			SizeBytes:    r.SizeBytes,
			Status:       r.Status,
			Origin:       r.Origin,
			FileCategory: r.FileCategory,
			UploadedAt:   uploaded,
			ExpiresAt:    expires,
		}
	}
	return out, nil
}

// GetDownloadURL issues a short-lived presigned GET URL for the artifact.
// Mirrors GetDownloadUrlAsync.
func (s *ArtifactService) GetDownloadURL(ctx context.Context, userID, artifactID string) (storage.DownloadURLResponse, error) {
	entity, err := s.repo.Find(ctx, artifactID)
	if err != nil {
		return storage.DownloadURLResponse{}, err
	}
	if entity == nil {
		return storage.DownloadURLResponse{}, errors.New("artifact not found")
	}
	if entity.OwnerUserID != userID {
		return storage.DownloadURLResponse{}, errors.New("artifact belongs to another user")
	}
	if entity.Status != ArtifactStatusReady {
		return storage.DownloadURLResponse{}, fmt.Errorf("artifact not ready: %s", entity.Status)
	}
	s.recordAudit(userID, "artifact.download", entity.ID)
	return s.storage.GenerateDownloadURL(entity.SessionID, entity.Filename)
}

// Delete soft-deletes a single artifact: drop the S3 object, flip status
// to Deleted, fan out ArtifactChanged(deleted). Mirrors DeleteAsync.
func (s *ArtifactService) Delete(ctx context.Context, userID, artifactID string) error {
	entity, err := s.repo.Find(ctx, artifactID)
	if err != nil {
		return err
	}
	if entity == nil {
		return errors.New("artifact not found")
	}
	if entity.OwnerUserID != userID {
		return errors.New("artifact belongs to another user")
	}
	if entity.Status == ArtifactStatusDeleted {
		return nil
	}
	if err := s.storage.DeleteObject(entity.SessionID, entity.Filename); err != nil {
		s.logger.Warn("delete s3 object failed", "artifact", entity.ID, "err", err)
	}
	if err := s.repo.UpdateStatus(ctx, entity.ID, ArtifactStatusDeleted, nil, nil); err != nil {
		return err
	}
	s.recordAudit(userID, "artifact.delete", entity.ID)
	s.logger.Info("artifact deleted by user", "artifact", entity.ID)

	if s.broadcaster != nil {
		if err := s.broadcaster.BroadcastArtifactChanged(entity.OwnerUserID, ArtifactChangedEvent{
			SessionID:  entity.SessionID,
			ArtifactID: entity.ID,
			ChangeType: ArtifactChangeDeleted,
			Artifact:   nil,
		}); err != nil {
			s.logger.Warn("broadcast artifact changed failed",
				"artifact", entity.ID, "err", err)
		}
	}
	return nil
}

// DeleteByWorker is the worker-reported deletion of a local-mirrored
// file. Soft-deletes the matching row by (sessionId, filename) and
// fans out ArtifactChanged(deleted). Mirrors DeleteByWorkerAsync.
func (s *ArtifactService) DeleteByWorker(ctx context.Context, sessionID, filename string) error {
	entity, err := s.repo.FindBySessionFilename(ctx, sessionID, filename)
	if err != nil {
		return err
	}
	if entity == nil || entity.Status == ArtifactStatusDeleted {
		return nil
	}
	if err := s.storage.DeleteObject(entity.SessionID, entity.Filename); err != nil {
		s.logger.Warn("delete s3 object failed", "artifact", entity.ID, "err", err)
	}
	if err := s.repo.UpdateStatus(ctx, entity.ID, ArtifactStatusDeleted, nil, nil); err != nil {
		return err
	}
	s.logger.Info("artifact deleted by worker",
		"artifact", entity.ID,
		"filename", entity.Filename)
	if s.broadcaster != nil {
		if err := s.broadcaster.BroadcastArtifactChanged(entity.OwnerUserID, ArtifactChangedEvent{
			SessionID:  entity.SessionID,
			ArtifactID: entity.ID,
			ChangeType: ArtifactChangeDeleted,
			Artifact:   nil,
		}); err != nil {
			s.logger.Warn("broadcast artifact changed failed",
				"artifact", entity.ID, "err", err)
		}
	}
	return nil
}

// OnSessionTerminated clamps every artifact's ExpiresAtUtc to
// now + GracePeriodHours so the user can still pull files down for a
// short grace window, then notifies every client to refresh the
// remaining-days chip. Mirrors OnSessionTerminatedAsync.
func (s *ArtifactService) OnSessionTerminated(ctx context.Context, sessionID string) error {
	now := s.clock()
	graceEnd := now.Add(time.Duration(s.options.GracePeriodHours) * time.Hour)
	artifacts, err := s.repo.ListBySessionWithFutureExpiry(ctx, sessionID, graceEnd)
	if err != nil {
		return err
	}
	for _, a := range artifacts {
		if err := s.repo.UpdateExpiry(ctx, a.ID, graceEnd); err != nil {
			return err
		}
		dto := s.mapToDTO(&a, graceEnd)
		if s.broadcaster != nil {
			if err := s.broadcaster.BroadcastArtifactChanged(a.OwnerUserID, ArtifactChangedEvent{
				SessionID:  a.SessionID,
				ArtifactID: a.ID,
				ChangeType: ArtifactChangeUpdated,
				Artifact:   &dto,
			}); err != nil {
				s.logger.Warn("broadcast artifact changed failed",
					"artifact", a.ID, "err", err)
			}
		}
	}
	return nil
}

// CleanExpired performs a single cleanup pass — removes artifacts whose
// ExpiresAtUtc has passed. Mirrors CleanExpiredAsync.
func (s *ArtifactService) CleanExpired(ctx context.Context) (int, error) {
	now := s.clock()
	expired, err := s.repo.ListExpired(ctx, now)
	if err != nil {
		return 0, err
	}
	for _, e := range expired {
		if err := s.storage.DeleteObject(e.SessionID, e.Filename); err != nil {
			s.logger.Warn("failed to delete S3 object for expired artifact",
				"artifact", e.ID, "err", err)
		}
		if err := s.repo.UpdateStatus(ctx, e.ID, ArtifactStatusDeleted, nil, nil); err != nil {
			return 0, err
		}
		s.recordAudit("system", "artifact.cleanupExpired", e.ID)
	}
	for _, e := range expired {
		if s.broadcaster != nil {
			if err := s.broadcaster.BroadcastArtifactChanged(e.OwnerUserID, ArtifactChangedEvent{
				SessionID:  e.SessionID,
				ArtifactID: e.ID,
				ChangeType: ArtifactChangeDeleted,
				Artifact:   nil,
			}); err != nil {
				s.logger.Warn("broadcast artifact changed failed",
					"artifact", e.ID, "err", err)
			}
		}
	}
	if len(expired) > 0 {
		s.logger.Info("cleaned expired artifacts", "count", len(expired))
	}
	return len(expired), nil
}

// --- helpers ---

func (s *ArtifactService) mapToDTO(e *data.ArtifactEntity, completedAt time.Time) ArtifactInfo {
	uploadedAt, _ := time.Parse(time.RFC3339Nano, e.CreatedAtUTC)
	if e.CompletedAtUTC.Valid {
		if t, err := time.Parse(time.RFC3339Nano, e.CompletedAtUTC.String); err == nil {
			uploadedAt = t
		}
	}
	if completedAt.After(uploadedAt) {
		uploadedAt = completedAt
	}
	expiresAt, _ := time.Parse(time.RFC3339Nano, e.ExpiresAtUTC)
	return ArtifactInfo{
		ID:           e.ID,
		SessionID:    e.SessionID,
		Filename:     e.Filename,
		SizeBytes:    e.SizeBytes,
		Status:       e.Status,
		Origin:       e.Origin,
		FileCategory: e.FileCategory,
		UploadedAt:   uploadedAt,
		ExpiresAt:    expiresAt,
	}
}

func (s *ArtifactService) ensureSessionOwnedByUser(sessionID, userID string) error {
	session, ok := s.sessions.TryGetSession(sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	if session.UserID != userID {
		return errors.New("session belongs to another user")
	}
	return nil
}

func (s *ArtifactService) ensureSessionOwnedByWorker(sessionID, workerConnectionID, workerOwnerUserID string) error {
	session, ok := s.sessions.TryGetSession(sessionID)
	if !ok {
		return fmt.Errorf("session not found: %s", sessionID)
	}
	if session.WorkerConnectionID != workerConnectionID {
		return errors.New("worker connection mismatch")
	}
	if session.UserID != workerOwnerUserID {
		return errors.New("worker does not own session")
	}
	return nil
}

func (s *ArtifactService) computeExpiresAt(now time.Time, sessionTerminated bool, existing *data.ArtifactEntity) time.Time {
	ttlEnd := now.AddDate(0, 0, s.options.MaxArtifactAgeDays)
	if sessionTerminated {
		graceEnd := now.Add(time.Duration(s.options.GracePeriodHours) * time.Hour)
		if ttlEnd.Before(graceEnd) {
			return ttlEnd
		}
		return graceEnd
	}
	if existing != nil && existing.ExpiresAtUTC != "" {
		if existingExp, err := time.Parse(time.RFC3339Nano, existing.ExpiresAtUTC); err == nil {
			if existingExp.Before(ttlEnd) {
				return existingExp
			}
		}
	}
	return ttlEnd
}

func (s *ArtifactService) notifyWorkerToMirror(entity *data.ArtifactEntity) {
	if s.worker == nil {
		return
	}
	session, ok := s.sessions.TryGetSession(entity.SessionID)
	if !ok || session.WorkerConnectionID == "" {
		return
	}
	dl, err := s.storage.GenerateDownloadURL(entity.SessionID, entity.Filename)
	if err != nil {
		s.logger.Warn("generate download URL for worker mirror failed",
			"artifact", entity.ID, "err", err)
		return
	}
	sha := ""
	if entity.ContentSHA256.Valid {
		sha = entity.ContentSHA256.String
	}
	if err := s.worker.NotifyArtifactUploaded(session.WorkerConnectionID, NotifyArtifactUploadedFrame{
		SessionID:     entity.SessionID,
		Filename:      entity.Filename,
		DownloadURL:   dl.DownloadURL,
		SizeBytes:     entity.SizeBytes,
		ContentSHA256: sha,
	}); err != nil {
		s.logger.Warn("notify worker mirror failed",
			"artifact", entity.ID, "err", err)
	}
}

func (s *ArtifactService) recordAudit(userID, action, artifactID string) {
	if s.audit == nil {
		return
	}
	s.audit.Record(userID, action, "artifact", artifactID)
}

func validOrigin(origin string) bool {
	return strings.EqualFold(origin, ArtifactOriginConsole) ||
		strings.EqualFold(origin, ArtifactOriginWorker)
}

func toNullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func ptrString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}