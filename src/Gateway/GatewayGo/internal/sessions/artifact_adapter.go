package sessions

import (
	"context"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/signalr"
)

// SignalRArtifactService bridges the sessions.ArtifactService into the
// signalr.ArtifactService interface. The wire-level types differ slightly
// (signalr.*Wire vs sessions.*) but the field set is the same.
type SignalRArtifactService struct {
	s *ArtifactService
}

func NewSignalRArtifactService(s *ArtifactService) *SignalRArtifactService {
	return &SignalRArtifactService{s: s}
}

func (a *SignalRArtifactService) CreateForWorkerUpload(
	ctx context.Context, connectionID, userID string, req signalr.CreateArtifactRequestWire,
) (signalr.UploadUrlResponseWire, error) {
	internalReq := CreateArtifactRequest{
		SessionID:     req.SessionID,
		Filename:      req.Filename,
		SizeBytes:     req.ContentLength,
		ContentSHA256: req.ContentSHA256,
		Origin:        ArtifactOriginWorker,
	}
	out, err := a.s.CreateForWorkerUpload(ctx, connectionID, userID, internalReq)
	if err != nil {
		return signalr.UploadUrlResponseWire{}, err
	}
	return signalr.UploadUrlResponseWire{
		ArtifactID: out.ArtifactID,
		UploadURL:  out.UploadURL,
	}, nil
}

func (a *SignalRArtifactService) CompleteWorkerUpload(
	ctx context.Context, connectionID, userID, artifactID, contentSHA256 string,
) error {
	return a.s.CompleteWorkerUpload(ctx, connectionID, userID, artifactID, contentSHA256)
}

func (a *SignalRArtifactService) DeleteByWorker(
	ctx context.Context, sessionID, filename string,
) error {
	return a.s.DeleteByWorker(ctx, sessionID, filename)
}