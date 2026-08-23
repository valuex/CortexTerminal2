package storage

import "time"

// UploadURLResponse mirrors CortexTerminal.Contracts.Sessions.UploadUrlResponse.
// Field order is preserved (msgpack Keys 0..3). The C# client uses these
// fields by name on the wire.
type UploadURLResponse struct {
	ArtifactID string    `json:"artifactId" msgpack:"artifactId"`
	UploadURL  string    `json:"uploadUrl"  msgpack:"uploadUrl"`
	S3Key      string    `json:"s3Key"      msgpack:"s3Key"`
	ExpiresAt  time.Time `json:"expiresAt"  msgpack:"expiresAt"`
}

// DownloadURLResponse mirrors CortexTerminal.Contracts.Sessions.DownloadUrlResponse.
type DownloadURLResponse struct {
	DownloadURL string    `json:"downloadUrl" msgpack:"downloadUrl"`
	ExpiresAt   time.Time `json:"expiresAt"    msgpack:"expiresAt"`
}

// ArtifactStorage is the port of IArtifactStorage. Implementations must
// be safe for concurrent use — multiple hub invocations can issue
// presigned URLs simultaneously.
//
// Object key layout: {sessionId}/{filename}. DeleteSessionPrefix removes
// every artifact under one session — used when the session is terminated
// permanently and the grace window closes.
type ArtifactStorage interface {
	// GenerateUploadURL returns a presigned PUT URL the client uses to
	// upload directly to S3. sessionID + filename uniquely identify the
	// object within the bucket.
	GenerateUploadURL(sessionID, filename string) (UploadURLResponse, error)

	// GenerateDownloadURL returns a short-lived presigned GET URL.
	GenerateDownloadURL(sessionID, filename string) (DownloadURLResponse, error)

	// DeleteObject removes a single object. Errors when the object is
	// missing are surfaced to the caller — the cleanup pass treats them as
	// warnings, but explicit Delete calls should propagate.
	DeleteObject(sessionID, filename string) error

	// DeleteSessionPrefix removes every object under sessionID/.
	DeleteSessionPrefix(sessionID string) error

	// GetObjectSize returns the size in bytes. Used by the Complete step
	// to verify the client uploaded what they claimed.
	GetObjectSize(sessionID, filename string) (int64, error)

	// ObjectExists returns true when the object is present.
	ObjectExists(sessionID, filename string) (bool, error)
}