package sessions

import "time"

// ArtifactInfo mirrors CortexTerminal.Contracts.Sessions.ArtifactInfo.
// Field names match the C# Key(N) ordering so existing Console clients
// can deserialize the msgpack frames unchanged.
type ArtifactInfo struct {
	ID           string    `json:"id"           msgpack:"id"`
	SessionID    string    `json:"sessionId"    msgpack:"sessionId"`
	Filename     string    `json:"filename"     msgpack:"filename"`
	SizeBytes    int64     `json:"sizeBytes"    msgpack:"sizeBytes"`
	Status       string    `json:"status"       msgpack:"status"`
	Origin       string    `json:"origin"       msgpack:"origin"`
	FileCategory string    `json:"fileCategory" msgpack:"fileCategory"`
	UploadedAt   time.Time `json:"uploadedAt"   msgpack:"uploadedAt"`
	ExpiresAt    time.Time `json:"expiresAt"    msgpack:"expiresAt"`
}

// CreateArtifactRequest mirrors CortexTerminal.Contracts.Sessions.CreateArtifactRequest.
type CreateArtifactRequest struct {
	SessionID     string `json:"sessionId"      msgpack:"sessionId"`
	Filename      string `json:"filename"       msgpack:"filename"`
	SizeBytes     int64  `json:"sizeBytes"      msgpack:"sizeBytes"`
	ContentSHA256 string `json:"contentSha256,omitempty" msgpack:"contentSha256,omitempty"`
	Origin        string `json:"origin"         msgpack:"origin"`
}

// ArtifactChangedEvent mirrors CortexTerminal.Contracts.Streaming.ArtifactChangedEvent.
// The C# Console listens for this method name on the /hubs/terminal hub.
type ArtifactChangedEvent struct {
	SessionID  string       `json:"sessionId"  msgpack:"sessionId"`
	ArtifactID string       `json:"artifactId" msgpack:"artifactId"`
	ChangeType string       `json:"changeType" msgpack:"changeType"`
	Artifact   *ArtifactInfo `json:"artifact,omitempty" msgpack:"artifact,omitempty"`
}

// ArtifactChangeType constants mirror CortexTerminal.Contracts.Streaming.ArtifactChangeType.
const (
	ArtifactChangeCreated = "created"
	ArtifactChangeUpdated = "updated"
	ArtifactChangeDeleted = "deleted"
)

// Artifact status constants. Mirrors CortexTerminal.Contracts.Sessions.ArtifactStatus.
// Re-declared here so callers in the sessions package don't need to
// import the data package for these wire-shared strings.
const (
	ArtifactStatusPending = "pending"
	ArtifactStatusReady   = "ready"
	ArtifactStatusDeleted = "deleted"
)

// Artifact origin constants. Mirrors CortexTerminal.Contracts.Sessions.ArtifactOrigin.
const (
	ArtifactOriginConsole = "console"
	ArtifactOriginWorker  = "worker"
)

// NotifyArtifactUploadedFrame mirrors
// CortexTerminal.Contracts.Streaming.NotifyArtifactUploadedFrame.
// Sent to the Worker over /hubs/worker so it mirrors the file locally.
type NotifyArtifactUploadedFrame struct {
	SessionID     string `json:"sessionId"     msgpack:"sessionId"`
	Filename      string `json:"filename"      msgpack:"filename"`
	DownloadURL   string `json:"downloadUrl"   msgpack:"downloadUrl"`
	SizeBytes     int64  `json:"sizeBytes"     msgpack:"sizeBytes"`
	ContentSHA256 string `json:"contentSha256" msgpack:"contentSha256"`
}