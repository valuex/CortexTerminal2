// Package storage owns the artifact blob-storage layer — interface,
// options, and S3-compatible implementation. Mirrors
// CortexTerminal.Gateway.Storage.* in C#.
package storage

import "time"

// ArtifactStorageOptions mirrors CortexTerminal.Gateway.Storage.ArtifactStorageOptions.
// Section name is "Storage" in YAML; sub-sections Endpoint / Bucket etc.
// are loaded directly into these fields by the config loader.
type ArtifactStorageOptions struct {
	Endpoint             string        `yaml:"endpoint"`
	Bucket               string        `yaml:"bucket"`
	Region               string        `yaml:"region"`
	AccessKey            string        `yaml:"accessKey"`
	SecretKey            string        `yaml:"secretKey"`
	ForcePathStyle       bool          `yaml:"forcePathStyle"`
	PresignedUrlTtl      time.Duration `yaml:"presignedUrlTtl"`
	MaxArtifactSizeBytes int64         `yaml:"maxArtifactSizeBytes"`
	MaxArtifactAgeDays   int           `yaml:"maxArtifactAgeDays"`
	GracePeriodHours     int           `yaml:"gracePeriodHours"`
	MaxArtifactsPerSession int         `yaml:"maxArtifactsPerSession"`
}

// WithDefaults returns a copy with zero-value fields populated with the
// same defaults the C# options class uses. The config loader calls this
// before applying the YAML overlay.
func (o ArtifactStorageOptions) WithDefaults() ArtifactStorageOptions {
	if o.Region == "" {
		o.Region = "us-east-1"
	}
	if o.PresignedUrlTtl == 0 {
		o.PresignedUrlTtl = 15 * time.Minute
	}
	if o.MaxArtifactSizeBytes == 0 {
		o.MaxArtifactSizeBytes = 50 * 1024 * 1024
	}
	if o.MaxArtifactAgeDays == 0 {
		o.MaxArtifactAgeDays = 7
	}
	if o.GracePeriodHours == 0 {
		o.GracePeriodHours = 24
	}
	if o.MaxArtifactsPerSession == 0 {
		o.MaxArtifactsPerSession = 100
	}
	return o
}

// IsConfigured returns true when the minimum fields are populated and the
// storage backend should be wired. An empty Endpoint + Bucket means the
// admin hasn't configured S3 yet — the gateway should boot without
// artifact features, mirroring the C# startup behaviour.
func (o ArtifactStorageOptions) IsConfigured() bool {
	return o.Bucket != "" && o.AccessKey != "" && o.SecretKey != ""
}