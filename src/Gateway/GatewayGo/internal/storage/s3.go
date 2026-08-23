package storage

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3CompatibleArtifactStorage is the Go port of
// CortexTerminal.Gateway.Storage.S3CompatibleArtifactStorage.
//
// Uses aws-sdk-go-v2's presign client (s3.NewPresignClient). The signature
// presigner is SigV4 — works against AWS S3, MinIO, Cloudflare R2, and
// any other S3-compatible endpoint. ForcePathStyle mirrors the C# options
// default; the MinIO docker image requires it.
//
// The endpoint URL is rewritten so http:// or https:// is preserved. When
// Endpoint is left empty, the SDK falls back to the AWS regional endpoint
// resolver — required for production S3.
type S3CompatibleArtifactStorage struct {
	client    *s3.Client
	presigner *s3.PresignClient
	options   ArtifactStorageOptions
}

// NewS3CompatibleArtifactStorage builds the S3 client. The presigned URL
// TTL is taken from options.PresignedUrlTtl. The MaxSize argument on the
// PUT presign is set from MaxArtifactSizeBytes — clients requesting
// anything larger than the cap get a 403-equivalent from S3.
func NewS3CompatibleArtifactStorage(ctx context.Context, options ArtifactStorageOptions) (*S3CompatibleArtifactStorage, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(options.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			options.AccessKey, options.SecretKey, "",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if options.Endpoint != "" {
			o.BaseEndpoint = aws.String(normalizeEndpoint(options.Endpoint))
		}
		o.UsePathStyle = options.ForcePathStyle
	})

	return &S3CompatibleArtifactStorage{
		client:    client,
		presigner: s3.NewPresignClient(client),
		options:   options,
	}, nil
}

func normalizeEndpoint(endpoint string) string {
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		return "https://" + endpoint
	}
	return endpoint
}

func (s *S3CompatibleArtifactStorage) objectKey(sessionID, filename string) string {
	return sessionID + "/" + filename
}

func (s *S3CompatibleArtifactStorage) presignTTL() time.Duration {
	if s.options.PresignedUrlTtl > 0 {
		return s.options.PresignedUrlTtl
	}
	return 15 * time.Minute
}

func (s *S3CompatibleArtifactStorage) GenerateUploadURL(sessionID, filename string) (UploadURLResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := s.objectKey(sessionID, filename)
	contentLength := s.options.MaxArtifactSizeBytes
	if contentLength <= 0 {
		contentLength = 50 * 1024 * 1024
	}
	req, err := s.presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.options.Bucket),
		Key:           aws.String(key),
		ContentLength: aws.Int64(contentLength),
	}, s3.WithPresignExpires(s.presignTTL()))
	if err != nil {
		return UploadURLResponse{}, fmt.Errorf("presign put: %w", err)
	}
	return UploadURLResponse{
		ArtifactID: "", // populated by ArtifactService after INSERT
		UploadURL:  req.URL,
		S3Key:      key,
		ExpiresAt:  time.Now().UTC().Add(s.presignTTL()),
	}, nil
}

func (s *S3CompatibleArtifactStorage) GenerateDownloadURL(sessionID, filename string) (DownloadURLResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := s.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.options.Bucket),
		Key:    aws.String(s.objectKey(sessionID, filename)),
	}, s3.WithPresignExpires(s.presignTTL()))
	if err != nil {
		return DownloadURLResponse{}, fmt.Errorf("presign get: %w", err)
	}
	return DownloadURLResponse{
		DownloadURL: req.URL,
		ExpiresAt:   time.Now().UTC().Add(s.presignTTL()),
	}, nil
}

func (s *S3CompatibleArtifactStorage) DeleteObject(sessionID, filename string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.options.Bucket),
		Key:    aws.String(s.objectKey(sessionID, filename)),
	})
	if err != nil {
		return fmt.Errorf("s3 delete: %w", err)
	}
	return nil
}

func (s *S3CompatibleArtifactStorage) DeleteSessionPrefix(sessionID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := sessionID + "/"
	var continuationToken *string
	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.options.Bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return fmt.Errorf("s3 list: %w", err)
		}
		for _, obj := range out.Contents {
			if obj.Key == nil {
				continue
			}
			if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(s.options.Bucket),
				Key:    obj.Key,
			}); err != nil {
				return fmt.Errorf("s3 delete %s: %w", *obj.Key, err)
			}
		}
		if out.NextContinuationToken == nil {
			return nil
		}
		continuationToken = out.NextContinuationToken
	}
}

func (s *S3CompatibleArtifactStorage) GetObjectSize(sessionID, filename string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.options.Bucket),
		Key:    aws.String(s.objectKey(sessionID, filename)),
	})
	if err != nil {
		return 0, fmt.Errorf("s3 head: %w", err)
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

func (s *S3CompatibleArtifactStorage) ObjectExists(sessionID, filename string) (bool, error) {
	_, err := s.GetObjectSize(sessionID, filename)
	if err == nil {
		return true, nil
	}
	// S3 returns NoSuchKey for missing objects. Anything else is a real
	// error — surface it.
	if isNotFound(err) {
		return false, nil
	}
	return false, err
}

// isNotFound matches aws-sdk-go-v2 error sentinels for missing objects
// without importing the service-internal error package directly. The
// underlying type is *smithy.GenericAPIError with Code "NoSuchKey".
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "NoSuchKey") ||
		strings.Contains(msg, "NotFound") ||
		strings.Contains(msg, "404")
}

// URLForKey reconstructs the public URL for an object key. Used by tests
// + the C# mirror that may want to log the underlying path without going
// through a presign round-trip.
func (s *S3CompatibleArtifactStorage) URLForKey(key string) string {
	if s.options.Endpoint == "" {
		return fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s",
			s.options.Bucket, s.options.Region, key)
	}
	endpoint := normalizeEndpoint(s.options.Endpoint)
	if s.options.ForcePathStyle {
		u, _ := url.Parse(endpoint)
		if u == nil {
			return endpoint + "/" + s.options.Bucket + "/" + key
		}
		u.Path = strings.TrimRight(u.Path, "/") + "/" + s.options.Bucket + "/" + key
		return u.String()
	}
	return endpoint + "/" + key
}