package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	log "log/slog"
	"net/http"
	"time"

	"github.com/avast/retry-go"
	"github.com/dariusigna/object-storage/internal/registry"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// NotFoundError is returned when the object is not found in the object storage
type NotFoundError struct {
}

// Error returns the error message
func (n NotFoundError) Error() string {
	return "object not found"
}

// ObjectStorage is a gateway to the object storage
type ObjectStorage struct {
	registry *registry.Registry
}

// NewObjectStorage creates a new ObjectStorage instance
func NewObjectStorage(registry *registry.Registry) (*ObjectStorage, error) {
	return &ObjectStorage{registry: registry}, nil
}

// GetObject retrieves the object from the object storage
func (o *ObjectStorage) GetObject(ctx context.Context, bucket, id string) ([]byte, error) {
	minioInstance, err := o.getMatchingInstance(id)
	if err != nil {
		return nil, err
	}

	object, err := minioInstance.GetObject(ctx, bucket, id, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer object.Close()

	data, err := io.ReadAll(object)
	if err != nil {
		var minioErr minio.ErrorResponse
		if errors.As(err, &minioErr) && minioErr.StatusCode == http.StatusNotFound {
			return nil, NotFoundError{}
		}
		return nil, fmt.Errorf("failed to read object: %w", err)
	}

	return data, nil
}

// PutObject stores the object in the object storage
func (o *ObjectStorage) PutObject(ctx context.Context, bucket, id string, data []byte) error {
	minioInstance, err := o.getMatchingInstance(id)
	if err != nil {
		return err
	}

	exists, err := minioInstance.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !exists {
		if err = minioInstance.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return fmt.Errorf("failed to create bucket: %w", err)
		}
	}
	_, err = minioInstance.PutObject(ctx, bucket, id, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to put object: %w", err)
	}

	return nil
}

func (o *ObjectStorage) getMatchingInstance(id string) (*minio.Client, error) {
	var (
		instance registry.ServiceMetadata
		err      error
	)

	// Retry mechanism to make it resilient to transient failures
	err = retry.Do(
		func() error {
			instance, err = o.registry.MatchService(id)
			if err != nil {
				return fmt.Errorf("failed to get minio instance for object id: %w", err)
			}
			return nil
		},
		retry.Attempts(3),
		retry.Delay(300*time.Millisecond))
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s:9000", instance.IPAddress)
	minioInstance, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(instance.AccessKey, instance.SecretKey, ""),
		Secure: false, // In production, we would use SSL
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create minio client: %w", err)
	}

	log.Debug("Matched instance", "object_id", id, "instance", instance.IPAddress)
	return minioInstance, nil
}
