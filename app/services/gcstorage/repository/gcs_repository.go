package gcs_repo

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"errors"
	"fmt"
	"github.com/google/martian/v3/log"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	"google.golang.org/api/iterator"
	"io"
	"os"
	"time"
)

type GCSRepository struct {
	c *config.GCSConfig
}

func NewGCPStorage(c *config.GCSConfig) *GCSRepository {
	return &GCSRepository{
		c: c,
	}
}

func (s *GCSRepository) CreateBucket(bucket string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCP StorageRepository client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %v", err)
		}
	}()

	projectID := ""
	if err := client.Bucket(bucket).Create(ctx, projectID, nil); err != nil {
		return fmt.Errorf("failed to create bucket: %v", err)
	}

	return nil
}

func (s *GCSRepository) DeleteBucket(bucket string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCP StorageRepository client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %v", err)
		}
	}()

	b := client.Bucket(bucket)
	// Check if the bucket exists
	if _, err := b.Attrs(ctx); err != nil {
		return nil
	}
	if err := deleteBucketObjects(ctx, client, bucket); err != nil {
		return fmt.Errorf("failed to delete bucket objects: %v", err)
	}

	if err := b.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete bucket: %v", err)
	}

	return nil
}

func deleteBucketObjects(ctx context.Context, client *storage.Client, bucket string) error {
	b := client.Bucket(bucket)
	it := b.Objects(ctx, &storage.Query{Versions: true})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to list objects: %v", err)
		}
		if err := b.Object(attrs.Name).Generation(attrs.Generation).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %v", attrs.Name, err)
		}
	}
	return nil
}

func (s *GCSRepository) UploadFile(bucket string, filename string) error {
	object := filename
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCP StorageRepository client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %v", err)
		}
	}()

	// Open local file.
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %v", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Errorf("failed to close file: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	o := client.Bucket(bucket).Object(object)

	// Upload an object with storage.Writer.
	wc := o.NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}

	return nil
}

func (s *GCSRepository) UploadFileFromBytes(bucket string, object string, data []byte) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCP StorageRepository client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %v", err)
		}
	}()

	b := client.Bucket(bucket)
	o := b.Object(object)
	writer := o.NewWriter(ctx)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Errorf("failed to close writer: %v", err)
		}
	}()

	buf := bytes.NewBuffer(data)
	if _, err = io.Copy(writer, buf); err != nil {
		return fmt.Errorf("failed to copy data: %w", err)
	}

	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("failed to write data to GCS: %v", err)
	}

	return nil
}

func (s *GCSRepository) DownloadFileToBytes(bucket, object string) (data []byte, err error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return data, fmt.Errorf("storage.NewClient: %v", err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %v", err)
		}
	}()
	reader, err := client.Bucket(bucket).Object(object).NewReader(context.TODO())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			log.Errorf("failed to close reader: %v", err)
		}
	}()
	return io.ReadAll(reader)
}

func (s *GCSRepository) DownloadFile(bucket, object, destination string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	f, err := os.Create(destination)
	if err != nil {
		return fmt.Errorf("failed to create file: os.Create: %v", err)
	}

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %v", object, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Errorf("failed to close object reader: %v", err)
		}
	}()

	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("f.Close: %v", err)
	}

	return nil
}

func (s *GCSRepository) ListBucketObjects(bucket string) ([]string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return []string{}, fmt.Errorf("storage.NewClient: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	var objects []string
	it := client.Bucket(bucket).Objects(context.Background(), &storage.Query{})
	for {
		objectAttrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		objects = append(objects, objectAttrs.Name)
	}

	return objects, nil
}
