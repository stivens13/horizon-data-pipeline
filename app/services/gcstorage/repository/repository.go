package gcs_repo

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/google/martian/v3/log"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	"google.golang.org/api/iterator"
	"io"
	"os"
	"time"
)

type GCSRepository struct {
	client *config.GCSConfig
}

func NewGCPStorage(c *config.GCSConfig) *GCSRepository {
	return &GCSRepository{
		client: c,
	}
}

func (s *GCSRepository) CreateBucket(bucket string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCP StorageRepository client: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %w", err)
		}
	}()

	projectID := ""
	if err := client.Bucket(bucket).Create(ctx, projectID, nil); err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	return nil
}

func (s *GCSRepository) UploadFile(filename string, bucket string) error {
	object := filename
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCP StorageRepository client: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %w", err)
		}
	}()

	// Open local file.
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Errorf("failed to close file: %w", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	o := client.Bucket(bucket).Object(object)

	// Upload an object with storage.Writer.
	wc := o.NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %w", err)
	}

	return nil
}

func (s *GCSRepository) UploadFileFromBytes(bucket string, object string, data []byte) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCP StorageRepository client: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %v", err)
		}
	}()
	writer := client.Bucket(bucket).Object(object).NewWriter(ctx)
	defer func() {
		if err := writer.Close(); err != nil {
			log.Errorf("failed to close writer: %v", err)
		}
	}()

	// Write data to the object
	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("failed to write data to GCS: %w", err)
	}

	// Close the writer and finalize the upload
	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer: %w", err)
	}

	return nil
}

func (s *GCSRepository) DownloadFileToBytes(bucket, object string) (data []byte, err error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return data, fmt.Errorf("storage.NewClient: %w", err)
	}

	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %w", err)
		}
	}()
	reader, err := client.Bucket(bucket).Object(object).NewReader(context.TODO())
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			log.Errorf("failed to close reader: %w", err)
		}
	}()
	return io.ReadAll(reader)
}

func (s *GCSRepository) DownloadFile(bucket, object, destination string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP StorageRepository client: %w", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	f, err := os.Create(destination)
	if err != nil {
		return fmt.Errorf("failed to create file: os.Create: %w", err)
	}

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Errorf("failed to close object reader: %w", err)
		}
	}()

	if _, err := io.Copy(f, rc); err != nil {
		return fmt.Errorf("io.Copy: %w", err)
	}

	if err = f.Close(); err != nil {
		return fmt.Errorf("f.Close: %w", err)
	}

	return nil
}

func list(client *storage.Client, bucketName string) ([]string, error) {
	var objects []string
	it := client.Bucket(bucketName).Objects(context.Background(), &storage.Query{})
	for {
		oattrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		objects = append(objects, oattrs.Name)
	}
	return objects, nil
}
