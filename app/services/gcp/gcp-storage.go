package gcp

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/google/martian/v3/log"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	"io"
	"os"
	"time"
)

type GCPStorage struct {
	client *config.GCPStorageClient
}

func (s *GCPStorage) UploadFile(filename string, bucket string) error {
	object := filename
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCP Storage client: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP Storage client: %w", err)
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

func (s *GCPStorage) ReadFileBytes(bucket, object string) (res []byte, err error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return res, fmt.Errorf("storage.NewClient: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP Storage client: %w", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return res, fmt.Errorf("Object(%q).NewReader: %w", object, err)
	}
	defer func() {
		if err := rc.Close(); err != nil {
			log.Errorf("failed to close object reader: %w", err)
		}
	}()

	// TODO: read to bytes

	return res, nil
}

func (s *GCPStorage) DownloadFile(bucket, object, destFileName string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP Storage client: %w", err)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	f, err := os.Create(destFileName)
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
