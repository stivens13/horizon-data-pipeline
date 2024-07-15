package gcp_gateway

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
	//client, err := storage.NewClient(ctx, option.WithEndpoint("http://gcp:4443/storage/v1/"))
	//client, err := storage.NewClient(ctx, option.WithEndpoint("http://gcs:4443/storage/v1/"))
	client, err := storage.NewClient(ctx)
	if err != nil {
		return res, fmt.Errorf("storage.NewClient: %w", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Errorf("failed to close GCP Storage client: %w", err)
		}
	}()
	return downloadFile(client, bucket, object)

	//ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	//defer cancel()
	//
	////objects, err := list(client, bucket)
	////if err != nil {
	////	log.Errorf("failed to list: %v", err)
	////}
	////fmt.Printf("objects: %+v\n", objects)
	//
	//rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	//if err != nil {
	//	return res, fmt.Errorf("Object(%q).NewReader: %w", object, err)
	//}
	//defer func() {
	//	if err := rc.Close(); err != nil {
	//		log.Errorf("failed to close object reader: %w", err)
	//	}
	//}()
	//
	//// TODO: read to bytes
	//res, err = io.ReadAll(rc)
	//if err != nil {
	//	return nil, fmt.Errorf("ioutil.ReadAll: %w", err)
	//}
	//
	//return res, nil
}

func downloadFile(client *storage.Client, bucketName, fileKey string) ([]byte, error) {
	reader, err := client.Bucket(bucketName).Object(fileKey).NewReader(context.TODO())
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
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

//func (s *GCPStorage) DownloadFile(bucket, object, dest string) ([]byte, error) {
//	// bucket := "bucket-name"
//	// object := "object-name"
//	ctx := context.Background()
//	client, err := storage.NewClient(ctx)
//	if err != nil {
//		return nil, fmt.Errorf("storage.NewClient: %w", err)
//	}
//	defer client.Close()
//
//	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
//	defer cancel()
//
//	rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
//	if err != nil {
//		return nil, fmt.Errorf("Object(%q).NewReader: %w", object, err)
//	}
//	defer rc.Close()
//
//	data, err := io.ReadAll(rc)
//	if err != nil {
//		return nil, fmt.Errorf("ioutil.ReadAll: %w", err)
//	}
//	//fmt.Fprintf(w, "Blob %v downloaded.\n", object)
//	return data, nil
//}
