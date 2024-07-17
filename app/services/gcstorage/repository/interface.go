package gcs_repo

type StorageRepository interface {
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	UploadFile(bucket string, filename string) error
	UploadFileFromBytes(bucket string, object string, data []byte) error
	DownloadFileToBytes(bucket, object string) (data []byte, err error)
	DownloadFile(bucket, object, destination string) error
	DeleteFile(bucket, object string) error
	ListBucketObjects(bucket string) ([]string, error)
}
