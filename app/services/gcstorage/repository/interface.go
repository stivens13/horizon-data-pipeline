package gcs_repo

type StorageRepository interface {
	CreateBucket(bucket string) error
	UploadFile(filename string, bucket string) error
	UploadFileFromBytes(filename string, bucket string, object []byte) error
	DownloadFileToBytes(bucket, object string) (data []byte, err error)
	DownloadFile(bucket, object, destination string) error
}
