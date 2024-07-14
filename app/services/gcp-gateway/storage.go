package gcp_gateway

type Storage interface {
	UploadFile(filename string, bucket string) error
	ReadFileBytes(bucket, object string) (res []byte, err error)
	DownloadFile(bucket, object, destFileName string) error
}
