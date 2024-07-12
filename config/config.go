package config

var Config *config

type config struct {
	GCPStorageClient *GCPStorageClient
}

type GCPStorageClient struct {
}

func init() {
	gcp := &GCPStorageClient{}

	Config = &config{
		GCPStorageClient: gcp,
	}
}
