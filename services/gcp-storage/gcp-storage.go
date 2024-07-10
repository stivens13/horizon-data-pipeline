package gcp_storage

import (
	horizon_data_pipeline "github.com/stivens13/horizon-data-pipeline/config"
)

type GCPStorage struct {
	client *horizon_data_pipeline.GCPStorageClient
}
