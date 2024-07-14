package main

import (
	"fmt"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	"github.com/stivens13/horizon-data-pipeline/app/services/clickhouse_analytics"
	"github.com/stivens13/horizon-data-pipeline/app/services/etl"
	gcp_gateway "github.com/stivens13/horizon-data-pipeline/app/services/gcp-gateway"
	"log"
)

func main() {
	cfg := config.InitConfig()
	clickhouseRepo := clickhouse_analytics.NewClickhouseRepository(cfg.ClickhouseConfig)
	if err := clickhouseRepo.DB.Exec("SHOW USERS;").Error; err != nil {
		log.Fatalf("Error running clickhouse query: %v", err)
	}
	fmt.Printf("Connected to clickhouse successfully")

	storage := gcp_gateway.NewGCPStorageMock()

	date := "20240415"
	ETL := etl.NewETL(storage, clickhouseRepo)
	if err := ETL.StartETL(date); err != nil {
		log.Fatalf("failed to perform ETL: %v", err)
	}

	return
}
