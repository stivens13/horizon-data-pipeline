package main

import (
	"fmt"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	chanalytics "github.com/stivens13/horizon-data-pipeline/app/services/clickhouse_analytics"
	currency "github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker/usecase"
	"github.com/stivens13/horizon-data-pipeline/app/services/etl"
	gcs "github.com/stivens13/horizon-data-pipeline/app/services/gcstorage/usecase"
	"log"
)

type Services struct {
	Clickhouse *chanalytics.ClickhouseRepository
	GCStorage  *gcs.GCSInteractor
	Currency   *currency.CurrencyInteractor
	ETL        *etl.ETL
}

func InitServices(c *config.Config) *Services {
	clickhouse := chanalytics.NewClickhouseRepository(c.ClickhouseConfig)
	gcStorage := gcs.NewGCSInteractor(c.GCSConfig)
	return &Services{
		Clickhouse: clickhouse,
		GCStorage:  gcStorage,
		Currency:   currency.NewCurrencyInteractor(gcStorage),
		ETL:        etl.NewETL(gcStorage, clickhouse),
	}
}

func main() {
	fmt.Println("ETL application starts, initializing services...")
	cfg := config.InitConfig()
	services := InitServices(cfg)

	fmt.Println("Start ETL Sequence")
	date := "2024-04-15"
	if err := services.ETL.StartETL(date); err != nil {
		log.Fatalf("failed to perform ETL: %v", err)
	}
	fmt.Println("ETL Sequence Complete")

	return
}
