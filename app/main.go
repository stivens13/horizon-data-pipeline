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
	Clickhouse         *chanalytics.ClickhouseRepository
	GCSInteractor      *gcs.GCSInteractor
	CurrencyInteractor *currency.CurrencyInteractor
	ETL                *etl.ETL
}

func InitServices(c *config.Config) *Services {
	clickhouse := chanalytics.NewClickhouseRepository(c.ClickhouseConfig)
	GCSInteractor := gcs.NewGCSInteractor(c.GCSConfig)
	currencyInteractor := currency.NewCurrencyInteractor(GCSInteractor)
	return &Services{
		Clickhouse:         clickhouse,
		GCSInteractor:      GCSInteractor,
		CurrencyInteractor: currencyInteractor,
		ETL:                etl.NewETL(GCSInteractor, clickhouse, currencyInteractor),
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
