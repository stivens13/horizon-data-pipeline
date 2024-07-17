package main

import (
	"fmt"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	chanalytics "github.com/stivens13/horizon-data-pipeline/app/services/clickhouse_analytics"
	currency "github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker/usecase"
	"github.com/stivens13/horizon-data-pipeline/app/services/etl"
	gcs "github.com/stivens13/horizon-data-pipeline/app/services/gcstorage/usecase"
	"log"
	"os"
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
	currencyInteractor := currency.NewCurrencyInteractor(c.CurrencyConfig, GCSInteractor)
	return &Services{
		Clickhouse:         clickhouse,
		GCSInteractor:      GCSInteractor,
		CurrencyInteractor: currencyInteractor,
		ETL:                etl.NewETL(GCSInteractor, clickhouse, currencyInteractor),
	}
}

func InitState(c *config.AppDriverConfig, services *Services) {
	if c.InitFromScratch {
		fmt.Println("Initializing State From Scratch")
		if err := services.CurrencyInteractor.InitializeCurrencyDataFromScratch(); err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	fmt.Println("ETL application starts, initializing services...")
	cfg := config.InitConfig()
	services := InitServices(cfg)

	InitState(cfg.AppDriverConfig, services)

	fmt.Println("Start ETL Sequence")
	date := os.Getenv("ETL_DATE")
	if err := services.ETL.StartETL(date); err != nil {
		log.Fatalf("failed to perform ETL: %v", err)
	}
	fmt.Println("ETL Sequence Complete")

	return
}
