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
	//if err := clickhouse.DB.Exec("select 1;").Error; err != nil {
	//	log.Fatal(err)
	//}
	GCSInteractor := gcs.NewGCSInteractor(c.GCSConfig)
	currencyInteractor := currency.NewCurrencyInteractor(c.CurrencyConfig, GCSInteractor)
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

	//if err := services.CurrencyInteractor.InitializeCurrencyDataFromScratch(); err != nil {
	//	log.Fatal(err)
	//}
	//var err error
	//var data []byte
	//if data, err = services.GCSInteractor.GetTrackedCurrencies(); err != nil {
	//	log.Fatal(err)
	//}
	//
	//var tc []*models.TrackedCurrency
	//if err = gocsv.UnmarshalBytes(data, &tc); err != nil {
	//	log.Fatalf("failed to unmarshal currency registry, %v", err)
	//}
	//
	//if data, err = services.GCSInteractor.GetCurrencyRegistry(); err != nil {
	//	log.Fatal(err)
	//}
	//
	//var rg []models.Registry
	//if err = gocsv.UnmarshalBytes(data, &rg); err != nil {
	//	log.Fatalf("failed to unmarshal currency registry, %v", err)
	//}

	//if err := services.CurrencyInteractor.UpdateCurrencyRegistry(); err != nil {
	//	log.Fatal(err)
	//}

	fmt.Println("Start ETL Sequence")
	date := "2024-04-01"
	if err := services.ETL.StartETL(date); err != nil {
		log.Fatalf("failed to perform ETL: %v", err)
	}
	fmt.Println("ETL Sequence Complete")

	return
}
