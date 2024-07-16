package etl

import (
	"fmt"
	"github.com/google/martian/v3/log"
	cha "github.com/stivens13/horizon-data-pipeline/app/services/clickhouse_analytics"
	currencyusecase "github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker/usecase"
	gcs "github.com/stivens13/horizon-data-pipeline/app/services/gcstorage/usecase"
	"github.com/stivens13/horizon-data-pipeline/app/tools/helper"

	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	"github.com/stivens13/horizon-data-pipeline/app/tools/constants"
)

type ETL struct {
	GCStorage           *gcs.GCSInteractor
	Clickhouse          *cha.ClickhouseRepository
	Currency            *currencyusecase.CurrencyInteractor
	CurrencyUSDRegistry map[string]float64
}

func NewETL(
	gcpStorage *gcs.GCSInteractor,
	clickhouseClient *cha.ClickhouseRepository,
	currencyInteractor *currencyusecase.CurrencyInteractor,
) *ETL {
	return &ETL{
		GCStorage:  gcpStorage,
		Clickhouse: clickhouseClient,
		Currency:   currencyInteractor,
	}
}

func (e *ETL) StartETL(date string) error {
	//if err := e.ExtractTxs(date); err != nil {
	//	return fmt.Errorf("failed transactions extraction: %w", err)
	//}

	totalVolume, volumePerProject, err := e.TransformTxs(date)
	if err != nil {
		return fmt.Errorf("failed to process daily transactions into daily volumes: %w", err)
	}

	if err = e.LoadTxsToAnalytics(totalVolume, volumePerProject); err != nil {
		return fmt.Errorf("failed to load daily volumes to analytics storage: %w", err)
	}

	return nil
}

func (e *ETL) GetCurrencyUSDRegistry() (CurrencyUSDRegistry map[string]float64, err error) {
	return CurrencyUSDRegistry, fmt.Errorf("not implemented")
}

func (e *ETL) PopulateCurrencyUSDRegistry() error {
	var err error
	e.CurrencyUSDRegistry, err = e.GetCurrencyUSDRegistry()
	if err != nil {
		return fmt.Errorf("failed to fetch CurrencyInteractor USD registry: %w", err)
	}

	return nil
}

func (e *ETL) GetCurrencyValueInUSD(coinID string) float64 {
	// TODO add proper map checks
	return e.CurrencyUSDRegistry[coinID]
}

func (e *ETL) ExtractData(date string) (txs []*models.TransactionRaw, dailyPrices models.DailyPrices, err error) {
	txs, err = e.GCStorage.GetDailyTxs(date)
	if err != nil {
		return txs, dailyPrices, fmt.Errorf("failed to read daily analytics file for date %s: %w", date, err)
	}

	if dailyPrices, err := e.GCStorage.GetDailyPrices(date); err != nil {
		return txs, dailyPrices, fmt.Errorf("failed to read daily prices for date %s: %w", date, err)
	}

	return txs, dailyPrices, nil
}

func (e *ETL) TransformTxs(date string) (
	totalVolume *cha.DailyTotalMarketVolume,
	volumePerProject []*cha.DailyMarketVolumePerProject,
	err error,
) {
	txsRaw, _, err := e.ExtractData(helper.CSVFileDate(date))
	if err != nil {
		return totalVolume, volumePerProject, fmt.Errorf("error reading transaction data: %w", err)
	}

	txs, invalidTxsCount := filterTxs(date, txsRaw)
	if invalidTxsCount != 0 {
		log.Errorf("invalid transactions count: %d", invalidTxsCount)
	}

	var (
		totalVolumeUSD float64
		totalTxs       int64
	)
	totalVolumeUSD, totalTxs, volumePerProject = e.CalculateDailyVolume(date, txs)

	totalVolume = &cha.DailyTotalMarketVolume{
		Date:               date,
		TransactionsAmount: totalTxs,
		TotalVolumeUSD:     totalVolumeUSD,
	}

	return totalVolume, volumePerProject, nil
}

func (e *ETL) CalculateDailyVolume(date string, txs []*models.Transaction) (
	totalVolumeUSD float64,
	totalTxs int64,
	volumePerProject []*cha.DailyMarketVolumePerProject,
) {
	volumePerProjectMap := map[string]*cha.DailyMarketVolumePerProject{}
	for _, tx := range txs {
		txUSDValue := tx.CurrencyValue * e.GetCurrencyValueInUSD(tx.CurrencySymbol)
		if _, ok := volumePerProjectMap[tx.CurrencySymbol]; !ok {
			volumePerProjectMap[tx.ProjectID] = &cha.DailyMarketVolumePerProject{
				Date:               date,
				ProjectID:          tx.ProjectID,
				TransactionsAmount: int64(0),
				TotalVolumeUSD:     float64(0),
			}
		}
		project := volumePerProjectMap[tx.ProjectID]
		project.AddToVolume(txUSDValue)
		project.IncrementTxsAmount()
		totalVolumeUSD += txUSDValue
		totalTxs += 1
	}

	for _, volume := range volumePerProjectMap {
		volumePerProject = append(volumePerProject, volume)
	}

	return totalVolumeUSD, totalTxs, volumePerProject
}

func (e *ETL) LoadTxsToAnalytics(
	totalMarketVolume *cha.DailyTotalMarketVolume,
	volumePerProject []*cha.DailyMarketVolumePerProject,
) error {
	if err := e.Clickhouse.CreateDailyTotalVolume(totalMarketVolume); err != nil {
		return fmt.Errorf("failed to upload daily total market volume: %w", err)
	}

	if err := e.Clickhouse.CreateDailyVolumePerProject(volumePerProject); err != nil {
		return fmt.Errorf("failed to upload daily total market volume: %w", err)
	}
	return nil
}

func filterTxs(
	date string,
	txsRaw []*models.TransactionRaw,
) (
	txsFiltered []*models.Transaction,
	invalidTxsCount int32,
) {

	for _, txRaw := range txsRaw {
		txDate := txRaw.Timestamp.Format(constants.DateKeyLayout)
		if date != txDate {
			invalidTxsCount += 1
			log.Errorf("invalid transaction reported, processing date: %s and tx date: %s, tx details: %+v", date, txDate, txRaw)
			continue
		}

		txsFiltered = append(txsFiltered, txRaw.ToTransaction())
	}

	return txsFiltered, invalidTxsCount
}

//func (e *ETL) readDataFromFile(filepath string) (txs []*models.TransactionRaw, err error) {
//	txsFile, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, os.ModePerm)
//	if err != nil {
//		return txs, fmt.Errorf("failed to open data file %v: %w", filepath, err)
//	}
//	defer txsFile.Close()
//
//	if err := gocsv.UnmarshalFile(txsFile, &txs); err != nil { // Load txs from file
//		return txs, fmt.Errorf("failed to unmarshall csv file %v: %w", filepath, err)
//	}
//
//	return txs, nil
//}

//func (e *ETL) ExtractTxs(date string) error {
//	if err := e.GCStorageRepository.DownloadFile(bucket, filename, localDataDir+filename); err != nil {
//		return fmt.Errorf("error downloading data from GCS: %w", err)
//	}
//	return nil
//}
