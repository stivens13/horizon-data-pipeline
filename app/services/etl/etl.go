package etl

import (
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/google/martian/v3/log"
	cha "github.com/stivens13/horizon-data-pipeline/app/services/clickhouse_analytics"
	"github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker"
	gcp_gateway "github.com/stivens13/horizon-data-pipeline/app/services/gcp-gateway"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	"os"
	"path"
)

type ETL struct {
	GCPStorage *gcp_gateway.Storage
	Clickhouse *cha.ClickhouseRepository
	//CurrencyTracker *currency
}

func NewETL(gcpStorage gcp_gateway.Storage, clickhouseClient *cha.ClickhouseRepository) *ETL {
	return &ETL{
		GCPStorage: &gcpStorage,
		Clickhouse: clickhouseClient,
	}
}

var (
	filename      = "sample_data.csv"
	bucket        = ""
	localDataDir  = "data/"
	localFilepath = path.Join(localDataDir, filename)
)

const (
	dateKeyLayout = "20060102"
)

func (e *ETL) StartETL(date string) error {
	//if err := e.ExtractTxs(date); err != nil {
	//	return fmt.Errorf("failed transactions extraction: %w", err)
	//}

	totalVolume, volumePerProject, err := e.TransformTxs(date)
	if err != nil {
		return fmt.Errorf("failed to process daily transactions into daily volumes: %w", err)
	}

	if err = e.LoadTxs(totalVolume, volumePerProject); err != nil {
		return fmt.Errorf("failed to load daily volumes to analytics storage: %w", err)
	}

	return nil
}

//func (e *ETL) ExtractTxs(date string) error {
//	if err := e.GCPStorage.DownloadFile(bucket, filename, localDataDir+filename); err != nil {
//		return fmt.Errorf("error downloading data from GCS: %w", err)
//	}
//	return nil
//}

func (e *ETL) readData(filepath string) (txs []*models.TransactionRaw, err error) {
	pwd, _ := os.Getwd()
	fmt.Printf("Current pwd: %s\n", pwd)
	txsFile, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return txs, fmt.Errorf("failed to open data file %v: %w", filename, err)
	}
	defer txsFile.Close()

	if err := gocsv.UnmarshalFile(txsFile, &txs); err != nil { // Load txs from file
		return txs, fmt.Errorf("failed to unmarshall csv file %v: %w", filename, err)
	}

	return txs, nil
}

func filterTxs(date string, txsRaw []*models.TransactionRaw) (txsFiltered []*models.Transaction, invalidTxsCount int32) {

	for _, txRaw := range txsRaw {
		txDate := txRaw.Timestamp.Format(dateKeyLayout)
		if date != txDate {
			invalidTxsCount += 1
			log.Errorf("invalid transaction reported, processing date: %s and tx date: %s, tx details: %+v", date, txDate, txRaw)
			continue
		}

		txsFiltered = append(txsFiltered, txRaw.ToTransaction())
	}

	return txsFiltered, invalidTxsCount
}

func (e *ETL) TransformTxs(date string) (totalVolume *cha.DailyTotalMarketVolume, volumePerProject []*cha.DailyMarketVolumePerProject, err error) {
	txsRaw, err := e.readData(localFilepath)
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
		Date:              date,
		TransactionAmount: totalTxs,
		TotalVolume:       totalVolumeUSD,
	}

	return totalVolume, volumePerProject, nil
}

func (e *ETL) CalculateDailyVolume(date string, txs []*models.Transaction) (totalVolumeUSD float64, totalTxs int64, volumePerProject []*cha.DailyMarketVolumePerProject) {
	volumePerProjectMap := map[string]*cha.DailyMarketVolumePerProject{}
	for _, tx := range txs {
		txUSDValue := tx.CurrencyValue * currency_tracker.GetCurrencyDailyValueInUSD(tx.CurrencySymbol)
		if _, ok := volumePerProjectMap[tx.CurrencySymbol]; !ok {
			volumePerProjectMap[tx.ProjectID] = &cha.DailyMarketVolumePerProject{
				Date:              date,
				ProjectID:         tx.ProjectID,
				TransactionAmount: 0,
				TotalVolume:       float64(0),
			}
		}
		volumePerProjectMap[tx.ProjectID].TotalVolume += txUSDValue
		totalVolumeUSD += txUSDValue
		totalTxs += 1
	}

	for _, volume := range volumePerProjectMap {
		volumePerProject = append(volumePerProject, volume)
	}

	return totalVolumeUSD, totalTxs, volumePerProject
}

func (e *ETL) LoadTxs(totalMarketVolume *cha.DailyTotalMarketVolume, volumePerProject []*cha.DailyMarketVolumePerProject) error {
	if err := e.Clickhouse.UploadDailyTotalVolume(totalMarketVolume); err != nil {
		return fmt.Errorf("failed to upload daily total market volume: %w", err)
	}

	if err := e.Clickhouse.UploadDailyVolumePerProject(volumePerProject); err != nil {
		return fmt.Errorf("failed to upload daily total market volume: %w", err)
	}
	return nil
}
