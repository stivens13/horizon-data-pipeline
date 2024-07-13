package etl

import (
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker"
	"github.com/stivens13/horizon-data-pipeline/app/services/gcp"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	"os"
	"path"
	"sort"
)

type ETL struct {
	GCPStorage *gcp.GCPStorage
	//CurrencyTracker *currency
}

var (
	filename      = "sample_data.csv"
	bucket        = ""
	localDataDir  = "data/"
	localFilepath = path.Join(localDataDir, filename)
)

const (
	dateKeyLayout = "20060102"
	EVENT_BUY     = "BUY_ITEMS"
	EVENT_SELL    = "SELL_ITEMS"
)

func (e *ETL) ExtractTxs(filename string) error {
	if err := e.GCPStorage.DownloadFile(bucket, filename, localDataDir+filename); err != nil {
		return fmt.Errorf("error downloading data from GCS: %w", err)
	}

	return nil
}

func (e *ETL) ReadData(filepath string) (txs []*models.TransactionRaw, err error) {
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

func (e *ETL) TransformTxs() error {
	txsRaw, err := e.ReadData(localFilepath)
	if err != nil {
		return fmt.Errorf("error reading transaction data: %w", err)
	}
	txsByDate := map[string][]*models.Transaction{}
	for _, tx := range txsRaw {
		date := tx.Timestamp.Format(dateKeyLayout)
		txsByDate[date] = append(txsByDate[date], tx.ToTransaction())
	}

	keys := make([]string, 0, len(txsByDate))

	for k := range txsByDate {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		txs := txsByDate[k]
		totalVolume := e.CalculateDailyVolume(txs)
		fmt.Printf("Date: %s, volume: %f\n", k, totalVolume)
	}

	return nil
}

func (e *ETL) CalculateDailyVolume(txs []*models.Transaction) float64 {
	//tradesByCurrency := map[string]float64{}
	totalVolume := 0.0
	for _, tx := range txs {
		totalVolume += tx.CurrencyValue * currency_tracker.GetCurrencyDailyValueInUSD(tx.CurrencySymbol)
	}

	return totalVolume
}

func (e *ETL) LoadTxs(date string) error {
	return nil
}
