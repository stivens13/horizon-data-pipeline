package etl

import (
	"fmt"
	"github.com/google/martian/v3/log"
	cha "github.com/stivens13/horizon-data-pipeline/app/services/clickhouse_analytics"
	currencyusecase "github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker/usecase"
	gcs "github.com/stivens13/horizon-data-pipeline/app/services/gcstorage/usecase"
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
	txs, dailyPrices, err := e.ExtractData(date)
	if err != nil {
		return fmt.Errorf("failed extracting transaction data: %w", err)
	}

	totalVolume, volumePerProject, err := e.TransformTxs(date, txs, dailyPrices)
	if err != nil {
		return fmt.Errorf("failed to process daily transactions into daily volumes: %w", err)
	}

	if err = e.LoadAnalytics(totalVolume, volumePerProject); err != nil {
		return fmt.Errorf("failed to load daily volumes to analytics storage: %w", err)
	}

	return nil
}

func (e *ETL) ExtractData(date string) (txs []*models.Transaction, dailyPrices models.DailyPrices, err error) {
	var txsRaw models.TransactionsRawView
	if txsRaw, err = e.GCStorage.GetDailyTxs(date); err != nil {
		return txs, dailyPrices, fmt.Errorf("failed to read daily transactions file for date %s: %w", date, err)
	}

	if dailyPrices, err = e.GCStorage.FetchDailyPrices(date); err != nil {
		return txs, dailyPrices, fmt.Errorf("failed to read daily prices for date %s: %w", date, err)
	}

	if txs, dailyPrices, err = e.PreprocessTxs(date, txsRaw, dailyPrices); err != nil {
		return txs, dailyPrices, fmt.Errorf("failed to preprocess txs for date %s: %w", date, err)
	}

	return txs, dailyPrices, nil
}

func (e *ETL) TransformTxs(date string, txs []*models.Transaction, dailyPrices models.DailyPrices) (totalVolume *models.DailyMarketVolume, volumePerProject []*models.DailyProjectVolume, err error) {

	totalVolumeUSD, totalTxs, volumePerProject := e.CalculateDailyVolume(date, txs, dailyPrices)

	totalVolume = &models.DailyMarketVolume{
		Date:               date,
		TransactionsAmount: totalTxs,
		TotalVolumeUSD:     totalVolumeUSD,
	}

	return totalVolume, volumePerProject, nil
}

func (e *ETL) LoadAnalytics(
	totalMarketVolume *models.DailyMarketVolume,
	volumePerProject []*models.DailyProjectVolume,
) error {
	if err := e.Clickhouse.CreateDailyTotalVolume(totalMarketVolume); err != nil {
		return fmt.Errorf("failed to upload daily total market volume: %w", err)
	}

	if err := e.Clickhouse.CreateDailyVolumePerProject(volumePerProject); err != nil {
		return fmt.Errorf("failed to upload daily total market volume: %w", err)
	}
	return nil
}

func (e *ETL) PreprocessTxs(
	date string,
	txsRaw models.TransactionsRawView,
	dailyPrices models.DailyPrices,
) (
	txs []*models.Transaction,
	prices models.DailyPrices,
	err error,
) {
	var (
		symbolMap       models.TxsSymbolMap
		invalidTxsCount int64
	)
	txs, invalidTxsCount, symbolMap = filterTxs(date, txsRaw)
	if invalidTxsCount != 0 {
		log.Errorf("invalid transactions count: %d", invalidTxsCount)
	}

	if len(txs) == 0 {
		return txs, dailyPrices, fmt.Errorf("no transactions to process")
	}

	var untracked models.TxsSymbolMap
	for key := range symbolMap {
		if _, ok := dailyPrices[key]; !ok {
			if _, ok := untracked[key]; ok {
				untracked[key] = symbolMap[key]
			}
		}
	}

	if len(untracked) > 0 {
		log.Infof("untracked currencies found, updating tracked currencies and daily prices")
		if err := e.Currency.UpdateTrackedCurrencies(untracked); err != nil {
			return txs, dailyPrices, fmt.Errorf("failed to update tracked currencies: %w", err)
		}

		if err := e.Currency.UpdateDailyCurrencyPrices(date); err != nil {
			return txs, dailyPrices, fmt.Errorf("failed to update daily prices: %w", err)
		}

		if dailyPrices, err = e.GCStorage.FetchDailyPrices(date); err != nil {
			return txs, dailyPrices, fmt.Errorf("failed to fetch daily prices: %w", err)
		}
	}

	return txs, dailyPrices, nil
}

func (e *ETL) CalculateDailyVolume(
	date string,
	txs []*models.Transaction,
	prices models.DailyPrices,
) (
	totalVolumeUSD float64,
	totalTxs int64,
	volumePerProject []*models.DailyProjectVolume,
) {
	volumePerProjectMap := map[string]*models.DailyProjectVolume{}
	for _, tx := range txs {
		symbol := tx.Symbol()
		if _, ok := prices[symbol]; !ok {
			log.Errorf("invalid currency symbol: %s", symbol)
			continue
		}
		symbolPrice := prices[symbol].Price
		txVal := tx.Val()
		projectID := tx.ProjectID
		txUSDValue := txVal * symbolPrice
		if _, ok := volumePerProjectMap[projectID]; !ok {
			volumePerProjectMap[projectID] = &models.DailyProjectVolume{
				Date:               date,
				ProjectID:          projectID,
				TransactionsAmount: int64(0),
				TotalVolumeUSD:     float64(0),
			}
		}
		project := volumePerProjectMap[projectID]
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

func filterTxs(date string, txsRaw models.TransactionsRawView) (
	txsFiltered []*models.Transaction,
	invalidTxsCount int64,
	symbolMap models.TxsSymbolMap,
) {
	for _, tx := range txsRaw.Data {
		txDate := tx.Timestamp.Format(constants.DateKeyLayout)
		if date != txDate {
			invalidTxsCount += 1
			log.Errorf("invalid transaction - date mismatch, processing date: %s and tx date: %s, tx details: %+v", date, txDate, tx)
			continue
		}

		if tx.Props.CurrencyAddress == constants.GenesisAddress {
			log.Infof("burn transaction for symbol: %s: tx amount: %s", tx.Props.CurrencySymbol, tx.Nums.CurrencyValueDecimal)
			continue
		}

		txsFiltered = append(txsFiltered, tx.ToTransaction())
	}

	return txsFiltered, invalidTxsCount, symbolMap
}
