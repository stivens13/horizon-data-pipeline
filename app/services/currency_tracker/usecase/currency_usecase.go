package currency_usecase

import (
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/google/martian/v3/log"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	repo "github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker/repository"
	gcs "github.com/stivens13/horizon-data-pipeline/app/services/gcstorage/usecase"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	"github.com/stivens13/horizon-data-pipeline/app/tools/constants"
	"github.com/stivens13/horizon-data-pipeline/app/tools/helper"
	"os"
	"strings"
	"time"
)

type CurrencyInteractor struct {
	Repo repo.CurrencyRepository
	GCS  *gcs.GCSInteractor
}

func NewCurrencyInteractor(c *config.CurrencyConfig, storage *gcs.GCSInteractor) *CurrencyInteractor {
	return &CurrencyInteractor{
		Repo: repo.NewCurrencyRepository(c),
		GCS:  storage,
	}
}

func (ci *CurrencyInteractor) InitializeCurrencyDataFromScratch() (err error) {
	if err := ci.GCS.DestroyAllBuckets(); err != nil {
		return fmt.Errorf("failed to destroy all buckets: %w", err)
	}

	if err := ci.GCS.InitializeBuckets(); err != nil {
		return fmt.Errorf("failed to create buckets: %w", err)
	}

	if _, err = ci.UpdateCurrencyRegistry(); err != nil {
		return fmt.Errorf("failed to create buckets: %w", err)
	}
	var data []byte
	if data, err = ci.GCS.GetCurrencyRegistry(); err != nil {
		return fmt.Errorf("failed to get currency registry: %w", err)
	}

	var reg models.RegistryView
	if err = gocsv.UnmarshalBytes(data, &reg.Data); err != nil {
		return fmt.Errorf("failed to unmarshal currency registry: %w", err)
	}

	registryMap := reg.ToRegistryMap()

	filename := os.Getenv("SEED_DATA")
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to open seed data: %w", err)
	}
	defer file.Close()

	var txs []*models.TransactionRaw
	if err := gocsv.UnmarshalFile(file, &txs); err != nil { // Load txs from file
		return fmt.Errorf("failed to unmarshal seed data: %w", err)
	}

	txsByDate := map[string][]*models.TransactionRaw{}
	currencyTrackedMap := map[string]*models.TrackedCurrency{}

	// group txs by date to upload seed data by day and initialize currency registry
	for _, tx := range txs {
		txDate := tx.Timestamp.Format(constants.DateKeyLayout)
		if _, ok := txsByDate[txDate]; !ok {
			txsByDate[txDate] = []*models.TransactionRaw{}
		}
		txsByDate[txDate] = append(txsByDate[txDate], tx)

		// make list of currencies to track

		// check for genesis-related transactions
		// keep track of valid business-logic currencies only
		if helper.IsGenesisAddress(tx.Props.CurrencyAddress) {
			continue
		}

		platform := tx.Props.CurrencyAddress
		symbol := strings.ToLower(tx.Props.CurrencySymbol)
		var currencyID string

		if _, ok := registryMap[symbol]; ok {
			if _, ok := registryMap[symbol][platform]; ok {
				currencyID = registryMap[symbol][platform]
			}

			// populate currencies to track by id
			if _, ok := currencyTrackedMap[currencyID]; !ok {
				currencyTrackedMap[currencyID] = &models.TrackedCurrency{
					ID:       currencyID,
					Symbol:   symbol,
					Platform: platform,
				}
			}

			continue
		}

		log.Errorf("failed to find currency with symbol %s for platform %s", symbol, platform)
	}

	var currencyTracked []*models.TrackedCurrency
	for _, currency := range currencyTrackedMap {
		currencyTracked = append(currencyTracked, currency)
	}

	data = []byte{}
	if data, err = gocsv.MarshalBytes(currencyTracked); err != nil {
		return fmt.Errorf("failed to marshal currency tracked data: %w", err)
	}
	if err = ci.GCS.UploadTrackedCurrencies(data); err != nil {
		return fmt.Errorf("failed to upload tracked currencies: %w", err)
	}

	for key, txsPerDay := range txsByDate {
		date := key
		var data []byte
		if data, err = gocsv.MarshalBytes(txsPerDay); err != nil {
			return fmt.Errorf("failed to marshal transactions: %w", err)
		}
		if err := ci.GCS.UploadDailyTxs(date, data); err != nil {
			return fmt.Errorf("failed to marshal transactions: %w", err)
		}
		if err := ci.UpdateDailyCurrencyPrices(date); err != nil {
			return fmt.Errorf("failed to update daily prices: %w", err)
		}
	}

	return nil
}

func (ci *CurrencyInteractor) UpdateDailyCurrencyPrices(date string) (err error) {
	var data []byte
	if data, err = ci.GCS.GetTrackedCurrencies(); err != nil {
		return fmt.Errorf("failed to get tracked currencies: %w", err)
	}

	var trackedCoins []models.TrackedCurrency
	if err = gocsv.UnmarshalBytes(data, &trackedCoins); err != nil {
		return fmt.Errorf("failed to unmarshal currency registry, %w", err)
	}

	fmt.Printf("len: %d, currencies: %+v\n", len(trackedCoins), trackedCoins)

	var currencyPrices []*models.CurrencyPrice
	for _, coin := range trackedCoins {
		var historicalData *models.HistoricalData
		if historicalData, err = ci.Repo.FetchHistoricalData(coin.ID, date); err != nil {
			return fmt.Errorf("failed to fetch historical data for coin: %s: %w", coin.ID, err)
		}
		if len(historicalData.Prices) == 0 {
			log.Errorf("no data available for coin: %s, platform: %s", coin.ID, coin.Platform)
			continue
		}
		sleepTime := 20
		fmt.Printf("sleep for %d seconds to not bust API limis\n", sleepTime)
		time.Sleep(20 * time.Second)

		var priceUSD float64
		if priceUSD, err = CalculateAveragePrice(historicalData); err != nil {
			return fmt.Errorf("failed to calculate average price for coin: %s: %w", coin.ID, err)
		}
		currencyPrices = append(currencyPrices, &models.CurrencyPrice{TrackedCurrency: coin, Price: priceUSD})
	}

	data = []byte{}
	if data, err = gocsv.MarshalBytes(&currencyPrices); err != nil {
		return fmt.Errorf("failed to marshal result to csv: %w", err)
	}

	if err = ci.GCS.UpdateDailyPrices(date, data); err != nil {
		return fmt.Errorf("failed to update daily prices data: %w", err)
	}

	return nil
}

func (ci *CurrencyInteractor) UpdateCurrencyRegistry() (registry models.RegistryView, err error) {
	coins, err := ci.Repo.FetchAllCoinsData()
	if err != nil {
		return registry, fmt.Errorf("could not fetch all coins data: %w", err)
	}

	registry = ConvertCoinsToSymbolRegistry(coins)
	currencyRegistryCSV, err := gocsv.MarshalString(&registry.Data)
	if err != nil {
		return registry, fmt.Errorf("could not marshal symbol registry CSV: %w", err)
	}

	if err := ci.GCS.UpdateCurrencyRegistry([]byte(currencyRegistryCSV)); err != nil {
		return registry, fmt.Errorf("could not update currency registry: %w", err)
	}

	return registry, nil
}

func (ci *CurrencyInteractor) UpdateTrackedCurrencies(untracked map[string]*models.Transaction) (err error) {
	var data []byte
	if data, err = ci.GCS.GetCurrencyRegistry(); err != nil {
		return fmt.Errorf("could not get currency registry: %w", err)
	}
	var registry models.RegistryView
	if err = gocsv.UnmarshalBytes(data, &registry.Data); err != nil {
		return fmt.Errorf("failed to unmarshal registry data: %w", err)
	}

	registryMap := registry.ToRegistryMap()

	data = []byte{}
	if data, err = ci.GCS.GetTrackedCurrencies(); err != nil {
		return fmt.Errorf("could not fetch tracked currencies: %w", err)
	}

	var trackedCurrencies []*models.TrackedCurrency
	if err = gocsv.UnmarshalBytes(data, &trackedCurrencies); err != nil {
		return fmt.Errorf("failed to unmarshal currency registry, %w", err)
	}

	var currenciesToTrack []*models.TrackedCurrency
	for symbol, tx := range untracked {
		if _, ok := registryMap[symbol]; !ok {
			if _, err := ci.UpdateCurrencyRegistry(); err != nil {
				return fmt.Errorf("currency missing in registry and could not update currency registry: %w", err)
			}
			// validate the missing symbol was successfully loaded
			if _, ok := registryMap[symbol]; !ok {
				return fmt.Errorf("currency could not be found: %w", err)
			}
		}

		var currencyID string
		if _, ok := registryMap[symbol][tx.CurrencyAddress]; !ok {
			if _, ok := registryMap[symbol]["0"]; !ok {
				return fmt.Errorf("currency id could not be found: %w", err)
			}

			currencyID = registryMap[symbol]["0"]
		}

		if currencyID != "" {
			currencyID = registryMap[symbol][tx.CurrencyAddress]
		}

		currenciesToTrack = append(currenciesToTrack, &models.TrackedCurrency{
			ID:       currencyID,
			Symbol:   symbol,
			Platform: tx.CurrencyAddress,
		})
	}

	trackedCurrencies = append(trackedCurrencies, currenciesToTrack...)

	trackedCurrenciesCSV, err := gocsv.MarshalString(&trackedCurrencies)

	if err := ci.GCS.UploadTrackedCurrencies([]byte(trackedCurrenciesCSV)); err != nil {
		return fmt.Errorf("could not update tracked currencies: %w", err)
	}

	return nil
}

// CalculateAveragePrice calculates the average price in USD for the given historical data
func CalculateAveragePrice(data *models.HistoricalData) (float64, error) {
	if len(data.Prices) == 0 {
		return 0, fmt.Errorf("no price data available")
	}

	var total float64
	for _, price := range data.Prices {
		total += price[1]
	}

	average := total / float64(len(data.Prices))
	return average, nil
}

func ConvertCoinsToSymbolRegistry(coins []models.Currency) models.RegistryView {
	registry := models.RegistryMap{}
	for _, coin := range coins {
		if _, ok := registry[coin.Symbol]; !ok {
			registry[coin.Symbol] = models.Platforms{}
		}

		// skip coins without platform
		if len(coin.Platforms) == 0 {
			continue
		}
		for _, address := range coin.Platforms {
			registry[coin.Symbol][address] = coin.ID
		}
	}

	return registry.ToRegistryView()
}
