package currency_usecase

import (
	"context"
	"fmt"
	"github.com/gocarina/gocsv"
	repo "github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker/repository"
	gcs "github.com/stivens13/horizon-data-pipeline/app/services/gcstorage/usecase"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
)

type CurrencyInteractor struct {
	Repo repo.CurrencyRepository
	GCS  *gcs.GCSInteractor
}

func NewCurrencyInteractor(storage *gcs.GCSInteractor) *CurrencyInteractor {
	return &CurrencyInteractor{
		Repo: repo.NewCurrencyRepository(),
		GCS:  storage,
	}
}

func (ci *CurrencyInteractor) UpdateDailyCurrencyPrices(ctx context.Context, date string) error {
	return fmt.Errorf("not implemented")
}

func (ci *CurrencyInteractor) UpdateCurrencyRegistry() error {
	coins, err := ci.Repo.FetchAllCoinsData()
	if err != nil {
		return fmt.Errorf("could not fetch all coins data: %w", err)
	}

	currencyRegistry := ConvertCoinsToSymbolRegistry(coins)
	currencyRegistryCSV, err := gocsv.MarshalString(&currencyRegistry)
	if err != nil {
		return fmt.Errorf("could not marshal symbol registry CSV: %w", err)
	}

	if err := ci.GCS.UpdateCurrencyRegistry([]byte(currencyRegistryCSV)); err != nil {
		return fmt.Errorf("could not update currency registry: %w", err)
	}

	return nil
}

func (ci *CurrencyInteractor) UpdateTrackedCurrencies(untracked map[string]models.Transaction) (err error) {
	var registry models.RegistryMap
	if registry, err = ci.GCS.GetCurrencyRegistry(); err != nil {
		return fmt.Errorf("could not get currency registry: %w", err)
	}
	var trackedCurrencies []models.TrackedCurrency
	if trackedCurrencies, err = ci.GCS.GetTrackedCurrencies(); err != nil {
		return fmt.Errorf("could not fetch tracked currencies: %w", err)
	}

	var currenciesToTrack []models.TrackedCurrency
	for symbol, tx := range untracked {
		if _, ok := registry[symbol]; !ok {
			if err := ci.UpdateCurrencyRegistry(); err != nil {
				return fmt.Errorf("currency missing in registry and could not update currency registry: %w", err)
			}
			// validate that missing symbol was successfully loaded
			if _, ok := registry[symbol]; !ok {
				return fmt.Errorf("currency could not be found: %w", err)
			}
		}

		var currencyID string
		if _, ok := registry[symbol][tx.CurrencyAddress]; !ok {
			if _, ok := registry[symbol]["0"]; !ok {
				return fmt.Errorf("currency id could not be found: %w", err)
			}

			currencyID = registry[symbol]["0"]
		}

		if currencyID != "" {
			currencyID = registry[symbol][tx.CurrencyAddress]
		}

		currenciesToTrack = append(currenciesToTrack, models.TrackedCurrency{
			ID:       currencyID,
			Symbol:   symbol,
			Platform: tx.CurrencyAddress,
		})
	}

	trackedCurrencies = append(trackedCurrencies, currenciesToTrack...)

	trackedCurrenciesCSV, err := gocsv.MarshalString(&trackedCurrencies)

	if err := ci.GCS.UpdateTrackedCurrencies([]byte(trackedCurrenciesCSV)); err != nil {
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

func ConvertCoinsToSymbolRegistry(coins []models.Currency) (symbolStorage []models.Registry) {
	SymbolToAddressWithIDMap := make(map[string]models.Platforms)
	for _, coin := range coins {
		if _, ok := SymbolToAddressWithIDMap[coin.Symbol]; !ok {
			SymbolToAddressWithIDMap[coin.Symbol] = models.Platforms{}
		}
		if len(coin.Platforms) == 0 {
			SymbolToAddressWithIDMap[coin.Symbol]["0"] = coin.ID
			continue
		}
		for _, address := range coin.Platforms {
			SymbolToAddressWithIDMap[coin.Symbol][address] = coin.ID
		}
	}

	for symbol, val := range SymbolToAddressWithIDMap {
		symbolStorage = append(symbolStorage, models.Registry{
			Symbol:           symbol,
			PlatformsWithIds: val,
		})
	}
	return symbolStorage
}
