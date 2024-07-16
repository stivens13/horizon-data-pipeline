package currency_repository

import (
	"encoding/json"
	"fmt"
	"github.com/go-resty/resty/v2"
	models2 "github.com/stivens13/horizon-data-pipeline/app/services/models"
	"github.com/stivens13/horizon-data-pipeline/app/tools/constants"
	"time"
)

var (
	baseURL                     = "https://api.coingecko.com/api/v3"
	coinGeckoHistoricalChartURL = "https://api.coingecko.com/api/v3/coins/%s/market_chart/range?vs_currency=usd&from=%d&to=%d&precision=full"
	listAllCoinsURL             = "https://api.coingecko.com/api/v3/coins/list"
	SymbolToID                  = map[string]string{}
	CurrencyInUSDByID           = map[string]float64{}
)

type CurrencyRepository struct {
	CGBaseURL string
}

func NewCurrencyRepository() CurrencyRepository {
	return CurrencyRepository{
		CGBaseURL: baseURL,
	}
}

// FetchHistoricalData fetches the historical market data for a given coin on a specific date
func (ctr *CurrencyRepository) FetchHistoricalData(coinID, date string) (*models2.HistoricalData, error) {
	client := resty.New()

	dateTime, err := time.Parse(constants.DateKeyLayout, date)
	if err != nil {
		return nil, fmt.Errorf("failed to parse date: %w", err)
	}

	startTime := dateTime.UTC().Truncate(24 * time.Hour)
	endTime := dateTime.UTC().Add(24*time.Hour - 1*time.Second)

	url := fmt.Sprintf(coinGeckoHistoricalChartURL, coinID, startTime.Unix(), endTime.Unix())
	resp, err := client.R().Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	var data models2.HistoricalData
	if err := json.Unmarshal(resp.Body(), &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &data, nil
}

// FetchAllCoinsData fetches the list of coins with platform data from the CoinGecko API
func (ctr *CurrencyRepository) FetchAllCoinsData() ([]models2.Currency, error) {
	client := resty.New()

	url := "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
	resp, err := client.R().Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch data: %w", err)
	}

	if resp.StatusCode() != 200 {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode())
	}

	var coins []models2.Currency
	if err := json.Unmarshal(resp.Body(), &coins); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return coins, nil
}

//func FetchCoinHistory() {
//	cgClient := goingecko.NewClient(nil, os.Getenv("COINGECKO_API_KEY"), false)
//	defer cgClient.Close()
//	cgClient.CoinsIdHistory()
//	data, err := cgClient.CoinsId("bitcoin", true, true, true, false, false, false)
//	if err != nil {
//		fmt.Print("Somethig went wrong...")
//		return
//	}
//	fmt.Printf("Bitcoin price is: %f$", data.MarketData.CurrentPrice.Usd)
//}
