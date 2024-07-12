package currency_tracker

import (
	"encoding/json"
	"github.com/stivens13/horizon-data-pipeline/models"
	"net/http"
)

type CurrencyTracker struct {
}

var SymbolToID = map[string]string{}
var CurrencyInUSDByID = map[string]float64{}

// PriceResponse represents the price response from the CoinGecko API
//type PriceResponse map[string]map[string]float64

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

// TODO: implement actual fetch
func FetchCurrencyData(id string) {
	CurrencyInUSDByID[id] = 1.0
}

func GetCurrencyDailyValueInUSD(symbol string) float64 {
	// check if SymbolToID is populated, otherwise call FetchCoins to populate it
	if _, ok := SymbolToID[symbol]; !ok {
		FetchCoins()
	}

	id := SymbolToID[symbol]

	if _, ok := CurrencyInUSDByID[id]; !ok {
		FetchCurrencyData(id)
	}

	return CurrencyInUSDByID[id]
}

// Fetch All Coins Data
func FetchCoins() (coins []models.Coin, err error) {
	resp, err := http.Get("https://api.coingecko.com/api/v3/coins/list")
	if err != nil {
		return coins, err
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(&coins); err != nil {
		return coins, err
	}

	for _, coin := range coins {
		SymbolToID[coin.Symbol] = coin.ID
	}

	return coins, nil
}
