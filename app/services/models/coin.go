package models

// Coin represents a coin from the CoinGecko API
type Coin struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
	Name   string `json:"name"`
}
