package models

type DailyPrices map[string]*CurrencyPrice

type CurrencyPrice struct {
	TrackedCurrency
	Price float64 `json:"price" csv:"price"`
}
