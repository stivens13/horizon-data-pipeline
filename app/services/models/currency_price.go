package models

// DailyPrices holds average currency price for each currency symbol
// map[Symbol]price
type DailyPrices map[string]*CurrencyPrice

type CurrencyPrice struct {
	TrackedCurrency
	Price float64 `json:"price" csv:"price"`
}
