package models

// Currency represents a cryptocurrency with its ID, symbol, name, and platforms
type Currency struct {
	ID        string    `json:"id"`
	Symbol    string    `json:"symbol"`
	Name      string    `json:"name"`
	Platforms Platforms `json:"platforms"`
}
