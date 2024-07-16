package models

type TrackedCurrency struct {
	ID       string `json:"id" csv:"id"`
	Symbol   string `json:"symbol" csv:"symbol"`
	Platform string `json:"platform" csv:"platform"`
}
