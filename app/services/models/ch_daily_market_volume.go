package models

type DailyMarketVolume struct {
	ModelBase
	Date               string  `json:"date" gorm:"date"`
	TransactionsAmount int64   `json:"transactions_amount" gorm:"transactions_amount"`
	TotalVolumeUSD     float64 `json:"total_volume_usd" gorm:"total_volume_usd"`
}

func (DailyMarketVolume) TableName() string {
	return "daily_market_volume"
}
