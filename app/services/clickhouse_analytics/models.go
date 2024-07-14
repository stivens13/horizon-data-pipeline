package clickhouse_analytics

type DailyMarketVolumePerProject struct {
	ID                uint64  `json:"id" gorm:"primaryKey"`
	Date              string  `json:"date" gorm:"date"`
	ProjectID         string  `json:"project_id" gorm:"project_id"`
	TransactionAmount int64   `json:"transaction_amount" gorm:"transaction_amount"`
	TotalVolume       float64 `json:"total_volume_volume" gorm:"total_volume"`
}

type DailyTotalMarketVolume struct {
	ID                uint64  `json:"id" gorm:"primaryKey"`
	Date              string  `json:"date" gorm:"date"`
	TransactionAmount int64   `json:"transaction_amount" gorm:"transaction_amount"`
	TotalVolume       float64 `json:"total_volume_volume" gorm:"total_volume"`
}
