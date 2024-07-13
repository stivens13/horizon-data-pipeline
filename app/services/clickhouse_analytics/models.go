package clickhouse_analytics

type DailyMarketVolume struct {
	ID                uint64  `json:"id"`
	Date              string  `json:"date"`
	ProjectID         int     `json:"project_id"`
	TransactionAmount int     `json:"transaction_amount"`
	TotalVolume       float64 `json:"total_volume_volume"`
}
