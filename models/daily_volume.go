package models

type DailyVolumeData struct {
	Date              string  `json:"date"`
	ProjectID         int     `json:"project_id"`
	TransactionAmount int     `json:"transaction_amount"`
	TotalVolume       float64 `json:"total_volume_volume"`
}
