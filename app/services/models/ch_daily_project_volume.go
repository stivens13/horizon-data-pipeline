package models

type DailyProjectVolume struct {
	ModelBase
	Date               string  `json:"date" gorm:"date"`
	ProjectID          string  `json:"project_id" gorm:"project_id"`
	TransactionsAmount int64   `json:"transactions_amount" gorm:"transactions_amount"`
	TotalVolumeUSD     float64 `json:"total_volume_usd" gorm:"total_volume_usd"`
}

func (dpv *DailyProjectVolume) TableName() string {
	return "daily_project_volume"
}

func (dpv *DailyProjectVolume) IncrementTxsAmount() {
	dpv.TransactionsAmount += 1
}

func (dpv *DailyProjectVolume) AddToVolume(amount float64) {
	dpv.TotalVolumeUSD += amount
}
