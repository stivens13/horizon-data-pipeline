package clickhouse_analytics

import (
	"github.com/google/uuid"
	"gorm.io/gorm"
	"time"
)

func NewUUID() string {
	return uuid.New().String()
}

type ModelBase struct {
	gorm.Model
	ID        string `json:"id" gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
	//DeletedAt gorm.DeletedAt
}

func (mb *ModelBase) BeforeCreate(tx *gorm.DB) error {
	mb.ID = uuid.New().String()
	return nil
}

type DailyMarketVolumePerProject struct {
	ModelBase
	Date               string  `json:"date" gorm:"date"`
	ProjectID          string  `json:"project_id" gorm:"project_id"`
	TransactionsAmount int64   `json:"transactions_amount" gorm:"transactions_amount"`
	TotalVolumeUSD     float64 `json:"total_volume_usd" gorm:"total_volume_usd"`
}

func (dmvp *DailyMarketVolumePerProject) TableName() string {
	return "daily_market_volume_per_project"
}

func (dmvp *DailyMarketVolumePerProject) IncrementTxsAmount() {
	dmvp.TransactionsAmount += 1
}

func (dmvp *DailyMarketVolumePerProject) AddToVolume(amount float64) {
	dmvp.TotalVolumeUSD += amount
}

type DailyTotalMarketVolume struct {
	ModelBase
	Date               string  `json:"date" gorm:"date"`
	TransactionsAmount int64   `json:"transactions_amount" gorm:"transactions_amount"`
	TotalVolumeUSD     float64 `json:"total_volume_usd" gorm:"total_volume_usd"`
}

func (DailyTotalMarketVolume) TableName() string {
	return "daily_total_market_volume"
}
