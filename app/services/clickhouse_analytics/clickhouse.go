package clickhouse_analytics

import (
	"fmt"
	std_ck "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	ch_driver "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"log"
	"time"
)

type ClickhouseRepository struct {
	DB *gorm.DB
}

func NewClickhouseRepository(chConfig *config.ClickhouseConfig) *ClickhouseRepository {

	sqlDB := std_ck.OpenDB(&std_ck.Options{
		Addr: []string{fmt.Sprintf("%s:%s", chConfig.Host, chConfig.Port)},
		Auth: std_ck.Auth{
			Database: chConfig.Database,
			Username: chConfig.User,
			Password: chConfig.Password,
		},
		Settings: std_ck.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &std_ck.Compression{
			std_ck.CompressionLZ4,
			1,
		},
		Debug: false,
	})
	clickhouseDB, err := gorm.Open(ch_driver.New(
		ch_driver.Config{Conn: sqlDB}))

	if err != nil {
		log.Fatalf("failed to establish clickhouse database connection: %v", err)
	}
	return &ClickhouseRepository{DB: clickhouseDB}
}

func (cr *ClickhouseRepository) CreateDailyTotalVolume(dailyTotalVolume *models.DailyMarketVolume) error {
	if err := cr.DB.Create(&dailyTotalVolume).Error; err != nil {
		return fmt.Errorf("failed to create Daily Total Volume: %v", err)
	}
	return nil
}

func (cr *ClickhouseRepository) CreateDailyVolumePerProject(dailyVolumePerProject []*models.DailyProjectVolume) error {
	if err := cr.DB.Create(&dailyVolumePerProject).Error; err != nil {
		return fmt.Errorf("failed to create Daily Volume Per Project: %v", err)
	}
	return nil
}
