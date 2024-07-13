package clickhouse_analytics

import (
	"fmt"
	std_ck "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	ch_driver "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"log"
	"time"
)

type ClickhouseAnalytics struct {
	DB *gorm.DB
}

func NewClickhouseAnalytics(chConfig *config.ClickhouseConfig) *ClickhouseAnalytics {
	dsn := chConfig.DSN()
	fmt.Printf("dsn: %s\n", dsn)

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
		Debug: true,
	})
	clickhouseDB, err := gorm.Open(ch_driver.New(
		ch_driver.Config{Conn: sqlDB}))

	//clickhouseDB, err := gorm.Open(ch_driver.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to establish clickhouse database connection: %v", err)
	}
	return &ClickhouseAnalytics{DB: clickhouseDB}
}
