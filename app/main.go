package main

import (
	"fmt"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	"github.com/stivens13/horizon-data-pipeline/app/services/clickhouse_analytics"
	"time"
)

func main() {
	config := config.InitConfig()
	clickhouseAnalytics := clickhouse_analytics.NewClickhouseAnalytics(config.ClickhouseConfig)
	if err := clickhouseAnalytics.DB.Exec("SHOW USERS;").Error; err != nil {
		fmt.Println(err)
		time.Sleep(10 * time.Minute)
	}

	fmt.Printf("Connected to clickhouse successfully")
}
