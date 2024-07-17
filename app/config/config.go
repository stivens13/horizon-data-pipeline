package config

import (
	"encoding/json"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/stivens13/horizon-data-pipeline/app/tools/helper"
	"log"
	"os"
)

var clickhouseDsnFormat = "clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&read_timeout=20s"

var Cfg *Config

type Config struct {
	GCSConfig        *GCSConfig
	ClickhouseConfig *ClickhouseConfig
	CurrencyConfig   *CurrencyConfig
	AppDriverConfig  *AppDriverConfig
}

type CurrencyConfig struct {
	CoingeckoAPIKEY *string
	LimitRateFlag   bool
}

type GCSConfig struct {
	DailyTxsBucket            string
	CurrencyRegistryBucket    string
	CurrencyRegistryFilename  string
	TrackedCurrenciesFilename string
	DailyCurrencyPricesBucket string
}

type ClickhouseConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	Database string
}

type AppDriverConfig struct {
	InitFromScratch bool
}

func (ch *ClickhouseConfig) DSN() string {
	return fmt.Sprintf(clickhouseDsnFormat, ch.User, ch.Password, ch.Host, ch.Port, ch.Database)
}

func InitConfig() *Config {
	if os.Getenv("CLICKHOUSE_USER") == "" {
		if err := godotenv.Load("clickhouse.env"); err != nil {
			log.Fatal("Error loading clickhouse.env file")
		}
	}

	if os.Getenv("DAILY_TXS_BUCKET") == "" {
		if err := godotenv.Load("local.env"); err != nil {
			log.Fatal("Error loading local.env file")
		}
	}

	Cfg = &Config{
		GCSConfig: &GCSConfig{
			DailyTxsBucket:            os.Getenv("DAILY_TXS_BUCKET"),
			DailyCurrencyPricesBucket: os.Getenv("DAILY_CURRENCY_PRICES_BUCKET"),
			CurrencyRegistryBucket:    os.Getenv("CURRENCY_REGISTRY_BUCKET"),
			CurrencyRegistryFilename:  os.Getenv("CURRENCY_REGISTRY_FILENAME"),
			TrackedCurrenciesFilename: os.Getenv("TRACKED_CURRENCIES_FILENAME"),
		},
		ClickhouseConfig: &ClickhouseConfig{
			User:     os.Getenv("CLICKHOUSE_USER"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			Host:     os.Getenv("CLICKHOUSE_HOST"),
			Port:     os.Getenv("CLICKHOUSE_TCP_PORT"),
			Database: os.Getenv("CLICKHOUSE_DATABASE"),
		},
		AppDriverConfig: &AppDriverConfig{
			InitFromScratch: os.Getenv("INIT_FROM_SCRATCH") == "true",
		},
	}

	empJSON, _ := json.MarshalIndent(Cfg, "", "  ")
	fmt.Println(string(empJSON))

	// initialize CurrencyConfig after logging to prevent API Key leakage
	Cfg.CurrencyConfig = &CurrencyConfig{
		CoingeckoAPIKEY: helper.ToPtrStr(os.Getenv("COINGECKO_API_KEY")),
		LimitRateFlag:   os.Getenv("COINGECKO_LIMIT_RATE") == "true",
	}

	return Cfg
}
