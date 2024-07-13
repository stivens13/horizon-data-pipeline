package config

import (
	"fmt"
	"os"
)

var clickhouseDsnFormat = "clickhouse://%s:%s@%s:%s/%s?dial_timeout=10s&read_timeout=20s"

//var Cfg *Config

type Config struct {
	GCPStorageClient *GCPStorageClient
	ClickhouseConfig *ClickhouseConfig
}

type GCPStorageClient struct {
}

type ClickhouseConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	Database string
}

func (ch *ClickhouseConfig) DSN() string {
	return fmt.Sprintf(clickhouseDsnFormat, ch.User, ch.Password, ch.Host, ch.Port, ch.Database)
}

func InitConfig() *Config {
	gcp := &GCPStorageClient{}

	return &Config{
		GCPStorageClient: gcp,
		ClickhouseConfig: &ClickhouseConfig{
			User:     os.Getenv("CLICKHOUSE_USER"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
			Host:     os.Getenv("CLICKHOUSE_HOST"),
			Port:     os.Getenv("CLICKHOUSE_PORT"),
			Database: os.Getenv("CLICKHOUSE_DATABASE"),
		},
	}
}
