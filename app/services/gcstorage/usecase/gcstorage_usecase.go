package usecase

import (
	"bytes"
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/stivens13/horizon-data-pipeline/app/config"
	repo "github.com/stivens13/horizon-data-pipeline/app/services/gcstorage/repository"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	"github.com/stivens13/horizon-data-pipeline/app/tools/helper"
)

type GCSInteractor struct {
	repo repo.StorageRepository
	c    *config.GCSConfig
}

func NewGCSInteractor(c *config.GCSConfig) *GCSInteractor {
	return &GCSInteractor{
		repo: repo.NewGCPStorage(c),
		c:    c,
	}
}

func (g *GCSInteractor) InitializeBuckets() error {
	if err := g.repo.CreateBucket(g.c.DailyTxsBucket); err != nil {
		return fmt.Errorf("failed to create bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	if err := g.repo.CreateBucket(g.c.CurrencyRegistryBucket); err != nil {
		return fmt.Errorf("failed to create bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	if err := g.repo.CreateBucket(g.c.DailyCurrencyPricesBucket); err != nil {
		return fmt.Errorf("failed to create bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	return nil
}

func (g *GCSInteractor) GetDailyTxs(date string) (txs []*models.TransactionRaw, err error) {
	var data []byte
	if data, err = g.repo.DownloadFileToBytes(g.c.DailyTxsBucket, helper.CSVFileDate(date)); err != nil {
		return txs, fmt.Errorf("failed to download daily txs: %w", err)
	}

	if err = gocsv.Unmarshal(bytes.NewReader(data), &txs); err != nil {
		return txs, fmt.Errorf("failed to unmarshal daily txs: %w", err)
	}

	return txs, nil
}

func (g *GCSInteractor) GetDailyPrices(date string) (
	prices models.DailyPrices,
	err error,
) {
	var data []byte
	if data, err = g.repo.DownloadFileToBytes(g.c.DailyCurrencyPricesBucket, helper.CSVFileDate(date)); err != nil {
		return nil, fmt.Errorf("failed to download daily prices, %w", err)
	}

	var currencyPrices []*models.CurrencyPrice
	if err := gocsv.UnmarshalBytes(data, &prices); err != nil {
		return nil, fmt.Errorf("failed to unmarshal daily prices, %w", err)
	}

	for _, currency := range currencyPrices {
		prices[currency.Symbol] = currency
	}

	return prices, nil
}

func (g *GCSInteractor) UpdateDailyPrices(data []byte) (err error) {
	return g.repo.UploadFileFromBytes(g.c.DailyCurrencyPricesBucket, g.c.DailyCurrencyPricesBucket, data)
}

func (g *GCSInteractor) GetTrackedCurrencies() (
	currencyRegistry []models.TrackedCurrency,
	err error,
) {
	var data []byte
	if data, err = g.repo.DownloadFileToBytes(g.c.CurrencyRegistryBucket, g.c.TrackedCurrenciesFilename); err != nil {
		return currencyRegistry, fmt.Errorf("failed to download currency registry, %w", err)
	}

	if err = gocsv.UnmarshalBytes(data, &currencyRegistry); err != nil {
		return currencyRegistry, fmt.Errorf("failed to unmarshal currency registry, %w", err)
	}

	return currencyRegistry, nil
}

func (g *GCSInteractor) UpdateTrackedCurrencies(data []byte) (err error) {
	if err = g.repo.UploadFileFromBytes(g.c.TrackedCurrenciesFilename, g.c.CurrencyRegistryBucket, data); err != nil {
		return fmt.Errorf("failed to update tracked currencies, %w", err)
	}

	return nil
}

func (g *GCSInteractor) GetCurrencyRegistry() (
	currencyRegistryMap models.RegistryMap,
	err error,
) {
	var currencyRegistryRaw []byte
	if currencyRegistryRaw, err = g.repo.DownloadFileToBytes(
		g.c.CurrencyRegistryBucket,
		g.c.CurrencyRegistryFilename,
	); err != nil {
		return currencyRegistryMap, fmt.Errorf("failed to download file: %s, %w", g.c.CurrencyRegistryFilename, err)
	}

	var currencyRegistry []models.Registry
	if err = gocsv.UnmarshalBytes(currencyRegistryRaw, &currencyRegistry); err != nil {
		return currencyRegistryMap, fmt.Errorf("failed to unmarshal file: %s, %w", g.c.CurrencyRegistryFilename, err)
	}

	for _, entry := range currencyRegistry {
		currencyRegistryMap[entry.Symbol] = entry.PlatformsWithIds
	}

	return currencyRegistryMap, nil
}

func (g *GCSInteractor) UpdateCurrencyRegistry(data []byte) error {
	return fmt.Errorf("not implemented")
}
