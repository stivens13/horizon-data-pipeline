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
	Repo repo.StorageRepository
	c    *config.GCSConfig
}

func NewGCSInteractor(c *config.GCSConfig) *GCSInteractor {
	return &GCSInteractor{
		Repo: repo.NewGCSRepo(c),
		c:    c,
	}
}

func (g *GCSInteractor) InitializeBuckets() error {
	if err := g.Repo.CreateBucket(g.c.DailyTxsBucket); err != nil {
		return fmt.Errorf("failed to create bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	if err := g.Repo.CreateBucket(g.c.CurrencyRegistryBucket); err != nil {
		return fmt.Errorf("failed to create bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	if err := g.Repo.CreateBucket(g.c.DailyCurrencyPricesBucket); err != nil {
		return fmt.Errorf("failed to create bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	return nil
}

func (g *GCSInteractor) DestroyAllBuckets() error {
	if err := g.Repo.DeleteBucket(g.c.DailyTxsBucket); err != nil {
		return fmt.Errorf("failed to delete bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	if err := g.Repo.DeleteBucket(g.c.CurrencyRegistryBucket); err != nil {
		return fmt.Errorf("failed to delete bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	if err := g.Repo.DeleteBucket(g.c.DailyCurrencyPricesBucket); err != nil {
		return fmt.Errorf("failed to delete bucket: %s, %w", g.c.DailyTxsBucket, err)
	}

	return nil
}

func (g *GCSInteractor) GetDailyTxs(date string) (txsRaw models.TransactionsRawView, err error) {
	var data []byte
	filename := helper.CSVFileDate(date)
	if data, err = g.Repo.DownloadFileToBytes(g.c.DailyTxsBucket, filename); err != nil {
		return txsRaw, fmt.Errorf("failed to download daily txsRaw: %w", err)
	}

	if err = gocsv.Unmarshal(bytes.NewReader(data), &txsRaw.Data); err != nil {
		return txsRaw, fmt.Errorf("failed to unmarshal daily txsRaw: %w", err)
	}

	return txsRaw, nil
}

func (g *GCSInteractor) UploadDailyTxs(date string, data []byte) (err error) {
	filename := helper.CSVFileDate(date)
	if err = g.Repo.UploadFileFromBytes(g.c.DailyTxsBucket, filename, data); err != nil {
		return fmt.Errorf("failed to upload daily txs: %w", err)
	}

	return nil
}

func (g *GCSInteractor) FetchDailyPrices(date string) (
	prices models.DailyPrices,
	err error,
) {
	prices = models.DailyPrices{}
	var data []byte
	if data, err = g.Repo.DownloadFileToBytes(g.c.DailyCurrencyPricesBucket, helper.CSVFileDate(date)); err != nil {
		return nil, fmt.Errorf("failed to download daily prices, %w", err)
	}

	var currencyPrices []*models.CurrencyPrice
	if err := gocsv.UnmarshalBytes(data, &currencyPrices); err != nil {
		return nil, fmt.Errorf("failed to unmarshal daily prices, %w", err)
	}

	for _, currency := range currencyPrices {
		prices[currency.Symbol] = currency
	}

	return prices, nil
}

func (g *GCSInteractor) UpdateDailyPrices(date string, data []byte) (err error) {
	filename := helper.CSVFileDate(date)
	return g.Repo.UploadFileFromBytes(g.c.DailyCurrencyPricesBucket, filename, data)
}

func (g *GCSInteractor) GetTrackedCurrencies() (
	data []byte,
	err error,
) {
	if data, err = g.Repo.DownloadFileToBytes(g.c.CurrencyRegistryBucket, g.c.TrackedCurrenciesFilename); err != nil {
		return data, fmt.Errorf("failed to download currency registry, %w", err)
	}

	return data, nil
}

func (g *GCSInteractor) UploadTrackedCurrencies(data []byte) (err error) {
	if err = g.Repo.UploadFileFromBytes(g.c.CurrencyRegistryBucket, g.c.TrackedCurrenciesFilename, data); err != nil {
		return fmt.Errorf("failed to update tracked currencies, %w", err)
	}

	return nil
}

func (g *GCSInteractor) GetCurrencyRegistry() (
	data []byte,
	err error,
) {
	if data, err = g.Repo.DownloadFileToBytes(
		g.c.CurrencyRegistryBucket,
		g.c.CurrencyRegistryFilename,
	); err != nil {
		return data, fmt.Errorf("failed to download file: %s, %w", g.c.CurrencyRegistryFilename, err)
	}

	return data, nil
}

func (g *GCSInteractor) UpdateCurrencyRegistry(data []byte) error {
	if err := g.Repo.UploadFileFromBytes(
		g.c.CurrencyRegistryBucket,
		g.c.CurrencyRegistryFilename,
		data,
	); err != nil {
		return fmt.Errorf("failed to update currency registry, %w", err)
	}
	return nil
}
