package main

import (
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	"github.com/stivens13/horizon-data-pipeline/app/tools/constants"
	"github.com/stivens13/horizon-data-pipeline/app/tools/helper"
	"log"
	"os"
	"path"
)

var (
	sampleDataFilename = "seed_data.csv"
	dataPath           = "data/"
	sampleDataFilepath = path.Join(dataPath, sampleDataFilename)
)

func main() {
	txs, err := readData(sampleDataFilepath)
	if err != nil {
		log.Fatalf("failed to open sample data file: %v", err)
	}

	txsByDate := map[string][]*models.TransactionRaw{}
	txsByAddress := map[string]map[string]bool{}

	for _, tx := range txs {
		txDate := tx.Timestamp.Format(constants.DateKeyLayout)
		if _, ok := txsByDate[txDate]; !ok {
			txsByDate[txDate] = []*models.TransactionRaw{}
		}
		txsByDate[txDate] = append(txsByDate[txDate], tx)

		if _, ok := txsByAddress[tx.Props.CurrencyAddress]; !ok {
			txsByAddress[tx.Props.CurrencyAddress] = map[string]bool{}
		}
		txsByAddress[tx.Props.CurrencyAddress][tx.Props.CurrencySymbol] = true
	}

	for key, txsPerDay := range txsByDate {
		newFilename := helper.CSVFileDate(key)
		newFilepath := path.Join(dataPath, newFilename)
		newTxsPerDayFile, err := os.OpenFile(newFilepath, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			log.Fatalf("failed to create new file: %v", err)
		}
		if err := gocsv.MarshalFile(&txsPerDay, newTxsPerDayFile); err != nil {
			log.Fatalf("failed to write new file: %v", err)
		}
	}
}

func readData(filepath string) (txs []*models.TransactionRaw, err error) {
	txsFile, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return txs, fmt.Errorf("failed to open data file %v: %w", filepath, err)
	}
	defer txsFile.Close()

	if err := gocsv.UnmarshalFile(txsFile, &txs); err != nil { // Load txs from file
		return txs, fmt.Errorf("failed to unmarshall csv file %v: %w", filepath, err)
	}

	return txs, nil
}
