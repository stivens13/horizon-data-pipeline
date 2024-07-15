package main

import (
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	"log"
	"os"
	"path"
)

var (
	sampleDataFilename = "sample_data.csv"
	dataPath           = "data/"
	sampleDataFilepath = path.Join(dataPath, sampleDataFilename)
	dateKeyLayout      = "20060102"
)

func main() {
	txs, err := readData(sampleDataFilepath)
	if err != nil {
		log.Fatalf("failed to open sample data file: %v", err)
	}

	txsByDate := map[string][]*models.TransactionRaw{}

	for _, tx := range txs {
		fmt.Println(tx.Props.String())
		fmt.Println(tx.Nums.String())
		txDate := tx.Timestamp.Format(dateKeyLayout)
		if _, ok := txsByDate[txDate]; !ok {
			txsByDate[txDate] = []*models.TransactionRaw{}
		}

		txsByDate[txDate] = append(txsByDate[txDate], tx)
	}

	for key, txsPerDay := range txsByDate {
		newFilename := fmt.Sprintf("%s.csv", key)
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
