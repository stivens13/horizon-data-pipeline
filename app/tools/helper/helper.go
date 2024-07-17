package helper

import (
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/stivens13/horizon-data-pipeline/app/tools/constants"
	"os"
)

func CSVFileDate(date string) string {
	return fmt.Sprintf(constants.CSVFileMask, date)
}

func IsGenesisAddress(address string) bool {
	if address == constants.GenesisAddress {
		return true
	}
	return false
}

func UnmarshalCSVBytes[T any](bytes []byte) (out *T, err error) {
	out = new(T)
	if err := gocsv.UnmarshalBytes(bytes, out); err != nil {
		return out, err
	}
	return out, nil
}

func OpenCSVFile[T any](filename string) (out T, err error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return out, fmt.Errorf("failed to open seed data file %v: %w", filename, err)
	}
	defer file.Close()

	if err := gocsv.UnmarshalFile(file, &out); err != nil { // Load txs from file
		return out, fmt.Errorf("failed to unmarshall csv file %v: %w", filename, err)
	}

	return out, nil
}
