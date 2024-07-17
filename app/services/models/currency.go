package models

import (
	"encoding/json"
	"fmt"
	"github.com/gocarina/gocsv"
)

// Currency represents a cryptocurrency with its ID, symbol, name, and platforms
type Currency struct {
	ID        string    `json:"id"`
	Symbol    string    `json:"symbol"`
	Name      string    `json:"name"`
	Platforms Platforms `json:"platforms"`
}

type HistoricalData struct {
	Prices [][]float64 `json:"prices"`
}

type Platforms map[string]string

var _ gocsv.TypeMarshaller = new(Platforms)
var _ gocsv.TypeUnmarshaller = new(Platforms)

func (p *Platforms) MarshalCSV() (string, error) {
	jsonData, err := json.Marshal(p)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}

// UnmarshalCSV converts the CSV string as internal date
func (p *Platforms) UnmarshalCSV(csv string) (err error) {
	if err := json.Unmarshal([]byte(csv), p); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return nil
}
