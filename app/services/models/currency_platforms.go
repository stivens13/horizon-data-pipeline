package models

import (
	"encoding/json"
	"fmt"
	"github.com/gocarina/gocsv"
)

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

func (p *Platforms) String() string {
	return fmt.Sprintf("%+v", p)
}

// UnmarshalCSV converts the CSV string as internal date
func (p *Platforms) UnmarshalCSV(csv string) (err error) {
	if err := json.Unmarshal([]byte(csv), p); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}
	return nil
}
