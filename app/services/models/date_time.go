package models

import (
	"github.com/gocarina/gocsv"
	"github.com/stivens13/horizon-data-pipeline/app/tools/constants"
	"time"
)

type DateTime struct {
	time.Time
}

var _ gocsv.TypeMarshaller = new(DateTime)
var _ gocsv.TypeUnmarshaller = new(DateTime)

func (date *DateTime) MarshalCSV() (string, error) {
	return date.String(), nil
}

func (date *DateTime) String() string {
	return date.Time.Format(constants.TimestampTimeLayout)
}

// UnmarshalCSV converts the CSV string as internal date
func (date *DateTime) UnmarshalCSV(csv string) (err error) {
	date.Time, err = time.Parse(constants.TimestampTimeLayout, csv)
	return err
}
