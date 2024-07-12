package models

import (
	"github.com/gocarina/gocsv"
	"time"
)

const dateTimeLayout = "2006-01-02 15:04:05.000"

type DateTime struct {
	time.Time
}

var _ gocsv.TypeMarshaller = new(DateTime)
var _ gocsv.TypeUnmarshaller = new(DateTime)

func (date *DateTime) MarshalCSV() (string, error) {
	return date.String(), nil
}

func (date *DateTime) String() string {
	return date.Time.Format(dateTimeLayout)
}

// UnmarshalCSV converts the CSV string as internal date
func (date *DateTime) UnmarshalCSV(csv string) (err error) {
	date.Time, err = time.Parse(dateTimeLayout, csv)
	return err
}
