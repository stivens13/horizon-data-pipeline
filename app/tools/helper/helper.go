package helper

import (
	"fmt"
	"github.com/stivens13/horizon-data-pipeline/app/tools/constants"
)

func CSVFileDate(date string) string {
	return fmt.Sprintf(constants.CSVFileMask, date)
}
