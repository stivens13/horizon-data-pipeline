package currency_tracker

import (
	"fmt"
	"github.com/stivens13/horizon-data-pipeline/models"
	"github.com/stretchr/testify/suite"
	"testing"
)

type CurrencyTrackerSuite struct {
	suite.Suite
}

func (s *CurrencyTrackerSuite) SetupTest() {}

func (s *CurrencyTrackerSuite) TearDownTest() {}

func TestCurrencyTrackerSuite(t *testing.T) {
	suite.Run(t, new(CurrencyTrackerSuite))
}

func (s *CurrencyTrackerSuite) TestFetchCoins() {
	type fields struct{}
	tests := map[string]struct {
		name     string
		filepath string
		fields   fields
		wantTxs  []*models.TransactionRaw
		wantErr  bool
	}{
		"fetch coins": {
			wantErr: false,
		},
	}
	for name, test := range tests {
		s.Run(name, func() {
			coins, err := FetchCoins()
			if test.wantErr {
				s.Require().NoError(err)
				return
			}
			s.Require().NoError(err)
			s.Require().NotEmpty(coins)
			//for _, coin := range coins {
			//	empJSON, _ := json.MarshalIndent(coin, "", "  ")
			//	fmt.Println(string(empJSON))
			//}
			for k, v := range SymbolToID {
				fmt.Printf("Symbol: %s, ids: %v\n", k, v)
			}

		})
	}
}
