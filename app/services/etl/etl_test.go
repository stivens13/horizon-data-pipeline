package etl

import (
	"github.com/stivens13/horizon-data-pipeline/app/services/gcp"
	"github.com/stivens13/horizon-data-pipeline/app/services/models"
	"github.com/stretchr/testify/suite"
	"testing"
)

type ETLTestSuite struct {
	suite.Suite
}

func (s *ETLTestSuite) SetupTest() {}

func (s *ETLTestSuite) TearDownTest() {}

func TestETLSuite(t *testing.T) {
	suite.Run(t, new(ETLTestSuite))
}

//func (s *ETLTestSuite) TestExtractTxs() {
//	type fields struct {
//		GCPStorage *gcp.GCPStorage
//	}
//	type args struct {
//		filename string
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			e := &ETL{
//				GCPStorage: tt.fields.GCPStorage,
//			}
//			if err := e.ExtractTxs(tt.args.filename); (err != nil) != tt.wantErr {
//				t.Errorf("ExtractTxs() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}

func (s *ETLTestSuite) TestReadData() {
	filepath := "data/sample_data.csv"
	type fields struct {
		GCPStorage *gcp.GCPStorage
		filepath   string
	}
	tests := map[string]struct {
		name     string
		filepath string
		fields   fields
		wantTxs  []*models.TransactionRaw
		wantErr  bool
	}{
		"sample data": {
			fields:  fields{GCPStorage: &gcp.GCPStorage{}, filepath: filepath},
			wantErr: false,
		},
	}
	for name, test := range tests {
		s.Run(name, func() {
			e := &ETL{
				GCPStorage: test.fields.GCPStorage,
			}
			gotTxs, err := e.ReadData(test.fields.filepath)
			if test.wantErr {
				s.Require().NoError(err)
				return
			}
			s.Require().NoError(err)
			s.Require().NotEmpty(gotTxs)
		})
	}
}

func (s *ETLTestSuite) TestTransformData() {
	filepath := "data/sample_data.csv"
	type fields struct {
		GCPStorage *gcp.GCPStorage
		filepath   string
	}
	tests := map[string]struct {
		name     string
		filepath string
		fields   fields
		wantTxs  []*models.TransactionRaw
		wantErr  bool
	}{
		"sample data": {
			fields:  fields{GCPStorage: &gcp.GCPStorage{}, filepath: filepath},
			wantErr: false,
		},
	}
	for name, test := range tests {
		s.Run(name, func() {
			e := &ETL{
				GCPStorage: test.fields.GCPStorage,
			}
			err := e.TransformTxs()
			if test.wantErr {
				s.Require().NoError(err)
				return
			}
			s.Require().NoError(err)
		})
	}
}
