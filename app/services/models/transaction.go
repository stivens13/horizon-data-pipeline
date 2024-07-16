package models

import (
	"encoding/json"
	"fmt"
	"github.com/gocarina/gocsv"
	"strconv"
)

// Transaction represents minimum required data about a transaction
type Transaction struct {
	Timestamp       DateTime `json:"ts"`
	Event           string   `json:"event"`
	ProjectID       string   `json:"project_id"`
	CurrencySymbol  string   `json:"currency_symbol"`
	CurrencyAddress string   `json:"currency_address"`
	CurrencyValue   float64  `json:"currency_value"`
}

// TransactionRaw represents a single raw transaction data state stored on GCP GCS
type TransactionRaw struct {
	App              string   `json:"app" csv:"app"`
	Timestamp        DateTime `json:"ts" csv:"ts"`
	Event            string   `json:"event" csv:"event"`
	ProjectID        string   `json:"project_id" csv:"project_id"`
	Source           string   `json:"source" csv:"source"`
	Ident            int      `json:"ident" csv:"ident"`
	UserID           string   `json:"user_id" csv:"user_id"`
	SessionID        string   `json:"session_id" csv:"session_id"`
	Country          string   `json:"country" csv:"country"`
	DeviceType       string   `json:"device_type" csv:"device_type"`
	DeviceOS         string   `json:"device_os" csv:"device_os"`
	DeviceOSVersion  string   `json:"device_os_ver" csv:"device_os_ver"`
	DeviceBrowser    string   `json:"device_browser" csv:"device_browser"`
	DeviceBrowserVer string   `json:"device_browser_ver" csv:"device_browser_ver"`
	Props            Props    `json:"props" csv:"props"`
	Nums             Nums     `json:"nums" csv:"nums"`
}

// ToTransaction converts raw transaction into thin transaction with minimal required fields
func (tr *TransactionRaw) ToTransaction() *Transaction {
	currencyValue, err := strconv.ParseFloat(tr.Nums.CurrencyValueDecimal, 64)
	if err != nil {
		fmt.Printf("failed to convert currency value to float: %w", err)
	}
	return &Transaction{
		Timestamp:       tr.Timestamp,
		Event:           tr.Event,
		ProjectID:       tr.ProjectID,
		CurrencySymbol:  tr.Props.CurrencySymbol,
		CurrencyAddress: tr.Props.CurrencyAddress,
		CurrencyValue:   currencyValue,
	}
}

// Props represents transaction metadata, including CurrencySymbol
type Props struct {
	TokenID         string `json:"tokenId" csv:"token_id"`
	TxnHash         string `json:"txnHash" csv:"txn_hash"`
	CurrencyAddress string `json:"currencyAddress" csv:"currency_address"`
	MarketplaceType string `json:"marketplaceType" csv:"marketplace_type"`
	RequestID       string `json:"requestId" csv:"request_id"`
	CurrencySymbol  string `json:"currencySymbol" csv:"currency_symbol"`
	AdditionalProps string `json:"additionalProps" csv:"additional_props"`
}

var _ gocsv.TypeMarshaller = new(Props)
var _ gocsv.TypeUnmarshaller = new(Props)

func (p *Props) MarshalCSV() (string, error) {
	return p.String(), nil
}

func (p *Props) String() string {
	b, err := json.Marshal(p)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return ""
	}
	return string(b)
	//return fmt.Sprintf("%+v\n", p)
}

// UnmarshalCSV converts the CSV string as internal date
func (p *Props) UnmarshalCSV(csv string) (err error) {
	if err := json.Unmarshal([]byte(csv), p); err != nil {
		return fmt.Errorf("could not unmarshal props; %w", err)
	}
	return nil
}

// Nums represents transaction numerical value
// Note: tx value is in original currency nominal
type Nums struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal" csv:"currency_value_decimal"`
	CurrencyValueRaw     string `json:"currencyValueRaw" csv:"currency_value_raw"`
	AdditionalNums       string `json:"additionalNums" csv:"additional_nums"`
}

var _ gocsv.TypeMarshaller = new(Nums)
var _ gocsv.TypeUnmarshaller = new(Nums)

func (n *Nums) MarshalCSV() (string, error) {
	return n.String(), nil
}

func (n *Nums) String() string {
	b, err := json.Marshal(n)
	if err != nil {
		fmt.Printf("Error: %s", err)
		return ""
	}
	return string(b)
}

// UnmarshalCSV converts the CSV string as internal date
func (n *Nums) UnmarshalCSV(csv string) (err error) {
	if err := json.Unmarshal([]byte(csv), n); err != nil {
		return fmt.Errorf("could not unmarshal nums; %w", err)
	}
	return nil
}
