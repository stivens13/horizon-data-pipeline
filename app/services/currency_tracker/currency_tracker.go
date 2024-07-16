package main

import (
	"context"
	usecase "github.com/stivens13/horizon-data-pipeline/app/services/currency_tracker/usecase"
	gcp_gateway "github.com/stivens13/horizon-data-pipeline/app/services/gcp-gateway/repository"
	"log"
)

//type CurrencyTracker struct {
//}

func main() {

	usecase := usecase.NewCurrencyInteractor(&gcp_gateway.GCStorageRepository{})

	ctx := context.Background()
	date := "2024-04-15"
	err := usecase.UpdateCurrencyRegistry()
	if err != nil {
		log.Fatalf("Error fetching coins: %v", err)
	}

	//symbols := createSymbolToAddressWithID(coins)
	//
	//clientsFile, err := os.OpenFile("currency_map.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	//if err != nil {
	//	panic(err)
	//}
	//defer clientsFile.Close()
	//if err := gocsv.MarshalFile(&symbols, clientsFile); err != nil {
	//	panic(err)
	//}

}
