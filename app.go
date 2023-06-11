// check source, drop records that arent matching this apps.
// Add config for id in destination

package main

import (
	"context"
	"log"

	// Dependencies of Turbine
	"github.com/meroxa/turbine-go/pkg/turbine"
	"github.com/meroxa/turbine-go/pkg/turbine/cmd"
)

func main() {
	cmd.Start(App{})
}

var _ turbine.App = (*App)(nil)

type App struct{}

func (a App) Run(v turbine.Turbine) error {

	source, err := v.Resources("west-store-mongo")
	if err != nil {
		return err
	}

	rr, err := source.RecordsWithContext(context.Background(), "medicine", turbine.ConnectionOptions{
		{Field: "transforms", Value: "unwrap"},
		{Field: "transforms.unwrap.type", Value: "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"},
	})
	if err != nil {
		return err
	}

	res, err := v.Process(rr, ProcessStoreData{})
	if err != nil {
		return err
	}

	dest, err := v.Resources("meroxa-atlas")
	if err != nil {
		return err
	}

	err = dest.WriteWithConfig(res, "aggregated_medicine", turbine.ConnectionOptions{
		{Field: "max.batch.size", Value: "1"},
		// {Field: "writemodel.strategy"},
	})
	if err != nil {
		return err
	}

	return nil
}

type ProcessStoreData struct{}

// func (f ProcessStoreData) Process(stream []turbine.Record) []turbine.Record {
// 	for i, record := range stream {
// 		// log.Printf("Processing record %d: %+v\n", i, record) // Logging the record details
// 		log.Printf("Payload: \n%s\n", record.Payload) // Logging the payload

// 		log.Printf("Setting StoreID for West Store to 001")
// 		err := record.Payload.Set("storeId", "001")
// 		if err != nil {
// 			log.Println("error setting value: ", err)
// 			continue
// 		}

// 		stream[i] = record
// 	}
// 	return stream
// }

func (f ProcessStoreData) Process(stream []turbine.Record) []turbine.Record {
	processedStream := make([]turbine.Record, 0)

	for _, record := range stream {
		// log.Printf("Processing record %d: %+v\n", i, record) // Logging the record details
		log.Printf("Payload: \n%s\n", record.Payload) // Logging the payload

		source := record.Payload.Get("source")
		if source == nil {
			log.Printf("Error getting source: %v", source)
			continue
		}

		if source == "west-store-mongo" {
			log.Printf("Setting storeId for West Store to 001")
			err := record.Payload.Set("storeId", "001")
			if err != nil {
				log.Println("error setting value: ", err)
				continue
			}

			processedStream = append(processedStream, record)
		} else {
			log.Printf("Dropping record with source: %s", source)
		}
	}

	return processedStream
}
