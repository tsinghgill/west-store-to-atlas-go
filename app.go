package main

import (
	// Dependencies of the example data app

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

	rr, err := source.Records("medicine", nil)
	if err != nil {
		return err
	}

	res, err := v.Process(rr, Anonymize{})
	if err != nil {
		return err
	}

	dest, err := v.Resources("mongo-atlas")
	if err != nil {
		return err
	}

	err = dest.WriteWithConfig(res, "dispensedpillsall", []turbine.ConnectionOption{
		turbine.ConnectionOption{Field: "transforms", Value: "unwrap"},
		turbine.ConnectionOption{Field: "transforms.unwrap.type", Value: "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"},
	})
	if err != nil {
		return err
	}

	return nil
}

type Anonymize struct{}

func (f Anonymize) Process(stream []turbine.Record) []turbine.Record {
	for i, record := range stream {
		log.Printf("Processing record %d: %+v\n", i, record) // Logging the record details
		log.Printf("Payload: \n%s\n", record.Payload)        // Logging the payload

		log.Printf("Setting StoreID")
		err := record.Payload.Set("after.storeId", "001")
		if err != nil {
			log.Println("error setting value: ", err)
			continue
		}

		stream[i] = record
	}
	return stream
}