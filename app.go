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

	dest, err := v.Resources("atlas-mongo")
	if err != nil {
		return err
	}

	// Using pillName & patient_id as unique identifier
	// err = dest.WriteWithConfig(res, "medicine", turbine.ConnectionOptions{
	// 	{Field: "max.batch.size", Value: "1"},
	// 	{Field: "document.id.strategy", Value: "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy"},
	// 	{Field: "document.id.strategy.partial.value.projection.list", Value: "pillName,patient_id"},
	// 	{Field: "document.id.strategy.partial.value.projection.type", Value: "AllowList"},
	// 	{Field: "writemodel.strategy", Value: "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"},
	// })

	// ReplaceOneBusinessKeyStrategy will keep overriting records since it cant match on Business Key ("after.patient_id")
	// err = dest.WriteWithConfig(res, "aggregated", turbine.ConnectionOptions{
	// 	{Field: "document.id.strategy", Value: "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy"},
	// 	{Field: "document.id.strategy.partial.value.projection.list", Value: "after.patient_id"},
	// 	{Field: "document.id.strategy.partial.value.projection.type", Value: "AllowList"},
	// 	{Field: "writemodel.strategy", Value: "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"},
	// })

	// ReplaceOneBusinessKeyStrategy will keep overriting records since it cant match on Business Key (`${after.patient_id}`)
	// err = dest.WriteWithConfig(res, "aggregated", turbine.ConnectionOptions{
	// 	{Field: "document.id.strategy", Value: "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy"},
	// 	{Field: "document.id.strategy.partial.value.projection.list", Value: `${after.patient_id}`},
	// 	{Field: "document.id.strategy.partial.value.projection.type", Value: "AllowList"},
	// 	{Field: "writemodel.strategy", Value: "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"},
	// })

	// ReplaceOneBusinessKeyStrategy will keep overriting records since it cant match on Business Key ("${after.patient_id}")
	// err = dest.WriteWithConfig(res, "aggregated", turbine.ConnectionOptions{
	// 	{Field: "document.id.strategy", Value: "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy"},
	// 	{Field: "document.id.strategy.partial.value.projection.list", Value: "${after.patient_id}"},
	// 	{Field: "document.id.strategy.partial.value.projection.type", Value: "AllowList"},
	// 	{Field: "writemodel.strategy", Value: "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"},
	// })

	// RESOURCES:
	// https://stackoverflow.com/questions/59083430/kafka-mongodb-sink-connector-update-document
	// https://github.com/hpgrahsl/kafka-connect-mongodb#use-case-1-employing-business-keys

	// Using mongo object id as unique identifier
	err = dest.WriteWithConfig(res, "aggregated_medicine", turbine.ConnectionOptions{
		{Field: "max.batch.size", Value: "1"},
		{Field: "document.id.strategy", Value: "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy"},
		{Field: "document.id.strategy.partial.value.projection.list", Value: "after._id.$oid"}, // Tried ${after._id.$oid} after._id
		{Field: "document.id.strategy.partial.value.projection.type", Value: "AllowList"},
		{Field: "writemodel.strategy", Value: "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy"},
	})
	if err != nil {
		return err
	}

	return nil
}

type ProcessStoreData struct{}

func (f ProcessStoreData) Process(stream []turbine.Record) []turbine.Record {
	processedStream := make([]turbine.Record, 0)

	for _, record := range stream {
		// log.Printf("Processing record %d: %+v\n", i, record) // Logging the record details
		log.Printf("Payload: \n%s\n", record.Payload) // Logging the payload

		source := record.Payload.Get("source")
		log.Printf("source value: %v", source)
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
