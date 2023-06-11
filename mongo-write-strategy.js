// Reference:
// https://www.mongodb.com/docs/kafka-connector/current/sink-connector/configuration-properties/
// https://www.mongodb.com/docs/kafka-connector/current/sink-connector/configuration-properties/write-strategies/
// https://www.mongodb.com/docs/kafka-connector/current/sink-connector/fundamentals/write-strategies/#replace-one-business-key-strategy

// Using `patient_id` as a field to create a unique business key along with `pillName`. 
// This would mean that each pill is associated with a specific patient, creating a unique combination for each record.

// Create a unique index in the MongoDB collection on the `pillName` and `patient_id` fields:
db.medicine.createIndex({ "pillName": 1, "patient_id": 1 }, { unique: true })

// PartialValueStrategy strategy configuration
document.id.strategy = com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy
document.id.strategy.partial.value.projection.list = pillName, patient_id
document.id.strategy.partial.value.projection.type = AllowList

// ReplaceOneBusinessKeyStrategy strategy configuration
writemodel.strategy = com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy

// Inserts
db.medicine.insert({pillName: 'Tylenol', quantityDispensed: 500, patient_id: 'patient_1', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Advil', quantityDispensed: 300, patient_id: 'patient_2', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Aspirin', quantityDispensed: 200, patient_id: 'patient_1', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Ibuprofen', quantityDispensed: 150, patient_id: 'patient_3', dispensedAt: new Date()});

// Updates
// Only update fields that are not used in PartialValueStrategy
db.medicine.updateOne(
    { _id: ObjectId("64854c03d27c1d0dc0e1144b") },
    { $set: { quantityDispensed: 9 } }
)
