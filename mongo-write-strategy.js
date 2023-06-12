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
db.medicine.insert({pillName: 'Amoxicillin', quantityDispensed: 250, patient_id: 'patient_4', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Penicillin', quantityDispensed: 300, patient_id: 'patient_5', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Lipitor', quantityDispensed: 200, patient_id: 'patient_6', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Zithromax', quantityDispensed: 100, patient_id: 'patient_7', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Nexium', quantityDispensed: 500, patient_id: 'patient_8', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Plavix', quantityDispensed: 300, patient_id: 'patient_9', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Singulair', quantityDispensed: 250, patient_id: 'patient_10', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Prednisone', quantityDispensed: 400, patient_id: 'patient_11', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Lyrica', quantityDispensed: 350, patient_id: 'patient_12', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Prilosec', quantityDispensed: 150, patient_id: 'patient_13', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Zocor', quantityDispensed: 100, patient_id: 'patient_14', dispensedAt: new Date()});
db.medicine.insert({pillName: 'Prozac', quantityDispensed: 200, patient_id: 'patient_15', dispensedAt: new Date()});

// Updates
// Only update fields that are not used in PartialValueStrategy
db.medicine.updateOne(
    { _id: ObjectId("64866ce4dde0c60b222dfed5") },
    { $set: { quantityDispensed: 1 } }
)

db.medicine.updateOne(
    { patient_id: 5 },
    { $set: { quantityDispensed: 10000 } }
)



db.medicine.updateOne(
    { _id: ObjectId("64862353dde0c60b222dfe74") },
    { $set: { patient_id: 'patient_99999' } }
)

db.medicine.updateOne(
    { _id: ObjectId("6486415edde0c60b222dfec3") },
    { $set: { quantityDispensed: 10000 } },
    { upsert: true }
)


db.medicine.drop()

db.collection.createIndex({ "patient_id": 1, "order_id": 1}, { unique: true })
db.medicine.getIndexes()

db.medicine.find()

db.medicine.insert({pillName: 'Amoxicillin', quantityDispensed: 250, patient_id: "1", order_id: "1", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Penicillin', quantityDispensed: 300, patient_id: "5", order_id: "5", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Lipitor', quantityDispensed: 200, patient_id: "6", order_id: "6", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Zithromax', quantityDispensed: 100, patient_id: "7", order_id: "7", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Nexium', quantityDispensed: 500, patient_id: "8", order_id: "8", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Plavix', quantityDispensed: 300, patient_id: "9", order_id: "9", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Singulair', quantityDispensed: 250, patient_id: "10", order_id: "10", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Prednisone', quantityDispensed: 400, patient_id: "11", order_id: "11", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Lyrica', quantityDispensed: 350, patient_id: "12", order_id: "12", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Prilosec', quantityDispensed: 150, patient_id: "13", order_id: "13", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Zocor', quantityDispensed: 100, patient_id: "14", order_id: "14", dispensedAt: new Date()});
db.medicine.insert({pillName: 'Prozac', quantityDispensed: 200, patient_id: "15", order_id: "15", dispensedAt: new Date()});
db.medicine.insert({pillName: 'TEST', quantityDispensed: 200, patient_id: "22", order_id: "22", dispensedAt: new Date()});
