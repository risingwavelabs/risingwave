CREATE TABLE users (_id JSONB PRIMARY KEY, payload JSONB) WITH (
    connector = 'mongodb-cdc',
    mongodb.url = 'mongodb://mongodb:27017/?replicaSet=rs0',
    collection.name = 'random_data.*'
);