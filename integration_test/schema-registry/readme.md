This demo shows how to ingest Avro data into RisingWave with [Schema Registry](https://github.com/confluentinc/schema-registry), which manages multiple versions of Avro schemas.

At the beginning, there's a datagen process that ingests Avro data into Redpanda (a Kafka-compatible message queue). The Avro schema is as follows:

- ** Version 1 **

    ```json
    {
        "name": "student",
        "type": "record",
        "fields": [
            {
                "name": "id",
                "type": "int",
                "default": 0
            },
            {
                "name": "name",
                "type": "string",
                "default": ""
            },
            {
                "name": "avg_score",
                "type": "double",
                "default": 0.0
            },
            {
                "name": "age",
                "type": "int",
                "default": 0
            },
            {
                "name": "schema_version",
                "type": "string",
                "default": ""
            }
        ]
    }
    ```


- ** Version 2 **

    ```json
    {
        "name": "student",
        "type": "record",
        "fields": [
            {
                "name": "id",
                "type": "int",
                "default": 0
            },
            {
                "name": "name",
                "type": "string",
                "default": ""
            },
            {
                "name": "avg_score",
                "type": "double",
                "default": 0.0
            },
            {
                "name": "age",
                "type": "int",
                "default": 0
            },
            {
                "name": "facebook_id",
                "type": "string",
                "default": ""
            },
            {
                "name": "schema_version",
                "type": "string",
                "default": ""
            }
        ]
    }
    ```

As shown above, there are two versions of the schema. The new version contains an additional field `facebook_id`. Hence, it is backward-compatible with the old version. The data will be generated randomly (50/50) using one of the versions.

Then, this demo will connect RisingWave to the message queue. Here we specify the address as `confluent schema registry 'http://message_queue:8081'`. The final CREATE SOURCE query is as follows:

```sql
CREATE SOURCE student WITH (
    connector = 'kafka',
    topic = 'sr-test',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
)
ROW FORMAT avro message 'student'
row schema location confluent schema registry 'http://message_queue:8081';
```
