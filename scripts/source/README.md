This folder contains scripts to prepare data for testing sources.

## Kafka

`scripts/source/test_data` contains the data. Filename's convention is `<topic_name>.<n_partitions>`.

- If `<topic_name>` ends with `bin`, the whole file is a message with binary data.
- If `<topic_name>` ends with `avro_json` or `json_schema`:
    - The first line is the schema. Key and value are separated by `^`.
    - The rest of the lines are messages in JSON format. Key and value are separated by `^`.
    - Produced to Kafka with `schema_registry_producer.py` (serialized to Avro or JSON)
- Otherwise, each line is a message, and key/value is separated by `^`.
