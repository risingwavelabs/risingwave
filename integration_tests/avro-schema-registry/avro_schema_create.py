import psycopg2
import sys
import json
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema

# PostgreSQL Connection Details
DB_NAME = "dev"
DB_USER = "root"
DB_PASSWORD = ""
DB_HOST = sys.argv[1]
DB_PORT = "4566"
PG_TABLE = "t"
PG_SCHEMA = "public"

# Schema Registry Details
SCHEMA_REGISTRY_URL = f"http://{sys.argv[2]}:8081"


def fetch_postgres_schema():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    cursor = conn.cursor()

    # Fetch PostgreSQL schema information for a specific table
    cursor.execute(
        f"""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{PG_SCHEMA}' AND table_name = '{PG_TABLE}';
    """
    )

    postgres_schema = []
    for _, column_name, data_type in cursor.fetchall():
        postgres_schema.append({"name": column_name, "type": data_type})

    conn.close()
    return postgres_schema


def generate_avro_schema(postgres_schema):
    # Convert PostgreSQL data types to Avro data types
    avro_data_types = {
        "boolean": "boolean",
        "smallint": "int",
        "integer": "int",
        "bigint": "long",
        "real": "float",
        "double precision": "double",
        "character varying": "string",
        "bytea": "bytes",
        "timestamp with time zone": {"type": "long", "logicalType": "timestamp-micros"},
        "timestamp without time zone": {
            "type": "long",
            "logicalType": "local-timestamp-micros",
        },
        "date": {"type": "int", "logicalType": "date"},
        "time without time zone": {"type": "long", "logicalType": "time-micros"},
        "interval": {"type": "fixed", "size": 12, "logicalType": "duration"},
    }

    avro_schema = {
        "type": "record",
        "name": "{}.{}".format(PG_SCHEMA, PG_TABLE),
        "fields": [],
    }

    for column in postgres_schema:
        avro_data_type = avro_data_types.get(column["type"])
        if avro_data_type is None:
            raise Exception("Unsupported PostgreSQL data type '{}' for column '{}'.".format(
                column['type'], column['name']))
        avro_schema["fields"].append(
            {"name": column["name"], "type": ["null", avro_data_type]}
        )

    return avro_schema


def register_avro_schema(avro_schema, subject_name):
    avro_schema = Schema(json.dumps(avro_schema), "AVRO")

    # Create a Kafka Producer client
    client = SchemaRegistryClient({
        "url": SCHEMA_REGISTRY_URL
    })

    # Register the schema
    schema_id = client.register_schema(subject_name, avro_schema)

    print("Schema registered successfully with ID:", schema_id)


if __name__ == "__main__":
    postgres_schema = fetch_postgres_schema()

    if postgres_schema:
        avro_schema = generate_avro_schema(postgres_schema)
        subject_name = f"{PG_SCHEMA}-{PG_TABLE}"
        register_avro_schema(avro_schema, subject_name)
    else:
        print(f"No schema found for table '{PG_SCHEMA}.{PG_TABLE}'.")
