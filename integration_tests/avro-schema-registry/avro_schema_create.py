"""
A helper script for creating a Kafka Avro sink in the upsert mode.
The subject name for Kafka value is '{schema_name}.{table_name}'.
For Kafka key, it's '{schema_name}.{table_name}-key'.

This script will retrieve a RisingWave table's schema,
convert it to Avro schema, and register it to Confluent Schema Registry.

Please note that not all RisingWave types are supported due to the limitation
of Avro.
"""

from dataclasses import dataclass
import sys
import json
from confluent_kafka.schema_registry.schema_registry_client import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema
import psycopg2

# PostgreSQL Details
DB_NAME = "dev"
DB_USER = "root"
DB_PASSWORD = ""
DB_HOST = sys.argv[1]
DB_PORT = "4566"
PG_TABLE = "t"
PG_SCHEMA = "public"
PG_PRIMARY_KEYS = ""  # default to table's primary keys

# Schema Registry Details
SCHEMA_REGISTRY_URL = f"http://{sys.argv[2]}:8081"

AVRO_DATA_TYPES = {
    "boolean": "boolean",
    "smallint": "int",
    "integer": "int",
    "bigint": "long",
    "real": "float",
    "double precision": "double",
    "character varying": "string",
    "bytea": "bytes",
    "timestamp with time zone": {
        "type": "long",
        "logicalType": "timestamp-micros"
    },
    "timestamp without time zone": {
        "type": "long",
        "logicalType": "local-timestamp-micros",
    },
    "date": {"type": "int", "logicalType": "date"},
    "time without time zone": {"type": "long", "logicalType": "time-micros"},
}


@dataclass
class ColumnDef:
    """A RisingWave column"""
    name: str
    type: str
    is_primary_key: bool


class AvroSchema:
    schema_json: str
    subject_name: str

    def __init__(self, subject_name: str, record_name: str, fields: [ColumnDef]):
        self.subject_name = subject_name

        schema = {
            "type": "record",
            "name": record_name,
            "fields": []
        }
        for field in fields:
            avro_data_type = AVRO_DATA_TYPES.get(field.type)
            if avro_data_type is None:
                err = f"Unsupported PostgreSQL data type '{field.type}' for"
                f"column '{field.name}'."
                raise Exception(err)
            schema["fields"].append(
                {"name": field.name, "type": ["null", avro_data_type]}
            )
        self.schema_json = json.dumps(schema)

    def register(self, client: SchemaRegistryClient):
        """Register the schema to Schema Registry"""
        print(f"Registering schema {self.subject_name}")
        print(self.schema_json)

        schema_id = client.register_schema(
            self.subject_name, Schema(self.schema_json, "AVRO"))
        print("Schema registered successfully with ID:", schema_id)


@dataclass
class KeyValueSchema:
    key: AvroSchema
    value: AvroSchema

    def __init__(self, columns: [ColumnDef]):
        """Convert the RisignWave table schema to Avro schema"""

        pk_columns = [col for col in columns if col.is_primary_key]
        self.key = AvroSchema(
            f"{PG_SCHEMA}.{PG_TABLE}-key",
            f"{DB_NAME}.{PG_SCHEMA}.{PG_TABLE}.Key",
            pk_columns)
        self.value = AvroSchema(
            f"{PG_SCHEMA}.{PG_TABLE}-value",
            f"{DB_NAME}.{PG_SCHEMA}.{PG_TABLE}.Value",
            columns)

    def register(self):
        """Register the schema to Schema Registry"""

        # Create a Kafka Producer client
        client = SchemaRegistryClient({
            "url": SCHEMA_REGISTRY_URL
        })
        self.key.register(client)
        self.value.register(client)


def get_primary_keys(table_name, conn):
    """Retrieves the primary keys of a table.
    Returns:
        tuple: A tuple containing the primary key column names, or None
               if no primary key is found.
    """

    with conn.cursor() as cursor:
        cursor.execute(f"DESCRIBE {table_name}")
        rows = cursor.fetchall()

        primary_key_cols = None
        for row in rows:
            if row[0] == 'primary key':
                # Split composite keys
                primary_key_cols = tuple(row[1].split(", "))
                break

    return primary_key_cols


def fetch_postgres_schema() -> [ColumnDef]:
    """Fetch a table's columns, with primary keys unset."""
    pg_schema = []
    with psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
    ) as conn:
        with conn.cursor() as cursor:

            # Fetch PostgreSQL schema information for a specific table
            cursor.execute(
                f"""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{PG_SCHEMA}' AND table_name = '{PG_TABLE}';
            """
            )

            pg_schema = [ColumnDef(column_name, data_type, False)
                         for _, column_name, data_type in cursor.fetchall()]

        if PG_PRIMARY_KEYS.strip() == "":
            primary_keys = get_primary_keys(table_name=PG_TABLE, conn=conn)
        else:
            primary_keys = [pk.strip() for pk in PG_PRIMARY_KEYS.split(',')]

        for column in pg_schema:
            if column.name in primary_keys:
                column.is_primary_key = True

    return pg_schema


if __name__ == "__main__":
    postgres_schema = fetch_postgres_schema()

    if postgres_schema:
        KeyValueSchema(postgres_schema).register()
    else:
        print(f"No schema found for table '{PG_SCHEMA}.{PG_TABLE}'.")
