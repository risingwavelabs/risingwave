// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_connector::source::{ADBC_SNOWFLAKE_CONNECTOR, BATCH_POSIX_FS_CONNECTOR};

use super::*;

pub static SOURCE_ALLOWED_CONNECTION_CONNECTOR: LazyLock<HashSet<PbConnectionType>> =
    LazyLock::new(|| {
        hashset! {
            PbConnectionType::Unspecified,
            PbConnectionType::Kafka,
            PbConnectionType::Iceberg,
        }
    });

pub static SOURCE_ALLOWED_CONNECTION_SCHEMA_REGISTRY: LazyLock<HashSet<PbConnectionType>> =
    LazyLock::new(|| {
        hashset! {
            PbConnectionType::Unspecified,
            PbConnectionType::SchemaRegistry,
        }
    });

// TODO: Better design if we want to support ENCODE KEY where we will have 4 dimensional array
static CONNECTORS_COMPATIBLE_FORMATS: LazyLock<HashMap<String, HashMap<Format, Vec<Encode>>>> =
    LazyLock::new(|| {
        convert_args!(hashmap!(
                KAFKA_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes, Encode::Csv],
                    Format::Upsert => vec![Encode::Json, Encode::Avro, Encode::Protobuf],
                    Format::Debezium => vec![Encode::Json, Encode::Avro],
                    Format::Maxwell => vec![Encode::Json],
                    Format::Canal => vec![Encode::Json],
                    Format::DebeziumMongo => vec![Encode::Json],
                ),
                PULSAR_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                    Format::Maxwell => vec![Encode::Json],
                    Format::Canal => vec![Encode::Json],
                ),
                KINESIS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes, Encode::Csv],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                    Format::Maxwell => vec![Encode::Json],
                    Format::Canal => vec![Encode::Json],
                ),
                GOOGLE_PUBSUB_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Debezium => vec![Encode::Json],
                    Format::Maxwell => vec![Encode::Json],
                    Format::Canal => vec![Encode::Json],
                ),
                NEXMARK_CONNECTOR => hashmap!(
                    Format::Native => vec![Encode::Native],
                    Format::Plain => vec![Encode::Bytes],
                ),
                DATAGEN_CONNECTOR => hashmap!(
                    Format::Native => vec![Encode::Native],
                    Format::Plain => vec![Encode::Bytes, Encode::Json],
                ),
                OPENDAL_S3_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json, Encode::Parquet],
                ),
                GCS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json, Encode::Parquet],
                ),
                AZBLOB_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json, Encode::Parquet],
                ),
                POSIX_FS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv, Encode::Json, Encode::Parquet],
                ),
                BATCH_POSIX_FS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Csv],
                ),
                MYSQL_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
                POSTGRES_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
                CITUS_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                ),
                MONGODB_CDC_CONNECTOR => hashmap!(
                    Format::DebeziumMongo => vec![Encode::Json],
                ),
                NATS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Bytes],
                ),
                MQTT_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Bytes],
                ),
                TEST_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json],
                ),
                ICEBERG_CONNECTOR => hashmap!(
                    Format::None => vec![Encode::None],
                ),
                ADBC_SNOWFLAKE_CONNECTOR => hashmap!(
                    Format::None => vec![Encode::None],
                ),
                SQL_SERVER_CDC_CONNECTOR => hashmap!(
                    Format::Debezium => vec![Encode::Json],
                    // support source stream job
                    Format::Plain => vec![Encode::Json],
                ),
        ))
    });

fn validate_license(connector: &str) -> Result<()> {
    if connector == SQL_SERVER_CDC_CONNECTOR {
        Feature::SqlServerCdcSource.check_available()?;
    }
    Ok(())
}

pub fn validate_compatibility(
    format_encode: &FormatEncodeOptions,
    props: &mut BTreeMap<String, String>,
) -> Result<()> {
    let mut connector = props
        .get_connector()
        .ok_or_else(|| RwError::from(ProtocolError("missing field 'connector'".to_owned())))?;

    if connector == OPENDAL_S3_CONNECTOR {
        // reject s3_v2 creation
        return Err(RwError::from(Deprecated(
            OPENDAL_S3_CONNECTOR.to_owned(),
            LEGACY_S3_CONNECTOR.to_owned(),
        )));
    }
    if connector == LEGACY_S3_CONNECTOR {
        // S3 connector is deprecated, use OPENDAL_S3_CONNECTOR instead
        // do s3 -> s3_v2 migration
        let entry = props.get_mut(UPSTREAM_SOURCE_KEY).unwrap();
        *entry = OPENDAL_S3_CONNECTOR.to_owned();
        connector = OPENDAL_S3_CONNECTOR.to_owned();
    }

    let compatible_formats = CONNECTORS_COMPATIBLE_FORMATS
        .get(&connector)
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "connector {:?} is not supported, accept {:?}",
                connector,
                CONNECTORS_COMPATIBLE_FORMATS.keys()
            )))
        })?;

    validate_license(&connector)?;
    if connector != KAFKA_CONNECTOR {
        let res = match (&format_encode.format, &format_encode.row_encode) {
            (Format::Plain, Encode::Protobuf) | (Format::Plain, Encode::Avro) => {
                let mut options = WithOptions::try_from(format_encode.row_options())?;
                let (_, use_schema_registry) = get_schema_location(options.inner_mut())?;
                use_schema_registry
            }
            (Format::Debezium, Encode::Avro) => true,
            (_, _) => false,
        };
        if res {
            return Err(RwError::from(ProtocolError(format!(
                "The {} must be kafka when schema registry is used",
                UPSTREAM_SOURCE_KEY
            ))));
        }
    }

    let compatible_encodes = compatible_formats
        .get(&format_encode.format)
        .ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "connector {} does not support format {:?}",
                connector, format_encode.format
            )))
        })?;
    if !compatible_encodes.contains(&format_encode.row_encode) {
        return Err(RwError::from(ProtocolError(format!(
            "connector {} does not support format {:?} with encode {:?}",
            connector, format_encode.format, format_encode.row_encode
        ))));
    }

    if connector == POSTGRES_CDC_CONNECTOR || connector == CITUS_CDC_CONNECTOR {
        // Replication slot is now auto-managed by RisingWave.
        // Users cannot specify slot.name manually to ensure proper lifecycle management.
        if props.contains_key("slot.name") {
            return Err(RwError::from(ProtocolError(
                "Replication slot name ('slot.name') cannot be specified manually. \
                RisingWave now automatically manages replication slots. \
                Each source will have a unique auto-generated slot that will be \
                automatically created and cleaned up. Please remove 'slot.name' from your WITH clause."
                    .to_owned(),
            )));
        }

        // Always auto-generate a unique slot name with UUID
        // Format: "rw_cdc_{uuid}" for uniqueness
        // e.g. "rw_cdc_f9a3567e6dd54bf5900444c8b1c03815"
        let uuid = uuid::Uuid::new_v4();
        props.insert("slot.name".into(), format!("rw_cdc_{}", uuid.simple()));

        if !props.contains_key("schema.name") {
            // Default schema name is "public"
            props.insert("schema.name".into(), "public".into());
        }
        if !props.contains_key("publication.name") {
            // Default publication name is "rw_publication"
            props.insert("publication.name".into(), "rw_publication".into());
        }
        if !props.contains_key("publication.create.enable") {
            // Default auto create publication if doesn't exist
            props.insert("publication.create.enable".into(), "true".into());
        }
    }

    if connector == SQL_SERVER_CDC_CONNECTOR && !props.contains_key("schema.name") {
        // Default schema name is "dbo"
        props.insert("schema.name".into(), "dbo".into());
    }

    // Validate cdc.source.wait.streaming.start.timeout for all CDC connectors
    if (connector == MYSQL_CDC_CONNECTOR
        || connector == POSTGRES_CDC_CONNECTOR
        || connector == CITUS_CDC_CONNECTOR
        || connector == MONGODB_CDC_CONNECTOR
        || connector == SQL_SERVER_CDC_CONNECTOR)
        && let Some(timeout_value) = props.get("cdc.source.wait.streaming.start.timeout")
        && timeout_value.parse::<u32>().is_err()
    {
        return Err(ErrorCode::InvalidConfigValue {
            config_entry: "cdc.source.wait.streaming.start.timeout".to_owned(),
            config_value: timeout_value.to_owned(),
        }
        .into());
    }

    // Validate debezium.max.queue.size for all CDC connectors
    if (connector == MYSQL_CDC_CONNECTOR
        || connector == POSTGRES_CDC_CONNECTOR
        || connector == CITUS_CDC_CONNECTOR
        || connector == MONGODB_CDC_CONNECTOR
        || connector == SQL_SERVER_CDC_CONNECTOR)
        && let Some(queue_size_value) = props.get("debezium.max.queue.size")
        && queue_size_value.parse::<u32>().is_err()
    {
        return Err(ErrorCode::InvalidConfigValue {
            config_entry: "debezium.max.queue.size".to_owned(),
            config_value: queue_size_value.to_owned(),
        }
        .into());
    }

    Ok(())
}
