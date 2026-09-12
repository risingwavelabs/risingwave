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
                    Format::DebeziumMongo => vec![Encode::Json],
                ),
                PULSAR_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                ),
                KINESIS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes, Encode::Csv],
                    Format::Upsert => vec![Encode::Json, Encode::Avro],
                    Format::Debezium => vec![Encode::Json],
                ),
                GOOGLE_PUBSUB_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Avro, Encode::Bytes],
                    Format::Debezium => vec![Encode::Json],
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
                MONGODB_CDC_CONNECTOR => hashmap!(
                    Format::DebeziumMongo => vec![Encode::Json],
                ),
                NATS_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Bytes],
                ),
                MQTT_CONNECTOR => hashmap!(
                    Format::Plain => vec![Encode::Json, Encode::Protobuf, Encode::Bytes],
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

/// Keep this policy in sync with Java's `SourceValidateHandler.validateHeartbeatInterval`.
/// Validates user-supplied options, not the final Debezium configuration. On CREATE, an omitted
/// interval uses the connector default (300000 ms for PostgreSQL/Citus); on ALTER, omission leaves
/// the existing interval unchanged. Unrelated connector heartbeat mechanisms are not checked here.
pub(crate) fn validate_cdc_heartbeat_interval(
    connector: &str,
    props: &BTreeMap<String, String>,
) -> Result<()> {
    let heartbeat_required = match connector {
        POSTGRES_CDC_CONNECTOR | CITUS_CDC_CONNECTOR => true,
        MYSQL_CDC_CONNECTOR
        | SQL_SERVER_CDC_CONNECTOR
        | MONGODB_CDC_CONNECTOR
        | ORACLE_CDC_CONNECTOR => false,
        _ => return Ok(()),
    };
    let Some(value) = props.get("debezium.heartbeat.interval.ms") else {
        return Ok(());
    };

    // Debezium defines this field as INT with a nonnegative-integer validator, not LONG.
    let interval = value
        .parse::<i32>()
        .ok()
        .filter(|interval| *interval >= 0)
        .ok_or_else(|| {
            ErrorCode::InvalidParameterValue(format!(
                "'debezium.heartbeat.interval.ms' must be an integer between 0 and 2147483647, got: '{value}'"
            ))
        })?;
    if heartbeat_required && interval == 0 {
        return Err(ErrorCode::InvalidParameterValue(
            "'debezium.heartbeat.interval.ms' must be greater than 0 for PostgreSQL and Citus CDC: heartbeats are required for replication-slot progress and WAL reclamation".to_owned(),
        )
        .into());
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
        match props.get("slot.name") {
            None => {
                // Build a random slot name with UUID
                // e.g. "rw_cdc_f9a3567e6dd54bf5900444c8b1c03815"
                let uuid = uuid::Uuid::new_v4();
                props.insert("slot.name".into(), format!("rw_cdc_{}", uuid.simple()));
            }
            Some(slot_name) => {
                // please refer to
                // - https://github.com/debezium/debezium/blob/97956ce25b7612e3413d363658661896b7d2e0a2/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/PostgresConnectorConfig.java#L1179
                // - https://doxygen.postgresql.org/slot_8c.html#afac399f07320b9adfd2c599cf822aaa3
                if !slot_name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                    || slot_name.len() > 63
                {
                    return Err(RwError::from(ProtocolError(format!(
                        "Invalid replication slot name: {:?}. Valid replication slot name must contain only digits, lowercase characters and underscores with length <= 63",
                        slot_name
                    ))));
                }
            }
        }

        if !props.contains_key("schema.name") {
            // Default schema name is "public"
            props.insert("schema.name".into(), "public".into());
        }
        if !props.contains_key("publication.name") {
            // Build a random publication name with UUID to avoid conflicts between sources
            // e.g. "rw_publication_f9a3567e6dd54bf5900444c8b1c03815"
            let uuid = uuid::Uuid::new_v4();
            props.insert(
                "publication.name".into(),
                format!("rw_publication_{}", uuid.simple()),
            );
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
        || connector == SQL_SERVER_CDC_CONNECTOR
        || connector == ORACLE_CDC_CONNECTOR)
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
        || connector == SQL_SERVER_CDC_CONNECTOR
        || connector == ORACLE_CDC_CONNECTOR)
        && let Some(queue_size_value) = props.get("debezium.max.queue.size")
        && queue_size_value.parse::<u32>().is_err()
    {
        return Err(ErrorCode::InvalidConfigValue {
            config_entry: "debezium.max.queue.size".to_owned(),
            config_value: queue_size_value.to_owned(),
        }
        .into());
    }

    validate_cdc_heartbeat_interval(&connector, props)
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::ast::{Ident, SqlOption, SqlOptionValue, Value};

    use super::*;
    use crate::handler::alter_source_props::handle_alter_source_props_inner;
    use crate::test_utils::LocalFrontend;

    const HEARTBEAT_INTERVAL_CASES: [Option<&str>; 8] = [
        None,
        Some("0"),
        Some("1"),
        Some("300000"),
        Some("2147483647"),
        Some("-1"),
        Some("invalid"),
        Some("2147483648"),
    ];

    #[test]
    fn test_cdc_heartbeat_interval() {
        for connector in [
            MYSQL_CDC_CONNECTOR,
            SQL_SERVER_CDC_CONNECTOR,
            MONGODB_CDC_CONNECTOR,
            POSTGRES_CDC_CONNECTOR,
            CITUS_CDC_CONNECTOR,
            ORACLE_CDC_CONNECTOR,
        ] {
            let mut props = BTreeMap::new();
            assert!(validate_cdc_heartbeat_interval(connector, &props).is_ok());
            for value in ["1", "300000", "2147483647", "+1"] {
                props.insert(
                    "debezium.heartbeat.interval.ms".to_owned(),
                    value.to_owned(),
                );
                assert!(
                    validate_cdc_heartbeat_interval(connector, &props).is_ok(),
                    "{connector}: {value}"
                );
            }
            for value in [
                "-1",
                "",
                "invalid",
                "0.5",
                "2147483648",
                "9223372036854775807",
                "9223372036854775808",
                " 1",
                "1 ",
                "１",
            ] {
                props.insert(
                    "debezium.heartbeat.interval.ms".to_owned(),
                    value.to_owned(),
                );
                let err = validate_cdc_heartbeat_interval(connector, &props).unwrap_err();
                assert!(
                    err.to_string().contains("between 0 and 2147483647"),
                    "{err}"
                );
            }
            for value in ["0", "+0", "-0"] {
                props.insert(
                    "debezium.heartbeat.interval.ms".to_owned(),
                    value.to_owned(),
                );
                let result = validate_cdc_heartbeat_interval(connector, &props);
                if matches!(connector, POSTGRES_CDC_CONNECTOR | CITUS_CDC_CONNECTOR) {
                    let err = result.unwrap_err();
                    assert!(err.to_string().contains("WAL reclamation"), "{err}");
                } else {
                    result.unwrap();
                }
            }
        }
    }

    #[test]
    fn test_create_cdc_heartbeat_interval() {
        for (connector, format) in [
            (MYSQL_CDC_CONNECTOR, FormatEncodeOptions::debezium_json()),
            (POSTGRES_CDC_CONNECTOR, FormatEncodeOptions::debezium_json()),
            (
                MONGODB_CDC_CONNECTOR,
                FormatEncodeOptions::debezium_mongo_json(),
            ),
        ] {
            for value in HEARTBEAT_INTERVAL_CASES {
                let mut props = BTreeMap::from([("connector".to_owned(), connector.to_owned())]);
                if let Some(value) = value {
                    props.insert(
                        "debezium.heartbeat.interval.ms".to_owned(),
                        value.to_owned(),
                    );
                }
                let expected = validate_cdc_heartbeat_interval(connector, &props);
                let result = validate_compatibility(&format, &mut props);
                assert_eq!(
                    result.map_err(|e| e.to_string()),
                    expected.map_err(|e| e.to_string()),
                    "{connector}: {value:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_alter_cdc_heartbeat_interval() {
        let frontend = LocalFrontend::new(Default::default()).await;
        for connector in [
            MYSQL_CDC_CONNECTOR,
            POSTGRES_CDC_CONNECTOR,
            MONGODB_CDC_CONNECTOR,
            SQL_SERVER_CDC_CONNECTOR,
            CITUS_CDC_CONNECTOR,
            ORACLE_CDC_CONNECTOR,
        ] {
            for value in HEARTBEAT_INTERVAL_CASES {
                let mut props = BTreeMap::new();
                let mut options = Vec::new();
                if let Some(value) = value {
                    props.insert(
                        "debezium.heartbeat.interval.ms".to_owned(),
                        value.to_owned(),
                    );
                    options.push(SqlOption {
                        name: ObjectName(vec![Ident::new_unchecked(
                            "debezium.heartbeat.interval.ms",
                        )]),
                        value: SqlOptionValue::Value(Value::SingleQuotedString(value.to_owned())),
                    });
                }
                let expected = validate_cdc_heartbeat_interval(connector, &props);
                // Exercise the shared ALTER SOURCE/TABLE frontend path. The mock meta client
                // accepts valid requests; connector-specific ALTER allowlists are not tested here.
                let result = handle_alter_source_props_inner(
                    &frontend.session_ref(),
                    options,
                    SourceId::new(1),
                    connector,
                )
                .await;
                assert_eq!(
                    result.map_err(|e| e.to_string()),
                    expected.map_err(|e| e.to_string()),
                    "{connector}: {value:?}"
                );
            }
        }
    }

    #[test]
    fn test_non_cdc_heartbeat_interval_ignored() {
        let props = BTreeMap::from([(
            "debezium.heartbeat.interval.ms".to_owned(),
            "invalid".to_owned(),
        )]);
        assert!(validate_cdc_heartbeat_interval(KAFKA_CONNECTOR, &props).is_ok());
    }
}
