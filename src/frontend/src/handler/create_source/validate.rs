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

use risingwave_connector::dispatch_cdc_source_type;
use risingwave_connector::source::cdc::{
    CDC_CONNECTOR_NAMES, CdcSourceFrontendValidator, CdcSourceType, is_cdc_connector_name,
};
use risingwave_connector::source::{ADBC_SNOWFLAKE_CONNECTOR, BATCH_POSIX_FS_CONNECTOR};

use super::external_schema::{format_to_prost, row_encode_to_prost};
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
        ))
    });

/// Format, license and connector-specific option checks of a CDC connector, each defined by the
/// connector's `CdcSourceFrontendValidator` impl.
fn validate_cdc_connector(
    connector: &str,
    format_encode: &FormatEncodeOptions,
    props: &mut BTreeMap<String, String>,
) -> Result<()> {
    let format = format_to_prost(&format_encode.format);
    let encode = row_encode_to_prost(&format_encode.row_encode);
    dispatch_cdc_source_type!(
        CdcSourceType::from(connector),
        T,
        {
            let compatible_encodes = T::COMPATIBLE_FORMATS
                .iter()
                .find(|(compatible_format, _)| *compatible_format == format)
                .map(|(_, encodes)| *encodes)
                .ok_or_else(|| {
                    RwError::from(ProtocolError(format!(
                        "connector {} does not support format {:?}",
                        connector, format_encode.format
                    )))
                })?;
            if !compatible_encodes.contains(&encode) {
                return Err(RwError::from(ProtocolError(format!(
                    "connector {} does not support format {:?} with encode {:?}",
                    connector, format_encode.format, format_encode.row_encode
                ))));
            }
            if let Some(feature) = T::LICENSE_FEATURE {
                feature.check_available()?;
            }
            T::fill_and_validate_props(props)
                .map_err(|err| ProtocolError(err.to_report_string()))?;
        },
        unreachable!("{connector} is not a CDC connector")
    );
    Ok(())
}

fn validate_decimal_handling_mode(props: &BTreeMap<String, String>) -> Result<()> {
    if let Some(mode) = props.get("debezium.decimal.handling.mode")
        && mode != "string"
    {
        return Err(RwError::from(ProtocolError(format!(
            "'debezium.decimal.handling.mode' must be 'string', got: '{mode}'"
        ))));
    }
    Ok(())
}

/// Requires an explicitly supplied heartbeat interval to be a positive signed 32-bit integer.
/// Keep this policy in sync with Java's `SourceValidateHandler.validateHeartbeatInterval`.
/// Validates user-supplied options, not the final Debezium configuration. On CREATE, an omitted
/// interval uses the connector default; on ALTER, omission leaves the existing interval unchanged.
/// Unrelated connector heartbeat mechanisms are not checked here.
pub fn validate_heartbeat_interval(props: &BTreeMap<String, String>) -> Result<()> {
    let Some(value) = props.get("debezium.heartbeat.interval.ms") else {
        return Ok(());
    };

    // Match Java's ASCII integer syntax and Debezium's signed 32-bit range.
    if !value.parse::<i32>().is_ok_and(|interval| interval > 0) {
        return Err(ErrorCode::InvalidParameterValue(format!(
            "'debezium.heartbeat.interval.ms' must be a positive integer, got: '{value}'"
        ))
        .into());
    }
    Ok(())
}

fn validate_heartbeat_table_auto_initialize(
    connector: &str,
    props: &BTreeMap<String, String>,
) -> Result<()> {
    const AUTO_INITIALIZE_KEY: &str = "heartbeat.table.auto.initialize";
    let Some(value) = props.get(AUTO_INITIALIZE_KEY) else {
        return Ok(());
    };
    let enabled = value.parse::<bool>().map_err(|_| {
        ErrorCode::InvalidParameterValue(format!(
            "'{AUTO_INITIALIZE_KEY}' must be 'true' or 'false'"
        ))
    })?;
    if !enabled {
        return Ok(());
    }
    if connector != ORACLE_CDC_CONNECTOR {
        return Err(ErrorCode::InvalidParameterValue(format!(
            "'{AUTO_INITIALIZE_KEY}=true' is not supported for connector '{connector}'"
        ))
        .into());
    }

    if !props
        .get("heartbeat.table.name")
        .is_some_and(|value| !value.trim().is_empty())
    {
        return Err(ErrorCode::InvalidParameterValue(format!(
            "'{AUTO_INITIALIZE_KEY}' requires 'heartbeat.table.name'"
        ))
        .into());
    }
    Ok(())
}

/// Returns whether the Pulsar schema is used. The Pulsar schema options themselves are validated by
/// `PulsarSchemaConfig::from_options` when building the parser config. Only the connector and
/// format, which that config is unaware of, are checked here.
fn validate_pulsar_schema_options(
    format_encode: &FormatEncodeOptions,
    connector: &str,
) -> Result<bool> {
    let options = WithOptions::try_from(format_encode.row_options())?;
    if !options.contains_key(PULSAR_SCHEMA_URL_KEY)
        && !options.secret_ref().contains_key(PULSAR_SCHEMA_URL_KEY)
    {
        return Ok(false);
    }

    if connector != PULSAR_CONNECTOR
        || format_encode.format != Format::Plain
        || format_encode.row_encode != Encode::Avro
    {
        return Err(RwError::from(ProtocolError(format!(
            "Pulsar schema requires connector = '{PULSAR_CONNECTOR}' with FORMAT PLAIN ENCODE AVRO"
        ))));
    }

    Ok(true)
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

    let is_cdc = is_cdc_connector_name(&connector);
    if !is_cdc && !CONNECTORS_COMPATIBLE_FORMATS.contains_key(&connector) {
        return Err(RwError::from(ProtocolError(format!(
            "connector {:?} is not supported, accept {:?}",
            connector,
            CONNECTORS_COMPATIBLE_FORMATS
                .keys()
                .map(String::as_str)
                .chain(CDC_CONNECTOR_NAMES.iter().copied())
                .collect::<Vec<_>>()
        ))));
    }

    // RisingWave consumes schema-less JSON from Debezium and cannot reconstruct the scale of
    // binary logical decimals emitted by `precise`. `double` can lose precision, so an explicit
    // override must retain the common `string` default from `debezium.properties`.
    validate_decimal_handling_mode(props)?;

    let uses_pulsar_schema = validate_pulsar_schema_options(format_encode, &connector)?;
    if connector != KAFKA_CONNECTOR && !uses_pulsar_schema {
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

    if is_cdc {
        validate_cdc_connector(&connector, format_encode, props)?;
    } else {
        let compatible_encodes = CONNECTORS_COMPATIBLE_FORMATS[&connector]
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
    }

    // Validate cdc.source.wait.streaming.start.timeout for all CDC connectors
    if is_cdc
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
    if is_cdc
        && let Some(queue_size_value) = props.get("debezium.max.queue.size")
        && queue_size_value.parse::<u32>().is_err()
    {
        return Err(ErrorCode::InvalidConfigValue {
            config_entry: "debezium.max.queue.size".to_owned(),
            config_value: queue_size_value.to_owned(),
        }
        .into());
    }

    validate_heartbeat_interval(props)?;
    validate_heartbeat_table_auto_initialize(&connector, props)
}

#[cfg(test)]
mod tests {
    use risingwave_sqlparser::ast::SqlOption;

    use super::*;

    fn format_encode(
        format: Format,
        encode: Encode,
        options: &[(&str, &str)],
    ) -> FormatEncodeOptions {
        let row_options = options
            .iter()
            .map(|(name, value)| {
                let name = name.to_string();
                let value = value.to_string();
                SqlOption::try_from((&name, &value)).unwrap()
            })
            .collect();
        FormatEncodeOptions {
            format,
            row_encode: encode,
            row_options,
            key_encode: None,
        }
    }

    fn source_options(connector: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(UPSTREAM_SOURCE_KEY.to_owned(), connector.to_owned())])
    }

    #[test]
    fn pulsar_schema_accepts_minimal_plain_avro_options() {
        let format_encode = format_encode(
            Format::Plain,
            Encode::Avro,
            &[
                (PULSAR_SCHEMA_URL_KEY, "https://localhost:8443"),
                (PULSAR_SCHEMA_AUTH_TOKEN_KEY, "schema-token"),
            ],
        );
        validate_compatibility(&format_encode, &mut source_options(PULSAR_CONNECTOR)).unwrap();
    }

    #[test]
    fn pulsar_schema_rejects_other_connectors_and_formats() {
        for (connector, format, encode) in [
            (KAFKA_CONNECTOR, Format::Plain, Encode::Avro),
            (PULSAR_CONNECTOR, Format::Upsert, Encode::Avro),
            (PULSAR_CONNECTOR, Format::Plain, Encode::Protobuf),
            (PULSAR_CONNECTOR, Format::Plain, Encode::Json),
        ] {
            let format_encode = format_encode(
                format,
                encode,
                &[(PULSAR_SCHEMA_URL_KEY, "http://localhost:8080")],
            );
            let error =
                validate_compatibility(&format_encode, &mut source_options(connector)).unwrap_err();
            assert!(error.to_string().contains(
                "Pulsar schema requires connector = 'pulsar' with FORMAT PLAIN ENCODE AVRO"
            ));
        }
    }

    #[test]
    fn confluent_schema_registry_behavior_is_unchanged() {
        let format_encode = format_encode(
            Format::Plain,
            Encode::Avro,
            &[("schema.registry", "http://localhost:8081")],
        );
        validate_compatibility(&format_encode, &mut source_options(KAFKA_CONNECTOR)).unwrap();
    }
}
