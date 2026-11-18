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

use risingwave_connector::error::ConnectorError;
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

/// Returns whether the Pulsar schema is used. The Pulsar schema options themselves are validated by
/// `PulsarSchemaConfig::from_options` when building the parser config. Only the connector and
/// format, which that config is unaware of, are checked here.
fn validate_pulsar_schema_options(
    format_encode: &FormatEncodeOptions,
    connector: &str,
) -> Result<bool> {
    let options = WithOptions::try_from(format_encode.row_options())?;
    if !uses_pulsar_schema(&options, options.secret_ref()) {
        return Ok(false);
    }

    if connector != PULSAR_CONNECTOR
        || format_encode.format != Format::Plain
        || !matches!(format_encode.row_encode, Encode::Avro | Encode::Json)
    {
        return Err(RwError::from(ProtocolError(format!(
            "Pulsar schema requires connector = '{PULSAR_CONNECTOR}' with FORMAT PLAIN ENCODE AVRO or JSON"
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
    let uses_pulsar_schema = validate_pulsar_schema_options(format_encode, &connector)?;

    let protocol_error = |e: ConnectorError| RwError::from(ProtocolError(e.to_report_string()));
    dispatch_cdc_source_type!(
        props.cdc_source_type(),
        T,
        {
            T::check_license()?;
            T::check_format_encode(
                format_to_prost(&format_encode.format),
                row_encode_to_prost(&format_encode.row_encode),
            )
            .map_err(protocol_error)?;
            T::check_and_fill_create_with_options(props).map_err(protocol_error)?;
            return Ok(());
        },
        {}
    );

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
    if connector != KAFKA_CONNECTOR && !uses_pulsar_schema {
        let res = match (&format_encode.format, &format_encode.row_encode) {
            (Format::Plain | Format::Upsert, Encode::Protobuf | Encode::Avro) => {
                let mut options = WithOptions::try_from(format_encode.row_options())?;
                let (_, use_schema_registry) =
                    get_schema_location(options.inner_mut()).unwrap_or_default();
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
    Ok(())
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
    fn pulsar_schema_accepts_minimal_plain_avro_and_json_options() {
        for encode in [Encode::Avro, Encode::Json] {
            let format_encode = format_encode(
                Format::Plain,
                encode,
                &[
                    (PULSAR_SCHEMA_URL_KEY, "https://localhost:8443"),
                    (PULSAR_SCHEMA_AUTH_TOKEN_KEY, "schema-token"),
                ],
            );
            validate_compatibility(&format_encode, &mut source_options(PULSAR_CONNECTOR)).unwrap();
        }
    }

    #[test]
    fn pulsar_schema_rejects_other_connectors_and_formats() {
        for (connector, format, encode) in [
            (KAFKA_CONNECTOR, Format::Plain, Encode::Avro),
            (PULSAR_CONNECTOR, Format::Upsert, Encode::Avro),
            (KAFKA_CONNECTOR, Format::Plain, Encode::Json),
            (PULSAR_CONNECTOR, Format::Upsert, Encode::Json),
            (PULSAR_CONNECTOR, Format::Plain, Encode::Protobuf),
        ] {
            let format_encode = format_encode(
                format,
                encode,
                &[(PULSAR_SCHEMA_URL_KEY, "http://localhost:8080")],
            );
            let error =
                validate_compatibility(&format_encode, &mut source_options(connector)).unwrap_err();
            assert!(error.to_string().contains(
                "Pulsar schema requires connector = 'pulsar' with FORMAT PLAIN ENCODE AVRO or JSON"
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

    #[test]
    fn glue_schema_registry_non_kafka() {
        let format_encode = format_encode(
            Format::Plain,
            Encode::Avro,
            &[(AWS_GLUE_SCHEMA_ARN_KEY, "placeholder")],
        );
        validate_compatibility(&format_encode, &mut source_options(KINESIS_CONNECTOR)).unwrap();
    }
}
