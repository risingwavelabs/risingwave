// Copyright 2025 RisingWave Labs
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
use std::collections::BTreeMap;

use anyhow::Context;
use bytes::Bytes;
use reqwest::Url;
use risingwave_common::bail;
use risingwave_common::types::{Datum, DatumCow, DatumRef};
use risingwave_pb::data::DataType as PbDataType;

use crate::aws_utils::load_file_descriptor_from_s3;
use crate::connector_common::AwsAuthProps;
use crate::error::ConnectorResult;
use crate::source::SourceMeta;

macro_rules! log_error {
    ($name:expr, $err:expr, $message:expr) => {
        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
            tracing::error!(
                column = $name,
                error = %$err.as_report(),
                suppressed_count,
                $message,
            );
        }
    };
}
pub(crate) use log_error;
use risingwave_pb::plan_common::additional_column;
use risingwave_pb::plan_common::additional_column::ColumnType;

use crate::parser::{AccessError, AccessResult};
use crate::source::cdc::DebeziumCdcMeta;

/// get kafka topic name
pub(super) fn get_kafka_topic(props: &BTreeMap<String, String>) -> ConnectorResult<&String> {
    const KAFKA_TOPIC_KEY1: &str = "kafka.topic";
    const KAFKA_TOPIC_KEY2: &str = "topic";

    if let Some(topic) = props.get(KAFKA_TOPIC_KEY1) {
        return Ok(topic);
    }
    if let Some(topic) = props.get(KAFKA_TOPIC_KEY2) {
        return Ok(topic);
    }

    // config
    bail!(
        "Must specify '{}' or '{}'",
        KAFKA_TOPIC_KEY1,
        KAFKA_TOPIC_KEY2
    )
}

/// download bytes from http(s) url
pub(super) async fn download_from_http(location: &Url) -> ConnectorResult<Bytes> {
    let res = reqwest::get(location.clone())
        .await
        .with_context(|| format!("failed to make request to {location}"))?
        .error_for_status()
        .with_context(|| format!("http request failed for {location}"))?;

    let bytes = res
        .bytes()
        .await
        .with_context(|| format!("failed to read HTTP body of {location}"))?;

    Ok(bytes)
}

// For parser that doesn't support key currently
#[macro_export]
macro_rules! only_parse_payload {
    ($self:ident, $payload:ident, $writer:ident) => {
        if let Some(payload) = $payload {
            $self.parse_inner(payload, $writer).await
        } else {
            risingwave_common::bail!("empty payload with non-empty key")
        }
    };
}

/// Load raw bytes from:
/// * local file, for on-premise or testing.
/// * http/https, for common usage.
/// * s3 file location format: <s3://bucket_name/file_name>
pub(super) async fn bytes_from_url(
    url: &Url,
    config: Option<&AwsAuthProps>,
) -> ConnectorResult<Vec<u8>> {
    match (url.scheme(), config) {
        // TODO(Tao): support local file only when it's compiled in debug mode.
        ("file", _) => {
            let path = url
                .to_file_path()
                .ok()
                .with_context(|| format!("illegal path: {url}"))?;
            Ok(std::fs::read(&path)
                .with_context(|| format!("failed to read file from `{}`", path.display()))?)
        }
        ("https" | "http", _) => Ok(download_from_http(url).await?.into()),
        ("s3", Some(config)) => load_file_descriptor_from_s3(url, config).await,
        (scheme, _) => bail!("path scheme `{scheme}` is not supported"),
    }
}

pub fn extract_timestamp_from_meta(meta: &SourceMeta) -> DatumRef<'_> {
    match meta {
        SourceMeta::Kafka(kafka_meta) => kafka_meta.extract_timestamp(),
        SourceMeta::DebeziumCdc(cdc_meta) => cdc_meta.extract_timestamp(),
        SourceMeta::Kinesis(kinesis_meta) => kinesis_meta.extract_timestamp(),
        _ => None,
    }
}

pub fn extract_cdc_meta_column<'a>(
    cdc_meta: &'a DebeziumCdcMeta,
    column_type: &additional_column::ColumnType,
    column_name: &str,
) -> AccessResult<DatumRef<'a>> {
    match column_type {
        ColumnType::Timestamp(_) => Ok(cdc_meta.extract_timestamp()),
        ColumnType::DatabaseName(_) => Ok(cdc_meta.extract_database_name()),
        ColumnType::TableName(_) => Ok(cdc_meta.extract_table_name()),
        _ => Err(AccessError::UnsupportedAdditionalColumn {
            name: column_name.to_owned(),
        }),
    }
}

pub fn extract_headers_from_meta(meta: &SourceMeta) -> Option<Datum> {
    match meta {
        SourceMeta::Kafka(kafka_meta) => kafka_meta.extract_headers(), /* expect output of type `array[struct<varchar, bytea>]` */
        _ => None,
    }
}

pub fn extract_header_inner_from_meta<'a>(
    meta: &'a SourceMeta,
    inner_field: &str,
    data_type: Option<&PbDataType>,
) -> Option<DatumCow<'a>> {
    match meta {
        SourceMeta::Kafka(kafka_meta) => kafka_meta.extract_header_inner(inner_field, data_type), /* expect output of type `bytea` or `varchar` */
        _ => None,
    }
}

pub fn extract_subject_from_meta(meta: &SourceMeta) -> Option<DatumRef<'_>> {
    match meta {
        SourceMeta::Nats(nats_meta) => Some(nats_meta.extract_subject()),
        _ => None,
    }
}
