// Copyright 2023 RisingWave Labs
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
use std::collections::HashMap;
use std::path::Path;

use bytes::Bytes;
use itertools::Itertools;
use reqwest::Url;
use risingwave_common::error::ErrorCode::{
    InternalError, InvalidConfigValue, InvalidParameterValue, ProtocolError,
};
use risingwave_common::error::{Result, RwError};

use crate::aws_auth::AwsAuthProps;
use crate::aws_utils::{default_conn_config, s3_client};
use crate::parser::WriteGuard;

const AVRO_SCHEMA_LOCATION_S3_REGION: &str = "region";

/// get kafka topic name
pub(super) fn get_kafka_topic(props: &HashMap<String, String>) -> Result<&String> {
    const KAFKA_TOPIC_KEY1: &str = "kafka.topic";
    const KAFKA_TOPIC_KEY2: &str = "topic";

    if let Some(topic) = props.get(KAFKA_TOPIC_KEY1) {
        return Ok(topic);
    }
    if let Some(topic) = props.get(KAFKA_TOPIC_KEY2) {
        return Ok(topic);
    }

    Err(RwError::from(ProtocolError(format!(
        "Must specify '{}' or '{}'",
        KAFKA_TOPIC_KEY1, KAFKA_TOPIC_KEY2,
    ))))
}

/// download bytes from http(s) url
pub(super) async fn download_from_http(location: &Url) -> Result<Bytes> {
    let res = reqwest::get(location.clone()).await.map_err(|e| {
        InvalidParameterValue(format!(
            "failed to make request to URL: {}, err: {}",
            location, e
        ))
    })?;
    if !res.status().is_success() {
        return Err(RwError::from(InvalidParameterValue(format!(
            "Http request err, URL: {}, status code: {}",
            location,
            res.status()
        ))));
    }
    res.bytes()
        .await
        .map_err(|e| InvalidParameterValue(format!("failed to read HTTP body: {}", e)).into())
}

// `results.len()` should greater that zero
// if all results are errors, return err
// if all ok, return ok
// if part of them are errors, log err and return ok
#[inline]
pub(super) fn at_least_one_ok(mut results: Vec<Result<WriteGuard>>) -> Result<WriteGuard> {
    let errors = results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .collect_vec();
    let first_ok_index = results.iter().position(|r| r.is_ok());
    let err_message = errors
        .into_iter()
        .map(|r| r.to_string())
        .collect_vec()
        .join(", ");

    if let Some(first_ok_index) = first_ok_index {
        if !err_message.is_empty() {
            tracing::error!("failed to parse some columns: {}", err_message)
        }
        results.remove(first_ok_index)
    } else {
        Err(RwError::from(InternalError(format!(
            "failed to parse all columns: {}",
            err_message
        ))))
    }
}

// For parser that doesn't support key currently
#[macro_export]
macro_rules! only_parse_payload {
    ($self:ident, $payload:ident, $writer:ident) => {
        if $payload.is_some() {
            $self.parse_inner($payload.unwrap(), $writer).await
        } else {
            Err(RwError::from(ErrorCode::InternalError(
                "Empty payload with nonempty key".into(),
            )))
        }
    };
}

// Extract encoding config and encoding type from ParserProperties
// for message key.
//
// Suppose (A, B) is the combination of key/payload combination:
// For (None, B), key should be the the key setting from B
// For (A, B), key should be the value setting from A
#[macro_export]
macro_rules! extract_key_config {
    ($props:ident) => {
        match $props.key_encoding_config {
            Some(config) => (config, EncodingType::Value),
            None => ($props.encoding_config.clone(), EncodingType::Key),
        }
    };
}

/// Read schema from local file. For on-premise or testing.
pub(super) fn read_schema_from_local(path: impl AsRef<Path>) -> Result<String> {
    std::fs::read_to_string(path.as_ref()).map_err(|e| e.into())
}

/// Read schema from http/https. For common usage.
pub(super) async fn read_schema_from_http(location: &Url) -> Result<String> {
    let schema_bytes = download_from_http(location).await?;

    String::from_utf8(schema_bytes.into()).map_err(|e| {
        RwError::from(InternalError(format!(
            "read schema string from https failed {}",
            e
        )))
    })
}

/// Read schema from s3 bucket.
/// S3 file location format: <s3://bucket_name/file_name>
pub(super) async fn read_schema_from_s3(url: &Url, config: &AwsAuthProps) -> Result<String> {
    let bucket = url
        .domain()
        .ok_or_else(|| RwError::from(InternalError(format!("Illegal s3 path {}", url))))?;
    if config.region.is_none() {
        return Err(RwError::from(InvalidConfigValue {
            config_entry: AVRO_SCHEMA_LOCATION_S3_REGION.to_string(),
            config_value: "NONE".to_string(),
        }));
    }
    let key = url.path().replace('/', "");
    let sdk_config = config.build_config().await?;
    let s3_client = s3_client(&sdk_config, Some(default_conn_config()));
    let response = s3_client
        .get_object()
        .bucket(bucket.to_string())
        .key(key)
        .send()
        .await
        .map_err(|e| RwError::from(InternalError(e.to_string())))?;
    let body_bytes = response.body.collect().await.map_err(|e| {
        RwError::from(InternalError(format!(
            "Read Avro schema file from s3 {}",
            e
        )))
    })?;
    let schema_bytes = body_bytes.into_bytes().to_vec();
    String::from_utf8(schema_bytes)
        .map_err(|e| RwError::from(InternalError(format!("Avro schema not valid utf8 {}", e))))
}
