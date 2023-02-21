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

use bytes::Bytes;
use itertools::Itertools;
use reqwest::Url;
use risingwave_common::error::ErrorCode::{InternalError, InvalidParameterValue, ProtocolError};
use risingwave_common::error::{Result, RwError};

use crate::parser::WriteGuard;

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
