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
use std::collections::{BTreeMap, HashMap};

use anyhow::anyhow;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::S3;
use opendal::Operator;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::opendal_sink::FileSink;
use crate::sink::file_sink::opendal_sink::OpendalSinkBackend;
use crate::sink::{Result, SinkError, SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT};
use crate::source::UnknownFields;
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct S3Common {
    #[serde(rename = "s3.region_name")]
    pub region_name: String,
    #[serde(rename = "s3.bucket_name")]
    pub bucket_name: String,
    /// The directory where the sink file is located.
    #[serde(rename = "s3.path")]
    pub path: String,
    #[serde(rename = "s3.credentials.access", default)]
    pub access: Option<String>,
    #[serde(rename = "s3.credentials.secret", default)]
    pub secret: Option<String>,
    #[serde(rename = "s3.endpoint_url")]
    pub endpoint_url: Option<String>,
    #[serde(rename = "s3.assume_role", default)]
    pub assume_role: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct S3Config {
    #[serde(flatten)]
    pub common: S3Common,

    pub r#type: String, // accept "append-only"

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

pub const S3_SINK: &str = "s3";

impl<S: OpendalSinkBackend> FileSink<S> {
    pub fn new_s3_sink(config: S3Config) -> Result<Operator> {
        // Create s3 builder.
        let mut builder = S3::default();
        builder.bucket(&config.common.bucket_name);
        builder.region(&config.common.region_name);

        if let Some(endpoint_url) = config.common.endpoint_url {
            builder.endpoint(&endpoint_url);
        }

        if let Some(access) = config.common.access {
            builder.access_key_id(&access);
        } else {
            tracing::error!(
                "access key id of aws s3 is not set, bucket {}",
                config.common.bucket_name
            );
        }

        if let Some(secret) = config.common.secret {
            builder.secret_access_key(&secret);
        } else {
            tracing::error!(
                "secret access key of aws s3 is not set, bucket {}",
                config.common.bucket_name
            );
        }

        if let Some(assume_role) = config.common.assume_role {
            builder.role_arn(&assume_role);
        }
        builder.disable_config_load();
        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();

        Ok(operator)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3Sink;

impl UnknownFields for S3Config {
    fn unknown_fields(&self) -> HashMap<String, String> {
        self.unknown_fields.clone()
    }
}

impl OpendalSinkBackend for S3Sink {
    type Properties = S3Config;

    const SINK_NAME: &'static str = S3_SINK;

    fn from_btreemap(btree_map: BTreeMap<String, String>) -> Result<Self::Properties> {
        let config = serde_json::from_value::<S3Config>(serde_json::to_value(btree_map).unwrap())
            .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY && config.r#type != SINK_TYPE_UPSERT {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }

    fn new_operator(properties: S3Config) -> Result<Operator> {
        FileSink::<S3Sink>::new_s3_sink(properties)
    }

    fn get_path(properties: Self::Properties) -> String {
        properties.common.path
    }

    fn get_engine_type() -> super::opendal_sink::EngineType {
        super::opendal_sink::EngineType::S3
    }
}
