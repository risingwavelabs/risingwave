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
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Gcs;
use crate::sink::opendal::OpenDalSinkWriter;
use opendal::{Metakey, Operator};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use serde::Deserialize;
use serde_derive::Serialize;
use serde_json::Value;
use serde_with::serde_as;
use thiserror_ext::AsReport;
use with_options::WithOptions;

use crate::error::ConnectorError;
use crate::sink::encoder::{JsonEncoder, RowEncoder, TimestampHandlingMode};
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SinkWriter, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct GcsCommon {
    #[serde(rename = "gcs.bucket_name")]
    pub bucket_name: String,

    /// The base64 encoded credential key. If not set, ADC will be used.
    #[serde(rename = "gcs.credential")]
    pub credential: Option<String>,

    /// If credential/ADC is not set. The service account can be used to provide the credential info.
    #[serde(rename = "gcs.service_account", default)]
    pub service_account: Option<String>,

    #[serde(rename = "match_pattern", default)]
    pub match_pattern: Option<String>,

    #[serde(flatten)]
    pub unknown_fields: HashMap<String, String>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct GcsConfig {
    #[serde(flatten)]
    pub common: GcsCommon,

    pub r#type: String, // accept "append-only" or "upsert"
}

pub const GCS_SINK: &str = "gcs";

impl GcsConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<GcsConfig>(serde_json::to_value(properties).unwrap())
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
}

#[derive(Debug)]
pub struct GcsSink {
    pub config: GcsConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl GcsSink {
    pub fn new(
        config: GcsConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Result<Self> {
        Ok(Self {
            config,
            schema,
            pk_indices,
            is_append_only,
        })
    }
}

impl Sink for GcsSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<OpenDalSinkWriter>;

    const SINK_NAME: &'static str = GCS_SINK;

    async fn validate(&self) -> Result<()> {
        todo!()
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        todo!()
    }
}

impl TryFrom<SinkParam> for GcsSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = GcsConfig::from_hashmap(param.properties)?;
        GcsSink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}

impl OpenDalSinkWriter {
    pub async fn new_gcs_sink(config: GcsConfig) -> Result<Operator> {
        // Create gcs builder.
        let mut builder = Gcs::default();

        builder.bucket(&config.common.bucket_name);

        // if credential env is set, use it. Otherwise, ADC will be used.
        if let Some(cred) = config.common.credential {
            builder.credential(&cred);
        } else {
            let cred = std::env::var("GOOGLE_APPLICATION_CREDENTIALS");
            if let Ok(cred) = cred {
                builder.credential(&cred);
            }
        }

        if let Some(service_account) = config.common.service_account {
            builder.service_account(&service_account);
        }
        let operator: Operator = Operator::new(builder)
            .map_err(|e| SinkError::Connector(e.into()))?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(operator)
    }
}
