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

use anyhow::anyhow;
use opendal::layers::{LoggingLayer, RetryLayer};
use opendal::services::Gcs;
use opendal::Operator;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use crate::sink::opendal_sink::OpenDalSinkWriter;
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY,
    SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

const GCS_WRITE_BUFFER_SIZE: usize = 16 * 1024 * 1024;
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

    #[serde(rename = "gcs.path", default)]
    pub path: String,

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

impl GcsSink {
    pub fn new_gcs_sink(config: GcsConfig) -> Result<Operator> {
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
        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();
        Ok(operator)
    }
}

impl Sink for GcsSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<OpenDalSinkWriter>;

    const SINK_NAME: &'static str = GCS_SINK;

    async fn validate(&self) -> Result<()> {
        let _op = Self::new_gcs_sink(self.config.clone())?;
        Ok(())
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let op = Self::new_gcs_sink(self.config.clone())?;
        let path = self.config.common.path.as_ref();
        let writer = op
            .writer_with(path)
            .concurrent(8)
            .buffer(GCS_WRITE_BUFFER_SIZE)
            .await?;
        Ok(OpenDalSinkWriter::new(
            writer,
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )?
        .into_log_sinker(writer_param.sink_metrics))
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
