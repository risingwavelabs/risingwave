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
use opendal::services::S3;
use opendal::Operator;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use crate::sink::encoder::RowEncoder;
use crate::sink::opendal_sink::OpenDalSinkWriter;
use crate::sink::writer::{LogSinkerOf, SinkWriterExt};
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkParam, SINK_TYPE_APPEND_ONLY,
    SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

const S3_WRITE_BUFFER_SIZE: usize = 16 * 1024 * 1024;

#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct S3Common {
    #[serde(rename = "s3.region_name")]
    pub region_name: String,
    #[serde(rename = "s3.bucket_name")]
    pub bucket_name: String,
    #[serde(rename = "s3.path", default)]
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

    pub r#type: String, // accept "append-only" or "upsert"
}

pub const S3_SINK: &str = "s3";

impl S3Config {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config = serde_json::from_value::<S3Config>(serde_json::to_value(properties).unwrap())
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
pub struct S3Sink {
    pub config: S3Config,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl S3Sink {
    pub fn new(
        config: S3Config,
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

impl S3Sink {
    pub async fn new_s3_sink(config: S3Config) -> Result<Operator> {
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

        builder.enable_virtual_host_style();

        if let Some(assume_role) = config.common.assume_role {
            builder.role_arn(&assume_role);
        }

        let operator: Operator = Operator::new(builder)?
            .layer(LoggingLayer::default())
            .layer(RetryLayer::default())
            .finish();

        Ok(operator)
    }
}

impl Sink for S3Sink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<OpenDalSinkWriter>;

    const SINK_NAME: &'static str = S3_SINK;

    async fn validate(&self) -> Result<()> {
        let op = Self::new_s3_sink(self.config.clone()).await?;
        Ok(())
    }

    async fn new_log_sinker(
        &self,
        writer_param: crate::sink::SinkWriterParam,
    ) -> Result<Self::LogSinker> {
        let op = Self::new_s3_sink(self.config.clone()).await?;
        let path = self.config.common.path.as_ref();
        let writer = op
            .writer_with(&path)
            .concurrent(8)
            .buffer(S3_WRITE_BUFFER_SIZE)
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

impl TryFrom<SinkParam> for S3Sink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = S3Config::from_hashmap(param.properties)?;
        S3Sink::new(
            config,
            schema,
            param.downstream_pk,
            param.sink_type.is_append_only(),
        )
    }
}
