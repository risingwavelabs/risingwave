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
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_json::Value;
use serde_with::serde_as;
use with_options::WithOptions;

use super::encoder::{
    JsonEncoder, RowEncoder, TimeHandlingMode, TimestampHandlingMode, TimestamptzHandlingMode,
};
use super::snowflake_connector::{SnowflakeHttpClient, SnowflakeS3Client};
use super::writer::LogSinkerOf;
use super::{SinkError, SinkParam};
use crate::sink::writer::SinkWriterExt;
use crate::sink::{DummySinkCommitCoordinator, Result, Sink, SinkWriter, SinkWriterParam};

pub const SNOWFLAKE_SINK: &str = "snowflake";
const MAX_BATCH_NUM: u32 = 1000000;

// TODO: add comments
#[derive(Deserialize, Debug, Clone, WithOptions)]
pub struct SnowflakeCommon {
    #[serde(rename = "snowflake.database")]
    pub database: String,

    #[serde(rename = "snowflake.database.schema")]
    pub schema: String,

    #[serde(rename = "snowflake.database.schema.pipe")]
    pub pipe: String,

    #[serde(rename = "snowflake.account_identifier")]
    pub account_identifier: String,

    #[serde(rename = "snowflake.user")]
    pub user: String,

    #[serde(rename = "snowflake.rsa_public_key_fp")]
    pub rsa_public_key_fp: String,

    #[serde(rename = "snowflake.private.key")]
    pub private_key: String,

    #[serde(rename = "snowflake.private.key.passphrase")]
    pub private_key_passphrase: Option<String>,

    #[serde(rename = "snowflake.role")]
    pub role: String,

    #[serde(rename = "snowflake.s3_bucket")]
    pub s3_bucket: String,

    #[serde(rename = "snowflake.s3_file")]
    pub s3_file: Option<String>,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SnowflakeConfig {
    #[serde(flatten)]
    pub common: SnowflakeCommon,
}

impl SnowflakeConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SnowflakeConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

#[derive(Debug)]
pub struct SnowflakeSink {
    pub config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
}

impl Sink for SnowflakeSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = LogSinkerOf<SnowflakeSinkWriter>;

    const SINK_NAME: &'static str = SNOWFLAKE_SINK;

    async fn new_log_sinker(&self, writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(SnowflakeSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
        )
        .await
        .into_log_sinker(writer_param.sink_metrics))
    }

    async fn validate(&self) -> Result<()> {
        Ok(())
    }
}

impl TryFrom<SinkParam> for SnowflakeSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let config = SnowflakeConfig::from_hashmap(param.properties)?;
        Ok(SnowflakeSink {
            config,
            schema,
            pk_indices: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
        })
    }
}

pub struct SnowflakeSinkWriter {
    config: SnowflakeConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    /// the client used to send `insertFiles` post request
    http_client: SnowflakeHttpClient,
    /// the client to insert file to external storage (i.e., s3)
    s3_client: SnowflakeS3Client,
    row_encoder: JsonEncoder,
    counter: u32,
    payload: String,
}

impl SnowflakeSinkWriter {
    pub async fn new(
        config: SnowflakeConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
    ) -> Self {
        let http_client = SnowflakeHttpClient::new(
            config.common.account_identifier.clone(),
            config.common.user.clone(),
            config.common.database.clone(),
            config.common.schema.clone(),
            config.common.pipe.clone(),
            config.common.rsa_public_key_fp.clone(),
            config.common.private_key.clone(),
            HashMap::new(),
        );

        let s3_client = SnowflakeS3Client::new(config.common.s3_bucket.clone()).await;

        Self {
            config,
            schema: schema.clone(),
            pk_indices,
            is_append_only,
            http_client,
            s3_client,
            row_encoder: JsonEncoder::new(
                schema,
                None,
                super::encoder::DateHandlingMode::String,
                TimestampHandlingMode::String,
                TimestamptzHandlingMode::UtcString,
                TimeHandlingMode::String,
            ),
            counter: 0,
            payload: String::new(),
        }
    }

    fn reset(&mut self) {
        self.payload.clear();
        self.counter = 0;
    }

    async fn append_only(&mut self, chunk: StreamChunk) -> Result<()> {
        for (op, row) in chunk.rows() {
            if op != Op::Insert {
                continue;
            }
            let row_json_string = Value::Object(self.row_encoder.encode(row)?).to_string();
            self.payload.push_str(&row_json_string);
        }
        self.s3_client.sink_to_s3(self.payload.clone().into()).await?;
        self.http_client.send_request().await?;
        self.reset();
        Ok(())
    }
}

#[async_trait]
impl SinkWriter for SnowflakeSinkWriter {
    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }

    async fn update_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) -> Result<()> {
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<Self::CommitMetadata> {
        Ok(())
    }

    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        self.append_only(chunk).await
    }
}
