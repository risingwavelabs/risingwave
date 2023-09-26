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

use anyhow::anyhow;
use aws_sdk_kinesis::error::DisplayErrorContext;
use aws_sdk_kinesis::operation::put_record::PutRecordOutput;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::Client as KinesisClient;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

use super::{FormattedSink, SinkParam};
use crate::common::KinesisCommon;
use crate::dispatch_sink_formatter_impl;
use crate::sink::formatter::SinkFormatterImpl;
use crate::sink::{
    DummySinkCommitCoordinator, Result, Sink, SinkError, SinkWriter, SinkWriterParam,
    SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM, SINK_TYPE_OPTION, SINK_TYPE_UPSERT,
};

pub const KINESIS_SINK: &str = "kinesis";

#[derive(Clone, Debug)]
pub struct KinesisSink {
    pub config: KinesisSinkConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    is_append_only: bool,
    db_name: String,
    sink_from_name: String,
}

impl KinesisSink {
    pub fn new(config: KinesisSinkConfig, param: SinkParam) -> Self {
        Self {
            config,
            schema: param.schema(),
            pk_indices: param.downstream_pk,
            is_append_only: param.sink_type.is_append_only(),
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        }
    }
}

#[async_trait::async_trait]
impl Sink for KinesisSink {
    type Coordinator = DummySinkCommitCoordinator;
    type Writer = KinesisSinkWriter;

    async fn validate(&self) -> Result<()> {
        // For upsert Kafka sink, the primary key must be defined.
        if !self.is_append_only && self.pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {} kafka sink (please define in `primary_key` field)",
                self.config.r#type
            )));
        }

        // check reachability
        let client = self.config.common.build_client().await?;
        client
            .list_shards()
            .stream_name(&self.config.common.stream_name)
            .send()
            .await
            .map_err(|e| {
                tracing::warn!("failed to list shards: {}", DisplayErrorContext(&e));
                SinkError::Kinesis(anyhow!("failed to list shards: {}", DisplayErrorContext(e)))
            })?;
        Ok(())
    }

    async fn new_writer(&self, _writer_env: SinkWriterParam) -> Result<Self::Writer> {
        KinesisSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            self.is_append_only,
            self.db_name.clone(),
            self.sink_from_name.clone(),
        )
        .await
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct KinesisSinkConfig {
    #[serde(flatten)]
    pub common: KinesisCommon,

    pub r#type: String, // accept "append-only", "debezium", or "upsert"
}

impl KinesisSinkConfig {
    pub fn from_hashmap(properties: HashMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<KinesisSinkConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        if config.r#type != SINK_TYPE_APPEND_ONLY
            && config.r#type != SINK_TYPE_DEBEZIUM
            && config.r#type != SINK_TYPE_UPSERT
        {
            return Err(SinkError::Config(anyhow!(
                "`{}` must be {}, {}, or {}",
                SINK_TYPE_OPTION,
                SINK_TYPE_APPEND_ONLY,
                SINK_TYPE_DEBEZIUM,
                SINK_TYPE_UPSERT
            )));
        }
        Ok(config)
    }
}

pub struct KinesisSinkWriter {
    pub config: KinesisSinkConfig,
    formatter: SinkFormatterImpl,
    payload_writer: KinesisSinkPayloadWriter,
}

struct KinesisSinkPayloadWriter {
    client: KinesisClient,
    config: KinesisSinkConfig,
}

impl KinesisSinkWriter {
    pub async fn new(
        config: KinesisSinkConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        is_append_only: bool,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        let formatter = SinkFormatterImpl::new(
            &config.r#type,
            schema,
            pk_indices,
            is_append_only,
            db_name,
            sink_from_name,
        )?;
        let client = config
            .common
            .build_client()
            .await
            .map_err(SinkError::Kinesis)?;
        Ok(Self {
            config: config.clone(),
            formatter,
            payload_writer: KinesisSinkPayloadWriter { client, config },
        })
    }
}
impl KinesisSinkPayloadWriter {
    async fn put_record(&self, key: &str, payload: Vec<u8>) -> Result<PutRecordOutput> {
        let payload = Blob::new(payload);
        // todo: switch to put_records() for batching
        Retry::spawn(
            ExponentialBackoff::from_millis(100).map(jitter).take(3),
            || async {
                self.client
                    .put_record()
                    .stream_name(&self.config.common.stream_name)
                    .partition_key(key)
                    .data(payload.clone())
                    .send()
                    .await
            },
        )
        .await
        .map_err(|e| {
            tracing::warn!(
                "failed to put record: {} to {}",
                DisplayErrorContext(&e),
                self.config.common.stream_name
            );
            SinkError::Kinesis(anyhow!(
                "failed to put record: {} to {}",
                DisplayErrorContext(e),
                self.config.common.stream_name
            ))
        })
    }
}

impl FormattedSink for KinesisSinkPayloadWriter {
    type K = String;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        self.put_record(&k.unwrap(), v.unwrap_or_default())
            .await
            .map(|_| ())
    }
}

#[async_trait::async_trait]
impl SinkWriter for KinesisSinkWriter {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        dispatch_sink_formatter_impl!(&self.formatter, formatter, {
            self.payload_writer.write_chunk(chunk, formatter).await
        })
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        // Kinesis offers no transactional guarantees, so we do nothing here.
        Ok(())
    }

    async fn barrier(&mut self, _is_checkpoint: bool) -> Result<()> {
        Ok(())
    }
}
