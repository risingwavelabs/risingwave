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

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;
use aws_sdk_kinesis::error::DisplayErrorContext;
use aws_sdk_kinesis::operation::put_record::PutRecordOutput;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::Client as KinesisClient;
use futures_async_stream::for_await;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde_derive::Deserialize;
use serde_with::serde_as;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

use crate::common::KinesisCommon;
use crate::sink::utils::{
    gen_append_only_message_stream, gen_debezium_message_stream, gen_upsert_message_stream,
    AppendOnlyAdapterOpts, DebeziumAdapterOpts, UpsertAdapterOpts,
};
use crate::sink::{Result, Sink, SinkError, SINK_TYPE_DEBEZIUM, SINK_TYPE_UPSERT};

#[derive(Clone, Debug)]
pub struct KinesisSink<const APPEND_ONLY: bool> {
    pub config: KinesisSinkConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    client: KinesisClient,
}

#[serde_as]
#[derive(Clone, Debug, Deserialize)]
pub struct KinesisSinkConfig {
    #[serde(flatten)]
    pub common: KinesisCommon,

    pub r#type: String, // accept "append-only", "debezium", or "upsert"
}

impl<const APPEND_ONLY: bool> KinesisSink<APPEND_ONLY> {
    pub async fn new(
        config: KinesisSinkConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
    ) -> Result<Self> {
        let client = config
            .common
            .build_client()
            .await
            .map_err(SinkError::Kinesis)?;
        Ok(Self {
            config,
            schema,
            pk_indices,
            client,
        })
    }

    pub async fn validate(config: KinesisSinkConfig, pk_indices: Vec<usize>) -> Result<()> {
        // For upsert Kafka sink, the primary key must be defined.
        if !APPEND_ONLY && pk_indices.is_empty() {
            return Err(SinkError::Config(anyhow!(
                "primary key not defined for {} kafka sink (please define in `primary_key` field)",
                config.r#type
            )));
        }

        // check reachability
        let client = config.common.build_client().await?;
        client
            .list_shards()
            .stream_name(&config.common.stream_name)
            .send()
            .await
            .map_err(|e| {
                tracing::warn!("failed to list shards: {}", DisplayErrorContext(&e));
                SinkError::Kinesis(anyhow!("failed to list shards: {}", DisplayErrorContext(e)))
            })?;
        Ok(())
    }

    async fn put_record(&self, key: &str, payload: Blob) -> Result<PutRecordOutput> {
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

    async fn upsert(&self, chunk: StreamChunk) -> Result<()> {
        let upsert_stream = gen_upsert_message_stream(
            &self.schema,
            &self.pk_indices,
            chunk,
            UpsertAdapterOpts::default(),
        );

        crate::impl_load_stream_write_record!(upsert_stream, self.put_record);
        Ok(())
    }

    async fn append_only(&self, chunk: StreamChunk) -> Result<()> {
        let append_only_stream = gen_append_only_message_stream(
            &self.schema,
            &self.pk_indices,
            chunk,
            AppendOnlyAdapterOpts::default(),
        );

        crate::impl_load_stream_write_record!(append_only_stream, self.put_record);
        Ok(())
    }

    async fn debezium_update(&self, chunk: StreamChunk, ts_ms: u64) -> Result<()> {
        let dbz_stream = gen_debezium_message_stream(
            &self.schema,
            &self.pk_indices,
            chunk,
            ts_ms,
            DebeziumAdapterOpts::default(),
        );

        crate::impl_load_stream_write_record!(dbz_stream, self.put_record);

        Ok(())
    }
}

#[async_trait::async_trait]
impl<const APPEND_ONLY: bool> Sink for KinesisSink<APPEND_ONLY> {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if APPEND_ONLY {
            self.append_only(chunk).await
        } else if self.config.r#type == SINK_TYPE_DEBEZIUM {
            self.debezium_update(
                chunk,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            )
            .await
        } else if self.config.r#type == SINK_TYPE_UPSERT {
            self.upsert(chunk).await
        } else {
            unreachable!()
        }
    }

    async fn begin_epoch(&mut self, _epoch: u64) -> Result<()> {
        // Kinesis offers no transactional guarantees, so we do nothing here.
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

#[macro_export]
macro_rules! impl_load_stream_write_record {
    ($stream:ident, $op_fn:stmt) => {
        #[for_await]
        for msg in $stream {
            let (event_key_object, event_object) = msg?;
            let key_str = event_key_object.unwrap().to_string();
            $op_fn(
                &key_str,
                Blob::new(if let Some(value) = event_object {
                    value.to_string().into_bytes()
                } else {
                    vec![]
                }),
            )
            .await?;
        }
    };
}
