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

use std::future::Future;

use anyhow::anyhow;
use aws_sdk_kinesis::error::{DisplayErrorContext, SdkError};
use aws_sdk_kinesis::operation::put_record::{PutRecordError, PutRecordOutput};
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::Client as KinesisClient;
use futures_async_stream::for_await;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde_derive::{Deserialize, Serialize};
use serde_with::json::JsonString;
use serde_with::serde_as;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;

use crate::common::KinesisCommon;
use crate::sink::kafka::KafkaConfig;
use crate::sink::utils::{gen_debezium_message_stream, DebeziumAdapterOpts};
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
            SinkError::Kinesis(anyhow!(
                "failed to put record: {} to {}",
                DisplayErrorContext(e),
                self.config.common.stream_name
            ))
        })
    }

    async fn debezium_update(&self, chunk: StreamChunk, ts_ms: u64) -> Result<()> {
        let dbz_stream = gen_debezium_message_stream(
            &self.schema,
            &self.pk_indices,
            chunk,
            ts_ms,
            DebeziumAdapterOpts::default(),
        );

        #[for_await]
        for msg in dbz_stream {
            let (event_key_object, event_object) = msg?;
            let key_str = event_key_object.unwrap().to_string();
            self.put_record(
                &key_str,
                Blob::new(if let Some(value) = event_object {
                    value.to_string().into_bytes()
                } else {
                    vec![]
                }),
            )
            .await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl<const APPEND_ONLY: bool> Sink for KinesisSink<APPEND_ONLY> {
    async fn write_batch(&mut self, chunk: StreamChunk) -> Result<()> {
        if APPEND_ONLY {
            todo!()
        } else {
            if self.config.r#type == SINK_TYPE_DEBEZIUM {
                todo!()
            } else if self.config.r#type == SINK_TYPE_UPSERT {
                // upsert
                todo!()
            } else {
                unreachable!()
            }
        }
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<()> {
        todo!()
    }

    async fn commit(&mut self) -> Result<()> {
        todo!()
    }

    async fn abort(&mut self) -> Result<()> {
        todo!()
    }
}
