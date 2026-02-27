// Copyright 2026 RisingWave Labs
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

use std::collections::BTreeMap;

use anyhow::{Context, anyhow};
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use futures::{FutureExt, TryFuture};
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use serde::Deserialize;
use serde_with::serde_as;
use with_options::WithOptions;

use super::SinkParam;
use super::catalog::SinkFormatDesc;
use crate::connector_common::SqsCommon;
use crate::dispatch_sink_formatter_str_key_impl;
use crate::enforce_secret::EnforceSecret;
use crate::sink::formatter::SinkFormatterImpl;
use crate::sink::log_store::DeliveryFutureManagerAddFuture;
use crate::sink::writer::{
    AsyncTruncateLogSinkerOf, AsyncTruncateSinkWriter, AsyncTruncateSinkWriterExt, FormattedSink,
};
use crate::sink::{Result, Sink, SinkError, SinkWriterParam};

pub const SQS_SINK: &str = "sqs";

#[derive(Clone, Debug)]
pub struct SqsSink {
    pub config: SqsSinkConfig,
    schema: Schema,
    pk_indices: Vec<usize>,
    format_desc: SinkFormatDesc,
    db_name: String,
    sink_from_name: String,
}

impl EnforceSecret for SqsSink {
    fn enforce_secret<'a>(
        prop_iter: impl Iterator<Item = &'a str>,
    ) -> crate::error::ConnectorResult<()> {
        for prop in prop_iter {
            SqsSinkConfig::enforce_one(prop)?;
        }
        Ok(())
    }
}

impl TryFrom<SinkParam> for SqsSink {
    type Error = SinkError;

    fn try_from(param: SinkParam) -> std::result::Result<Self, Self::Error> {
        let schema = param.schema();
        let pk_indices = param.downstream_pk_or_empty();
        let config = SqsSinkConfig::from_btreemap(param.properties)?;
        Ok(Self {
            config,
            schema,
            pk_indices,
            format_desc: param
                .format_desc
                .ok_or_else(|| SinkError::Config(anyhow!("missing FORMAT ... ENCODE ...")))?,
            db_name: param.db_name,
            sink_from_name: param.sink_from_name,
        })
    }
}

const SQS_SINK_MAX_PENDING_CHUNK_NUM: usize = 64;

impl Sink for SqsSink {
    type LogSinker = AsyncTruncateLogSinkerOf<SqsSinkWriter>;

    const SINK_NAME: &'static str = SQS_SINK;

    async fn validate(&self) -> Result<()> {
        // Check for formatter constructor error, before it is too late for error reporting.
        SinkFormatterImpl::new(
            &self.format_desc,
            self.schema.clone(),
            self.pk_indices.clone(),
            self.db_name.clone(),
            self.sink_from_name.clone(),
            &self.config.common.queue_url,
        )
        .await?;

        // Check reachability by getting queue attributes
        let client = self.config.common.build_client().await?;
        client
            .get_queue_attributes()
            .queue_url(&self.config.common.queue_url)
            .send()
            .await
            .context("failed to get queue attributes")
            .map_err(SinkError::Sqs)?;
        Ok(())
    }

    async fn new_log_sinker(&self, _writer_param: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(SqsSinkWriter::new(
            self.config.clone(),
            self.schema.clone(),
            self.pk_indices.clone(),
            &self.format_desc,
            self.db_name.clone(),
            self.sink_from_name.clone(),
        )
        .await?
        .into_log_sinker(SQS_SINK_MAX_PENDING_CHUNK_NUM))
    }
}

#[serde_as]
#[derive(Clone, Debug, Deserialize, WithOptions)]
pub struct SqsSinkConfig {
    #[serde(flatten)]
    pub common: SqsCommon,
}

impl EnforceSecret for SqsSinkConfig {
    fn enforce_one(prop: &str) -> crate::error::ConnectorResult<()> {
        SqsCommon::enforce_one(prop)?;
        Ok(())
    }
}

impl SqsSinkConfig {
    pub fn from_btreemap(properties: BTreeMap<String, String>) -> Result<Self> {
        let config =
            serde_json::from_value::<SqsSinkConfig>(serde_json::to_value(properties).unwrap())
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
        Ok(config)
    }
}

pub struct SqsSinkWriter {
    pub config: SqsSinkConfig,
    formatter: SinkFormatterImpl,
    client: SqsClient,
}

struct SqsSinkPayloadWriter {
    client: SqsClient,
    entries: Vec<SendMessageBatchRequestEntry>,
    queue_url: String,
    message_group_id: Option<String>,
    message_count: usize,
}

impl SqsSinkWriter {
    pub async fn new(
        config: SqsSinkConfig,
        schema: Schema,
        pk_indices: Vec<usize>,
        format_desc: &SinkFormatDesc,
        db_name: String,
        sink_from_name: String,
    ) -> Result<Self> {
        let formatter = SinkFormatterImpl::new(
            format_desc,
            schema,
            pk_indices,
            db_name,
            sink_from_name,
            &config.common.queue_url,
        )
        .await?;
        let client = config
            .common
            .build_client()
            .await
            .map_err(|err| SinkError::Sqs(anyhow!(err)))?;
        Ok(Self {
            config: config.clone(),
            formatter,
            client,
        })
    }

    fn new_payload_writer(&self) -> SqsSinkPayloadWriter {
        SqsSinkPayloadWriter {
            client: self.client.clone(),
            entries: vec![],
            queue_url: self.config.common.queue_url.clone(),
            message_group_id: self.config.common.message_group_id.clone(),
            message_count: 0,
        }
    }
}

mod opaque_type {
    use std::cmp::min;
    use std::time::Duration;

    use thiserror_ext::AsReport;
    use tokio::time::sleep;
    use tokio_retry::strategy::{ExponentialBackoff, jitter};
    use tracing::warn;

    use super::*;

    pub type SqsSinkPayloadWriterDeliveryFuture =
        impl TryFuture<Ok = (), Error = SinkError> + Unpin + Send + 'static;

    impl SqsSinkPayloadWriter {
        #[define_opaque(SqsSinkPayloadWriterDeliveryFuture)]
        pub(super) fn finish(self) -> SqsSinkPayloadWriterDeliveryFuture {
            async move {
                // SQS SendMessageBatch supports up to 10 messages per request
                // Maximum message size is 256 KB
                // Maximum total payload size is 256 KB * 10 = 2.56 MB (but typically limited by individual message size)
                const MAX_BATCH_SIZE: usize = 10;
                const MAX_NO_PROGRESS_RETRY_COUNT: usize = 3;

                let mut remaining_no_progress_retry_count = MAX_NO_PROGRESS_RETRY_COUNT;
                let total_count = self.entries.len();
                let mut start_idx = 0;

                let mut throttle_delay = None;

                while start_idx < total_count {
                    let batch_size = min(MAX_BATCH_SIZE, total_count - start_idx);
                    let batch_entries: Vec<_> = self.entries[start_idx..start_idx + batch_size]
                        .iter()
                        .cloned()
                        .collect();

                    match self
                        .client
                        .send_message_batch()
                        .queue_url(&self.queue_url)
                        .set_entries(Some(batch_entries.clone()))
                        .send()
                        .await
                    {
                        Ok(output) => {
                            let failed_count = output.failed().len();
                            let successful_count = output.successful().len();

                            if failed_count > 0 {
                                let first_failure = &output.failed()[0];
                                let error_code = first_failure.code();
                                let error_message = first_failure.message().unwrap_or("unknown");

                                if successful_count > 0 {
                                    warn!(
                                        successful_count,
                                        failed_count,
                                        "SQS batch partially sent. code: [{}], message: [{}]",
                                        error_code,
                                        error_message
                                    );
                                    start_idx += successful_count;
                                    remaining_no_progress_retry_count = MAX_NO_PROGRESS_RETRY_COUNT;
                                } else if error_code == "Throttling"
                                    || error_code == "ServiceUnavailable"
                                {
                                    let throttle_delay = throttle_delay
                                        .get_or_insert_with(|| {
                                            ExponentialBackoff::from_millis(100)
                                                .factor(2)
                                                .max_delay(Duration::from_secs(2))
                                                .map(jitter)
                                        })
                                        .next()
                                        .expect("should not be none");
                                    warn!(?error_message, ?throttle_delay, "SQS throttling");
                                    sleep(throttle_delay).await;
                                } else {
                                    remaining_no_progress_retry_count -= 1;
                                    if remaining_no_progress_retry_count == 0 {
                                        return Err(SinkError::Sqs(anyhow!(
                                            "failed to send SQS messages. sent {} out of {}, last err: code: [{}], message: [{}]",
                                            start_idx,
                                            total_count,
                                            error_code,
                                            error_message
                                        )));
                                    } else {
                                        warn!(
                                            remaining_no_progress_retry_count,
                                            sent = start_idx,
                                            total_count,
                                            "failed to send SQS messages. code: [{}], message: [{}]",
                                            error_code,
                                            error_message
                                        );
                                    }
                                }
                            } else {
                                start_idx += batch_size;
                                remaining_no_progress_retry_count = MAX_NO_PROGRESS_RETRY_COUNT;
                                throttle_delay = None;
                            }
                        }
                        Err(e) => {
                            remaining_no_progress_retry_count -= 1;
                            if remaining_no_progress_retry_count == 0 {
                                return Err(SinkError::Sqs(anyhow!(e).context(format!(
                                    "failed to send SQS messages. sent {} out of {}",
                                    start_idx, total_count,
                                ))));
                            } else {
                                warn!(
                                    remaining_no_progress_retry_count,
                                    sent = start_idx,
                                    total_count,
                                    "failed to send SQS messages. err: [{:?}]",
                                    e.as_report(),
                                )
                            }
                        }
                    }
                }
                Ok(())
            }
            .boxed()
        }
    }
}

pub use opaque_type::SqsSinkPayloadWriterDeliveryFuture;

impl SqsSinkPayloadWriter {
    fn put_message(&mut self, key: Option<String>, payload: Vec<u8>) {
        self.message_count += 1;
        let message_body =
            String::from_utf8(payload).unwrap_or_else(|e| BASE64_STANDARD.encode(e.into_bytes()));

        let mut entry = SendMessageBatchRequestEntry::builder()
            .id(self.message_count.to_string())
            .message_body(message_body);

        // For FIFO queues, set message group ID and deduplication ID
        if let Some(ref group_id) = self.message_group_id {
            entry = entry.message_group_id(group_id);
            // Use key as deduplication ID if available, otherwise use message count + timestamp
            let dedup_id = key.unwrap_or_else(|| {
                format!(
                    "{}-{}",
                    self.message_count,
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos()
                )
            });
            entry = entry.message_deduplication_id(dedup_id);
        }

        self.entries.push(
            entry
                .build()
                .expect("should not fail because we have set required fields"),
        );
    }
}

impl FormattedSink for SqsSinkPayloadWriter {
    type K = String;
    type V = Vec<u8>;

    async fn write_one(&mut self, k: Option<Self::K>, v: Option<Self::V>) -> Result<()> {
        self.put_message(k, v.unwrap_or_default());
        Ok(())
    }
}

impl AsyncTruncateSinkWriter for SqsSinkWriter {
    type DeliveryFuture = SqsSinkPayloadWriterDeliveryFuture;

    async fn write_chunk<'a>(
        &'a mut self,
        chunk: StreamChunk,
        mut add_future: DeliveryFutureManagerAddFuture<'a, Self::DeliveryFuture>,
    ) -> Result<()> {
        let mut payload_writer = self.new_payload_writer();
        dispatch_sink_formatter_str_key_impl!(
            &self.formatter,
            formatter,
            payload_writer.write_chunk(chunk, formatter).await
        )?;

        add_future
            .add_future_may_await(payload_writer.finish())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_sqs::types::SendMessageBatchRequestEntry;

    #[test]
    fn test_sqs_config_parse() {
        let mut properties = std::collections::BTreeMap::new();
        properties.insert(
            "sqs.queue_url".to_string(),
            "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue".to_string(),
        );
        properties.insert("sqs.region".to_string(), "us-east-1".to_string());
        properties.insert("sqs.credentials.access".to_string(), "access".to_string());
        properties.insert("sqs.credentials.secret".to_string(), "secret".to_string());
        properties.insert("sqs.message_group_id".to_string(), "group1".to_string());

        let config = super::SqsSinkConfig::from_btreemap(properties).unwrap();
        assert_eq!(
            config.common.queue_url,
            "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
        );
        assert_eq!(config.common.region, "us-east-1");
        assert_eq!(config.common.credentials_access_key.unwrap(), "access");
        assert_eq!(
            config.common.credentials_secret_access_key.unwrap(),
            "secret"
        );
        assert_eq!(config.common.message_group_id.unwrap(), "group1");
    }

    #[test]
    fn test_sqs_entry_builder() {
        SendMessageBatchRequestEntry::builder()
            .id("1")
            .message_body("test message")
            .build()
            .unwrap();
    }

    #[test]
    fn test_sqs_fifo_entry_builder() {
        SendMessageBatchRequestEntry::builder()
            .id("1")
            .message_body("test message")
            .message_group_id("group1")
            .message_deduplication_id("dedup1")
            .build()
            .unwrap();
    }
}
