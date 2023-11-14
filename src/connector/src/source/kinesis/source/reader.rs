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

use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_kinesis::error::{DisplayErrorContext, SdkError};
use aws_sdk_kinesis::operation::get_records::{GetRecordsError, GetRecordsOutput};
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::types::ShardIteratorType;
use aws_sdk_kinesis::Client as KinesisClient;
use futures_async_stream::try_stream;
use tokio_retry;

use crate::parser::ParserConfig;
use crate::source::kinesis::source::message::KinesisMessage;
use crate::source::kinesis::split::{KinesisOffset, KinesisSplit};
use crate::source::kinesis::KinesisProperties;
use crate::source::{
    into_chunk_stream, BoxSourceWithStateStream, Column, CommonSplitReader, SourceContextRef,
    SourceMessage, SplitId, SplitMetaData, SplitReader,
};

#[derive(Debug, Clone)]
pub struct KinesisSplitReader {
    client: KinesisClient,
    stream_name: String,
    shard_id: SplitId,
    latest_offset: Option<String>,
    shard_iter: Option<String>,
    start_position: KinesisOffset,
    end_position: KinesisOffset,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for KinesisSplitReader {
    type Properties = KinesisProperties;
    type Split = KinesisSplit;

    async fn new(
        properties: KinesisProperties,
        splits: Vec<KinesisSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        assert!(splits.len() == 1);

        let split = splits.into_iter().next().unwrap();

        let start_position = match &split.start_position {
            KinesisOffset::None => match &properties.scan_startup_mode {
                None => KinesisOffset::Earliest,
                Some(mode) => match mode.as_str() {
                    "earliest" => KinesisOffset::Earliest,
                    "latest" => KinesisOffset::Latest,
                    "timestamp" => {
                        if let Some(ts) = &properties.timestamp_offset {
                            KinesisOffset::Timestamp(*ts)
                        } else {
                            return Err(anyhow!("scan.startup.timestamp.millis is required"));
                        }
                    }
                    _ => {
                        return Err(anyhow!(
                            "invalid scan_startup_mode, accept earliest/latest/timestamp"
                        ))
                    }
                },
            },
            start_position => start_position.to_owned(),
        };

        if !matches!(start_position, KinesisOffset::Timestamp(_))
            && properties.timestamp_offset.is_some()
        {
            return Err(
                anyhow!("scan.startup.mode need to be set to 'timestamp' if you want to start with a specific timestamp")
            );
        }

        let stream_name = properties.common.stream_name.clone();
        let client = properties.common.build_client().await?;

        let split_id = split.id();
        Ok(Self {
            client,
            stream_name,
            shard_id: split.shard_id,
            shard_iter: None,
            latest_offset: None,
            start_position,
            end_position: split.end_position,
            split_id,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self, parser_config, source_context)
    }
}

impl CommonSplitReader for KinesisSplitReader {
    #[try_stream(ok = Vec < SourceMessage >, error = anyhow::Error)]
    async fn into_data_stream(mut self) {
        self.new_shard_iter().await?;
        loop {
            if self.shard_iter.is_none() {
                tracing::warn!(
                    "shard iterator is none unexpectedly, may reach the end of shard {}, latest seq {}, retrying in one second",
                    self.shard_id,
                    self.latest_offset.as_ref().unwrap_or(&"None".to_string())
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
                self.new_shard_iter().await?;
            }
            match self.get_records().await {
                Ok(resp) => {
                    self.shard_iter = resp.next_shard_iterator().map(String::from);
                    let chunk = (resp.records().iter())
                        .map(|r| {
                            SourceMessage::from(KinesisMessage::new(
                                self.shard_id.clone(),
                                r.clone(),
                            ))
                        })
                        .collect::<Vec<SourceMessage>>();
                    if chunk.is_empty() {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    self.latest_offset = Some(chunk.last().unwrap().offset.clone());
                    tracing::debug!(
                        "shard {:?} latest offset: {:?}",
                        self.shard_id,
                        self.latest_offset
                    );
                    yield chunk;
                }
                Err(SdkError::ServiceError(e)) if e.err().is_resource_not_found_exception() => {
                    tracing::warn!("shard {:?} is closed, stop reading", self.shard_id);
                    break;
                }
                Err(SdkError::ServiceError(e)) if e.err().is_expired_iterator_exception() => {
                    tracing::warn!(
                        "stream {:?} shard {:?} iterator expired, renew it",
                        self.stream_name,
                        self.shard_id
                    );
                    self.new_shard_iter().await?;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Err(SdkError::ServiceError(e))
                    if e.err().is_provisioned_throughput_exceeded_exception() =>
                {
                    tracing::warn!(
                        "stream {:?} shard {:?} throughput exceeded, retry",
                        self.stream_name,
                        self.shard_id
                    );
                    self.new_shard_iter().await?;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Err(SdkError::DispatchFailure(e)) => {
                    tracing::warn!(
                        "stream {:?} shard {:?} dispatch failure: {:?}",
                        self.stream_name,
                        self.shard_id,
                        e
                    );
                    self.new_shard_iter().await?;
                    continue;
                }
                Err(e) => {
                    let error_msg = format!(
                        "Kinesis got a unhandled error: {:?}, stream {:?}, shard {:?}",
                        DisplayErrorContext(e),
                        self.stream_name,
                        self.shard_id,
                    );
                    tracing::error!("{}", error_msg);
                    return Err(anyhow!("{}", error_msg));
                }
            }
        }
    }
}
impl KinesisSplitReader {
    async fn new_shard_iter(&mut self) -> Result<()> {
        let (starting_seq_num, start_timestamp, iter_type) = if self.latest_offset.is_some() {
            (
                self.latest_offset.clone(),
                None,
                ShardIteratorType::AfterSequenceNumber,
            )
        } else {
            match &self.start_position {
                KinesisOffset::Earliest => (None, None, ShardIteratorType::TrimHorizon),
                KinesisOffset::SequenceNumber(seq) => (
                    Some(seq.clone()),
                    None,
                    ShardIteratorType::AfterSequenceNumber,
                ),
                KinesisOffset::Latest => (None, None, ShardIteratorType::Latest),
                KinesisOffset::Timestamp(ts) => (
                    None,
                    Some(DateTime::from_millis(*ts)),
                    ShardIteratorType::AtTimestamp,
                ),
                _ => unreachable!(),
            }
        };

        // `starting_seq_num` and `starting_timestamp` will not be both set
        async fn get_shard_iter_inner(
            client: &KinesisClient,
            stream_name: &str,
            shard_id: &str,
            starting_seq_num: Option<String>,
            starting_timestamp: Option<DateTime>,
            iter_type: ShardIteratorType,
        ) -> Result<String> {
            let resp = client
                .get_shard_iterator()
                .stream_name(stream_name)
                .shard_id(shard_id)
                .shard_iterator_type(iter_type)
                .set_starting_sequence_number(starting_seq_num)
                .set_timestamp(starting_timestamp)
                .send()
                .await
                .map_err(|e| anyhow!(DisplayErrorContext(e)))?;

            if let Some(iter) = resp.shard_iterator() {
                Ok(iter.to_owned())
            } else {
                Err(anyhow!("shard iterator is none"))
            }
        }

        self.shard_iter = Some(
            tokio_retry::Retry::spawn(
                tokio_retry::strategy::ExponentialBackoff::from_millis(100).take(3),
                || {
                    get_shard_iter_inner(
                        &self.client,
                        &self.stream_name,
                        &self.shard_id,
                        starting_seq_num.clone(),
                        start_timestamp,
                        iter_type.clone(),
                    )
                },
            )
            .await?,
        );

        tracing::info!(
            "resetting kinesis to: stream {:?} shard {:?} starting from {:?}",
            self.stream_name,
            self.shard_id,
            starting_seq_num
        );
        Ok(())
    }

    async fn get_records(
        &mut self,
    ) -> core::result::Result<GetRecordsOutput, SdkError<GetRecordsError>> {
        self.client
            .get_records()
            .limit(self.source_ctx.source_ctrl_opts.chunk_size as i32)
            .set_shard_iterator(self.shard_iter.take())
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use futures::{pin_mut, StreamExt};

    use super::*;
    use crate::common::KinesisCommon;
    use crate::source::kinesis::split::KinesisSplit;

    #[tokio::test]
    async fn test_reject_redundant_seq_props() {
        let properties = KinesisProperties {
            common: KinesisCommon {
                assume_role_arn: None,
                credentials_access_key: None,
                credentials_secret_access_key: None,
                stream_name: "kinesis_debug".to_string(),
                stream_region: "cn-northwest-1".to_string(),
                endpoint: None,
                session_token: None,
                assume_role_external_id: None,
            },

            scan_startup_mode: None,
            timestamp_offset: Some(123456789098765432),
        };
        let client = KinesisSplitReader::new(
            properties,
            vec![KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::Earliest,
                end_position: KinesisOffset::None,
            }],
            Default::default(),
            Default::default(),
            None,
        )
        .await;
        assert!(client.is_err());
    }

    #[tokio::test]
    #[ignore]
    async fn test_single_thread_kinesis_reader() -> Result<()> {
        let properties = KinesisProperties {
            common: KinesisCommon {
                assume_role_arn: None,
                credentials_access_key: None,
                credentials_secret_access_key: None,
                stream_name: "kinesis_debug".to_string(),
                stream_region: "cn-northwest-1".to_string(),
                endpoint: None,
                session_token: None,
                assume_role_external_id: None,
            },

            scan_startup_mode: None,
            timestamp_offset: None,
        };

        let trim_horizen_reader = KinesisSplitReader::new(
            properties.clone(),
            vec![KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::Earliest,
                end_position: KinesisOffset::None,
            }],
            Default::default(),
            Default::default(),
            None,
        )
        .await?
        .into_data_stream();
        pin_mut!(trim_horizen_reader);
        println!("{:?}", trim_horizen_reader.next().await.unwrap()?);

        let offset_reader = KinesisSplitReader::new(
            properties.clone(),
            vec![KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::SequenceNumber(
                    "49629139817504901062972448413535783695568426186596941842".to_string(),
                ),
                end_position: KinesisOffset::None,
            }],
            Default::default(),
            Default::default(),
            None,
        )
        .await?
        .into_data_stream();
        pin_mut!(offset_reader);
        println!("{:?}", offset_reader.next().await.unwrap()?);

        Ok(())
    }
}
