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

use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use aws_sdk_kinesis::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_kinesis::operation::get_records::{GetRecordsError, GetRecordsOutput};
use aws_sdk_kinesis::primitives::DateTime;
use aws_sdk_kinesis::types::ShardIteratorType;
use aws_sdk_kinesis::Client as KinesisClient;
use futures_async_stream::try_stream;
use risingwave_common::bail;
use thiserror_ext::AsReport;

use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::kinesis::source::message::from_kinesis_record;
use crate::source::kinesis::split::{KinesisOffset, KinesisSplit};
use crate::source::kinesis::KinesisProperties;
use crate::source::{
    into_chunk_stream, BoxChunkSourceStream, Column, SourceContextRef, SourceMessage, SplitId,
    SplitMetaData, SplitReader,
};

#[derive(Debug, Clone)]
pub struct KinesisSplitReader {
    client: KinesisClient,
    stream_name: String,
    shard_id: SplitId,
    latest_offset: Option<String>,
    shard_iter: Option<String>,
    next_offset: KinesisOffset,
    #[expect(dead_code)]
    end_offset: KinesisOffset,

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

        let next_offset = match &split.next_offset {
            KinesisOffset::None => match &properties.scan_startup_mode {
                None => KinesisOffset::Earliest,
                Some(mode) => match mode.as_str() {
                    "earliest" => KinesisOffset::Earliest,
                    "latest" => KinesisOffset::Latest,
                    "timestamp" => {
                        if let Some(ts) = &properties.timestamp_offset {
                            KinesisOffset::Timestamp(*ts)
                        } else {
                            bail!("scan.startup.timestamp.millis is required");
                        }
                    }
                    _ => {
                        bail!("invalid scan_startup_mode, accept earliest/latest/timestamp")
                    }
                },
            },
            next_offset => next_offset.to_owned(),
        };

        if !matches!(next_offset, KinesisOffset::Timestamp(_))
            && properties.timestamp_offset.is_some()
        {
            // cannot bail! here because all new split readers will fail to start if user set 'scan.startup.mode' to 'timestamp'
            tracing::warn!("scan.startup.mode needs to be set to 'timestamp' if you want to start with a specific timestamp, starting shard {} from the beginning",
                split.id()
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
            next_offset,
            end_offset: split.end_offset,
            split_id,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxChunkSourceStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl KinesisSplitReader {
    #[try_stream(ok = Vec < SourceMessage >, error = crate::error::ConnectorError)]
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
                    tracing::trace!(?self.shard_id, ?resp);
                    self.shard_iter = resp.next_shard_iterator().map(String::from);
                    let chunk = (resp.records().iter())
                        .map(|r| from_kinesis_record(r, self.split_id.clone()))
                        .collect::<Vec<SourceMessage>>();
                    if let Some(shard) = &resp.child_shards
                        && !shard.is_empty()
                    {
                        // according to the doc https://docs.rs/aws-sdk-kinesis/latest/aws_sdk_kinesis/operation/get_records/struct.GetRecordsOutput.html
                        //
                        // > The list of the current shard's child shards, returned in the GetRecords API's response only when the end of the current shard is reached.

                        // The response will be like:
                        // {
                        //     "Records": [], // empty
                        //     "MillisBehindLatest": 2665000, // non-zero
                        //     "ChildShards": [...] // non-empty
                        //     // no NextShardIterator
                        // }

                        // Other executors are going to read the child shards.

                        if !chunk.is_empty() {
                            // This should not happen. But be extra safe here.
                            yield chunk;
                        }

                        tracing::info!(
                            "shard {:?} reaches the end and is inactive, stop reading",
                            self.shard_id
                        );
                        break;
                    }
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
                Err(e) if e.code() == Some("InternalFailure") => {
                    tracing::warn!(
                        "stream {:?} shard {:?} met internal failure, retrying",
                        self.stream_name,
                        self.shard_id
                    );
                    self.new_shard_iter().await?;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Err(e) => {
                    let error = anyhow!(e).context(format!(
                        "Kinesis got an unhandled error on stream {:?}, shard {:?}",
                        self.stream_name, self.shard_id
                    ));
                    tracing::error!(error = %error.as_report());
                    return Err(error.into());
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
            match &self.next_offset {
                KinesisOffset::Earliest => (None, None, ShardIteratorType::TrimHorizon),
                KinesisOffset::AfterSequenceNumber(seq) => (
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
                .context("failed to get kinesis shard iterator")?;

            if let Some(iter) = resp.shard_iterator() {
                Ok(iter.to_owned())
            } else {
                bail!("shard iterator is none")
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
    use crate::connector_common::KinesisCommon;
    use crate::source::SourceContext;

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

            unknown_fields: Default::default(),
        };

        let trim_horizen_reader = KinesisSplitReader::new(
            properties.clone(),
            vec![KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                next_offset: KinesisOffset::Earliest,
                end_offset: KinesisOffset::None,
            }],
            Default::default(),
            SourceContext::dummy().into(),
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
                next_offset: KinesisOffset::AfterSequenceNumber(
                    "49629139817504901062972448413535783695568426186596941842".to_string(),
                ),
                end_offset: KinesisOffset::None,
            }],
            Default::default(),
            SourceContext::dummy().into(),
            None,
        )
        .await?
        .into_data_stream();
        pin_mut!(offset_reader);
        println!("{:?}", offset_reader.next().await.unwrap()?);

        Ok(())
    }
}
