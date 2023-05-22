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
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::model::ShardIteratorType;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::types::SdkError;
use aws_sdk_kinesis::Client as KinesisClient;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use tokio_retry;

use crate::impl_common_split_reader_logic;
use crate::parser::ParserConfig;
use crate::source::kinesis::source::message::KinesisMessage;
use crate::source::kinesis::split::KinesisOffset;
use crate::source::kinesis::KinesisProperties;
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SourceMessage, SplitId, SplitImpl,
    SplitMetaData, SplitReader,
};

impl_common_split_reader_logic!(KinesisSplitReader, KinesisProperties);

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

    async fn new(
        properties: KinesisProperties,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        assert!(splits.len() == 1);

        let split = splits.into_iter().next().unwrap().into_kinesis().unwrap();

        let start_position = match &split.start_position {
            KinesisOffset::None => match &properties.scan_startup_mode {
                None => KinesisOffset::Earliest,
                Some(mode) => match mode.as_str() {
                    "earliest" => KinesisOffset::Earliest,
                    "latest" => KinesisOffset::Latest,
                    "sequence_number" => {
                        if let Some(seq) = &properties.seq_offset {
                            KinesisOffset::SequenceNumber(seq.clone())
                        } else {
                            return Err(anyhow!("scan_startup_sequence_number is required"));
                        }
                    }
                    _ => {
                        return Err(anyhow!(
                            "invalid scan_startup_mode, accept earliest/latest/sequence_number"
                        ))
                    }
                },
            },
            start_position => start_position.to_owned(),
        };

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
        self.into_chunk_stream()
    }
}

impl KinesisSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub(crate) async fn into_data_stream(mut self) {
        self.new_shard_iter().await?;
        loop {
            if self.shard_iter.is_none() {
                tracing::warn!("shard iterator is none unexpectedly, renew it");
                self.new_shard_iter().await?;
            }
            match self.get_records().await {
                Ok(resp) => {
                    self.shard_iter = resp.next_shard_iterator().map(String::from);
                    let chunk = (resp.records().unwrap().iter())
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
                Err(SdkError::ServiceError { err, .. }) if err.is_expired_iterator_exception() => {
                    tracing::warn!(
                        "stream {:?} shard {:?} iterator expired, renew it",
                        self.stream_name,
                        self.shard_id
                    );
                    self.new_shard_iter().await?;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                Err(SdkError::ServiceError { err, .. })
                    if err.is_provisioned_throughput_exceeded_exception() =>
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
                Err(e) => return Err(anyhow!(e)),
            }
        }
    }

    async fn new_shard_iter(&mut self) -> Result<()> {
        let (starting_seq_num, iter_type) = if self.latest_offset.is_some() {
            (
                self.latest_offset.clone(),
                ShardIteratorType::AfterSequenceNumber,
            )
        } else {
            match &self.start_position {
                KinesisOffset::Earliest => (None, ShardIteratorType::TrimHorizon),
                KinesisOffset::SequenceNumber(seq) => {
                    (Some(seq.clone()), ShardIteratorType::AfterSequenceNumber)
                }
                KinesisOffset::Latest => (None, ShardIteratorType::Latest),
                _ => unreachable!(),
            }
        };

        async fn get_shard_iter_inner(
            client: &KinesisClient,
            stream_name: &str,
            shard_id: &str,
            starting_seq_num: Option<String>,
            iter_type: ShardIteratorType,
        ) -> Result<String> {
            let resp = client
                .get_shard_iterator()
                .stream_name(stream_name)
                .shard_id(shard_id)
                .shard_iterator_type(iter_type)
                .set_starting_sequence_number(starting_seq_num)
                .send()
                .await?;

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
            .set_shard_iterator(self.shard_iter.take())
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::common::KinesisCommon;
    use crate::source::kinesis::split::KinesisSplit;

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
            seq_offset: None,
            enable_split_reduction: None,
        };

        let mut trim_horizen_reader = KinesisSplitReader::new(
            properties.clone(),
            vec![SplitImpl::Kinesis(KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::Earliest,
                end_position: KinesisOffset::None,
            })],
            Default::default(),
            Default::default(),
            None,
        )
        .await?
        .into_data_stream();
        println!("{:?}", trim_horizen_reader.next().await.unwrap()?);

        let mut offset_reader = KinesisSplitReader::new(
            properties.clone(),
            vec![SplitImpl::Kinesis(KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::SequenceNumber(
                    "49629139817504901062972448413535783695568426186596941842".to_string(),
                ),
                end_position: KinesisOffset::None,
            })],
            Default::default(),
            Default::default(),
            None,
        )
        .await?
        .into_data_stream();
        println!("{:?}", offset_reader.next().await.unwrap()?);

        Ok(())
    }
}
