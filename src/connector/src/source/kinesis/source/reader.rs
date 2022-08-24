// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use core::result::Result::Ok;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::model::ShardIteratorType;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::types::SdkError;
use aws_sdk_kinesis::Client as KinesisClient;
use futures::future::join_all;
use futures_async_stream::{for_await, try_stream};
use futures_concurrency::prelude::*;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::source::kinesis::source::message::KinesisMessage;
use crate::source::kinesis::split::{KinesisOffset, KinesisSplit};
use crate::source::kinesis::{build_client, KinesisProperties};
use crate::source::{Column, ConnectorState, SourceMessage, SplitId, SplitImpl, SplitReader};

pub struct KinesisMultiSplitReader {
    /// splits are not allowed to be empty, otherwise connector source should create
    /// DummySplitReader which is always idling.
    splits: Vec<KinesisSplit>,
    properties: KinesisProperties,
    message_cache: Arc<Mutex<Vec<SourceMessage>>>,
    consumer_handler: Option<JoinHandle<()>>,
}

impl Drop for KinesisMultiSplitReader {
    fn drop(&mut self) {
        if let Some(handler) = self.consumer_handler.as_mut() {
            handler.abort();
        }
    }
}

#[derive(Debug, Clone)]
pub struct KinesisSplitReader {
    client: KinesisClient,
    stream_name: String,
    shard_id: SplitId,
    latest_offset: Option<String>,
    shard_iter: Option<String>,
    start_position: KinesisOffset,
    end_position: KinesisOffset,
}

impl KinesisSplitReader {
    pub async fn new(properties: KinesisProperties, split: KinesisSplit) -> Result<Self> {
        let stream_name = properties.stream_name.clone();
        let client = build_client(properties).await?;
        Ok(Self {
            client,
            stream_name,
            shard_id: split.shard_id,
            shard_iter: None,
            latest_offset: None,
            start_position: split.start_position,
            end_position: split.end_position,
        })
    }

    pub async fn next(&mut self) -> Result<Vec<SourceMessage>> {
        if self.shard_iter.is_none() {
            self.new_shard_iter().await?;
        }
        assert!(self.shard_iter.is_some());
        loop {
            match self.get_records().await {
                Ok(resp) => {
                    self.shard_iter = resp.next_shard_iterator().map(String::from);
                    let chunk = resp
                        .records()
                        .unwrap()
                        .iter()
                        .map(|r| {
                            SourceMessage::from(KinesisMessage::new(
                                self.shard_id.clone(),
                                r.clone(),
                            ))
                        })
                        .collect::<Vec<SourceMessage>>();
                    if chunk.is_empty() {
                        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                        continue;
                    }
                    self.latest_offset = Some(chunk.last().unwrap().offset.clone());
                    return Ok(chunk);
                }
                Err(e) => match e {
                    SdkError::ServiceError { err, .. } if err.is_expired_iterator_exception() => {
                        self.new_shard_iter().await?;
                        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                        continue;
                    }
                    e => return Err(anyhow!(e)),
                },
            };
        }
    }

    async fn new_shard_iter(&mut self) -> Result<()> {
        let (starting_seq_num, iter_type) = if self.latest_offset.is_some() {
            (
                self.latest_offset.take(),
                ShardIteratorType::AfterSequenceNumber,
            )
        } else {
            match &self.start_position {
                KinesisOffset::Earliest => (None, ShardIteratorType::TrimHorizon),
                KinesisOffset::SequenceNumber(seq) => {
                    (Some(seq.clone()), ShardIteratorType::AfterSequenceNumber)
                }
                _ => unreachable!(),
            }
        };

        let resp = self
            .client
            .get_shard_iterator()
            .stream_name(self.stream_name.clone())
            .shard_id(self.shard_id.as_ref())
            .shard_iterator_type(iter_type)
            .set_starting_sequence_number(starting_seq_num)
            .send()
            .await?;

        self.shard_iter = resp.shard_iterator().map(String::from);

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

#[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
async fn split_reader_into_stream(mut reader: KinesisSplitReader) {
    loop {
        match reader.next().await {
            Ok(chunk) => yield chunk,
            Err(e) => {
                tracing::error!("hang up kinesis reader due to polling error: {}", e);
                drop(reader);
                break;
            }
        }
    }
}

#[async_trait]
impl SplitReader for KinesisMultiSplitReader {
    type Properties = KinesisProperties;

    async fn new(
        properties: KinesisProperties,
        state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let splits = state.unwrap();
        Ok(Self {
            splits: splits
                .iter()
                .map(|split| match split {
                    SplitImpl::Kinesis(ks) => Ok(ks.to_owned()),
                    _ => Err(anyhow!(format!("expect KinesisSplit, got {:?}", split))),
                })
                .collect::<Result<Vec<KinesisSplit>>>()?,
            properties,
            message_cache: Arc::new(Mutex::new(Vec::new())),
            consumer_handler: None,
        })
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        if self.consumer_handler.is_none() {
            let split_readers = join_all(
                self.splits
                    .iter()
                    .map(|split| async {
                        KinesisSplitReader::new(self.properties.clone(), split.to_owned())
                            .await
                            .unwrap()
                    })
                    .collect::<Vec<_>>(),
            )
            .await;
            let cache = Arc::clone(&self.message_cache);

            self.consumer_handler = Some(tokio::spawn(async move {
                let join_stream = split_readers
                    .iter()
                    .map(|split| split_reader_into_stream(split.to_owned()))
                    .collect::<Vec<_>>()
                    .merge()
                    .into_stream();
                #[for_await]
                for msg in join_stream {
                    match msg {
                        Ok(chunk) => {
                            cache.lock().await.extend(chunk);
                        }
                        Err(e) => {
                            tracing::error!(
                                "split encountered error: {:?}, shutting down stream",
                                e
                            );
                            break;
                        }
                    }
                }
            }));
            tracing::info!("launch kinesis reader with splits: {:?}", self.splits);
        }
        loop {
            let mut cache_lock = self.message_cache.lock().await;
            if cache_lock.is_empty() {
                drop(cache_lock);
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                continue;
            }
            let chunk = cache_lock.clone();
            cache_lock.clear();
            drop(cache_lock);
            return Ok(Some(chunk));
        }
    }
}

impl KinesisMultiSplitReader {}
#[cfg(test)]
mod tests {

    use std::iter::Iterator;

    use futures_async_stream::for_await;
    use futures_concurrency::prelude::*;

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn test_single_thread_kinesis_reader() -> Result<()> {
        let properties = KinesisProperties {
            assume_role_arn: None,
            credentials_access_key: None,
            credentials_secret_access_key: None,
            stream_name: "kinesis_debug".to_string(),
            stream_region: "cn-northwest-1".to_string(),
            endpoint: None,
            session_token: None,
            assume_role_external_id: None,
        };

        let mut trim_horizen_reader = KinesisSplitReader::new(
            properties.clone(),
            KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::Earliest,
                end_position: KinesisOffset::None,
            },
        )
        .await?;
        let stream_reader = trim_horizen_reader.clone();
        println!("{:?}", trim_horizen_reader.next().await?);

        let mut offset_reader = KinesisSplitReader::new(
            properties.clone(),
            KinesisSplit {
                shard_id: "shardId-000000000001".to_string().into(),
                start_position: KinesisOffset::SequenceNumber(
                    "49629139817504901062972448413535783695568426186596941842".to_string(),
                ),
                end_position: KinesisOffset::None,
            },
        )
        .await?;
        println!("{:?}", offset_reader.next().await?);

        let stream1 = split_reader_into_stream(stream_reader.clone());
        let stream2 = split_reader_into_stream(stream_reader);
        let stream = vec![stream1, stream2].merge().into_stream();
        #[for_await]
        for msg in stream {
            println!("read stream: {:?}", msg);
            break;
        }

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_multi_splits() -> Result<()> {
        let properties = KinesisProperties {
            assume_role_arn: None,
            credentials_access_key: None,
            credentials_secret_access_key: None,
            stream_name: "kinesis_debug".to_string(),
            stream_region: "cn-northwest-1".to_string(),
            endpoint: None,
            session_token: None,
            assume_role_external_id: None,
        };

        let splits = vec!["shardId-000000000000", "shardId-000000000001"]
            .iter()
            .map(|split| {
                SplitImpl::Kinesis(KinesisSplit {
                    shard_id: split.to_string().into(),
                    start_position: KinesisOffset::Earliest,
                    end_position: KinesisOffset::None,
                })
            })
            .collect::<Vec<_>>();

        let mut reader = KinesisMultiSplitReader::new(properties, Some(splits), None).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        println!("1: {:?}", reader.next().await);
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        println!("2: {:?}", reader.next().await);
        Ok(())
    }
}
