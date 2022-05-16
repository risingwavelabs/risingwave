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
use std::collections::HashMap;
use std::sync::Arc;
use std::{thread, time};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::model::ShardIteratorType;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::types::SdkError;
use aws_sdk_kinesis::Client as KinesisClient;
use aws_smithy_types::DateTime;
use futures::TryFutureExt;
// pub struct KinesisSplitReader {
//     client: KinesisClient,
//     stream_name: String,
//     shard_id: String,
//     latest_sequence_num: String,
//     shard_iter: Option<String>,
//     assigned_split: Option<KinesisSplit>,
// }
use futures_async_stream::{for_await, try_stream};
use futures_concurrency::prelude::*;
use risingwave_common::error::RwError;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::base::{SourceMessage, SplitReader};
use crate::kinesis::build_client;
use crate::kinesis::source::message::KinesisMessage;
use crate::kinesis::source::state::KinesisSplitReaderState;
use crate::kinesis::split::{KinesisOffset, KinesisSplit};
use crate::{ConnectorStateV2, KinesisProperties};

pub struct KinesisMultiSplitReader {
    client: KinesisClient,
    /// splits are not allowed to be empty, otherwise connector source should create
    /// [`DummySplitReader`] which is always idling.
    splits: Vec<KinesisSplit>,
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

pub struct KinesisSplitReader<'a> {
    client: &'a KinesisClient,
    stream_name: String,
    shard_id: String,
    latest_offset: Option<String>,
    shard_iter: Option<String>,
    start_position: KinesisOffset,
    end_position: KinesisOffset,
}

impl<'a> KinesisSplitReader<'a> {
    pub fn new(
        client: &'a KinesisClient,
        stream_name: String,
        split: KinesisSplit,
    ) -> Result<Self> {
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
                        .into_iter()
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
            .shard_id(self.shard_id.clone())
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
        let s = self.shard_iter.clone();
        let resp = self
            .client
            .get_records()
            .set_shard_iterator(self.shard_iter.take())
            .send()
            .await;
        resp
    }
}

#[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
async fn split_reader_into_stream(mut reader: KinesisSplitReader) {
    loop {
        match reader.next().await {
            Ok(chunk) => yield chunk,
            Err(e) => {
                log::error!("hang up kinesis reader due to polling error: {}", e);
                drop(reader);
                break;
            }
        }
    }
}

#[async_trait]
impl SplitReader for KinesisMultiSplitReader {
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        if self.consumer_handler.is_none() {
            todo!()
        }
        todo!()
    }
}

impl KinesisMultiSplitReader {
    pub async fn new(properties: KinesisProperties, state: ConnectorStateV2) -> Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }
}
#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::iter::Iterator;

    use async_stream::stream;
    use futures::Stream;
    use futures_async_stream::{for_await, try_stream};
    use futures_concurrency::prelude::*;
    use rand::Rng;

    use super::*;

    #[try_stream(ok = i32, error = anyhow::Error)]
    async fn stream(i: i32, sleep: u64) {
        let mut j = i;
        loop {
            j += 1;
            yield j;
            tokio::time::sleep(tokio::time::Duration::from_millis(sleep)).await;
        }
    }

    // async fn get_chunk(store: &mut Arc<Mutex<Vec<i32>>>, stream: impl Stream<Item = Result<i32>>)
    // {     #[for_await]
    //     for msg in stream {
    //         store.lock().await.push(msg.unwrap());
    //     }
    // }

    #[tokio::test]
    async fn test_stream() -> Result<()> {
        // let s_1 = stream! {
        //     let mut i = 0;
        //     loop {
        //         yield Ok(i);
        //         i += 1;
        //         std::thread::sleep(std::time::Duration::from_millis(300))
        //     }
        // };
        // let s_2 = stream! {
        //     let mut i = 100;
        //     loop {
        //         yield Ok(i);
        //         i += 1;
        //         std::thread::sleep(std::time::Duration::from_millis(500))
        //     }
        // };
        // let s_3 = stream! {
        //     let mut i = 1000;
        //     loop {
        //         yield Ok(i);
        //         i += 1;
        //         std::thread::sleep(std::time::Duration::from_millis(700))
        //     }
        // };

        let s_1 = stream(0, 300);
        let s_2 = stream(100, 500);
        let s_3 = stream(1000, 700);

        let s = (s_1, s_2, s_3).merge().into_stream();

        let x: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
        let _x = Arc::clone(&x);
        let handler = tokio::spawn(async move {
            #[for_await]
            for msg in s {
                println!("get msg: {:?}", msg);
                _x.lock().await.push(msg.unwrap());
            }
        });

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // let mut v = x.lock().await;
        // println!("{:?}", v);
        // v.clear();
        // drop(v);
        // let v = s.collect().await;
        // println!("{:?}", v);

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        // let v = x.lock().await;
        // let v1 = s.collect().await;
        // println!("{:?}", v1);
        // handler.abort();
        Ok(())
    }

    #[tokio::test]
    async fn test_single_thread_kinesis_reader() -> Result<()> {
        let properties = KinesisProperties {
            assume_role_arn: None,
            credentials_access_key: None,
            credentials_secret_access_key: None,
            stream_name: "kinesis_debug".to_string(),
            stream_region: "cn-northwest-1".to_string(),
            endpoint: None,
            session_token: None,
            assume_role_externeal_id: None,
        };
        let client = build_client(properties.clone()).await?;

        let mut trim_horizen_reader = KinesisSplitReader::new(
            &client,
            properties.stream_name.clone(),
            KinesisSplit {
                shard_id: "shardId-000000000001".to_string(),
                start_position: KinesisOffset::Earliest,
                end_position: KinesisOffset::None,
            },
        )?;
        println!("{:?}", trim_horizen_reader.next().await?);

        let mut offset_reader = KinesisSplitReader::new(
            &client,
            properties.stream_name.clone(),
            KinesisSplit {
                shard_id: "shardId-000000000001".to_string(),
                start_position: KinesisOffset::SequenceNumber(
                    "49629139817504901062972448413535783695568426186596941842".to_string(),
                ),
                end_position: KinesisOffset::None,
            },
        )?;
        println!("{:?}", offset_reader.next().await?);

        Ok(())
    }
}
