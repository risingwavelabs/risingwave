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

use std::borrow::BorrowMut;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use pulsar::consumer::{InitialPosition, Message};
use pulsar::message::proto::MessageIdData;
use pulsar::{Consumer, ConsumerBuilder, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use risingwave_common::try_match_expand;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::base::{SourceMessage, SplitReader};
use crate::pulsar::split::PulsarSplit;
use crate::pulsar::{PulsarEnumeratorOffset, PulsarProperties};
use crate::{Column, ConnectorStateV2, SplitImpl};

struct PulsarSingleSplitReader {
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    split: PulsarSplit,
}

// {ledger_id}:{entry_id}:{partition}:{batch_index}
fn parse_message_id(id: &str) -> Result<MessageIdData> {
    let splits = id.split(':').collect_vec();

    if splits.len() < 2 || splits.len() > 4 {
        return Err(anyhow!("illegal message id string {}", id));
    }

    let ledger_id = splits[0]
        .parse::<u64>()
        .map_err(|e| anyhow!("illegal ledger id {}", e))?;
    let entry_id = splits[1]
        .parse::<u64>()
        .map_err(|e| anyhow!("illegal entry id {}", e))?;

    let mut message_id = MessageIdData {
        ledger_id,
        entry_id,
        partition: None,
        batch_index: None,
        ack_set: vec![],
        batch_size: None,
    };

    if splits.len() > 2 {
        let partition = splits[2]
            .parse::<i32>()
            .map_err(|e| anyhow!("illegal partition {}", e))?;
        message_id.partition = Some(partition);
    }

    if splits.len() == 4 {
        let batch_index = splits[3]
            .parse::<i32>()
            .map_err(|e| anyhow!("illegal batch index {}", e))?;
        message_id.batch_index = Some(batch_index);
    }

    Ok(message_id)
}

impl PulsarSingleSplitReader {
    async fn new(properties: &PulsarProperties, split: PulsarSplit) -> anyhow::Result<Self> {
        let service_url = &properties.service_url;
        let topic = split.topic.to_string();

        log::debug!("creating consumer for pulsar split topic {}", topic,);

        let pulsar: Pulsar<_> = Pulsar::builder(service_url, TokioExecutor)
            .build()
            .await
            .map_err(|e| anyhow!(e))?;

        let builder: ConsumerBuilder<TokioExecutor> = pulsar
            .consumer()
            .with_topic(topic)
            .with_subscription_type(SubType::Exclusive)
            .with_subscription(format!(
                "consumer-{}",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros()
            ));

        let builder = match split.start_offset.clone() {
            PulsarEnumeratorOffset::Earliest => builder.with_options(
                ConsumerOptions::default().with_initial_position(InitialPosition::Earliest),
            ),
            PulsarEnumeratorOffset::Latest => builder.with_options(
                ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
            ),
            PulsarEnumeratorOffset::MessageId(m) => builder.with_options(pulsar::ConsumerOptions {
                durable: Some(false),
                start_message_id: parse_message_id(m.as_str()).ok(),
                ..Default::default()
            }),

            PulsarEnumeratorOffset::Timestamp(_) => {
                // todo: implement
                builder
            }
        };

        let consumer: Consumer<Vec<u8>, _> = builder.build().await.map_err(|e| anyhow!(e))?;

        Ok(Self {
            pulsar,
            consumer,
            split,
        })
    }

    async fn run(
        &mut self,
        mut stop: oneshot::Receiver<()>,
        output: mpsc::UnboundedSender<Message<Vec<u8>>>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                chunk = self.consumer.try_next() => {
                    let msg = match chunk.map_err(|e| anyhow!("consume from pulsar failed, err: {}", e.to_string()))? {
                        None => return Ok(()),
                        Some(msg) => msg,
                    };

                    output.send(msg).map_err(|e| anyhow!("send to output channel failed, err: {}", e.to_string()))?;
                }
                _ = stop.borrow_mut() => {
                    break;
                }
            }
        }

        log::debug!("pulsar reader stopped");
        Ok(())
    }
}

pub struct PulsarSplitReader {
    stop_chs: Option<Vec<oneshot::Sender<()>>>,
    messages: UnboundedReceiverStream<Message<Vec<u8>>>,
}

const PULSAR_MAX_FETCH_MESSAGES: u32 = 1024;

#[async_trait]
impl SplitReader for PulsarSplitReader {
    type Properties = PulsarProperties;

    async fn new(
        props: PulsarProperties,
        state: ConnectorStateV2,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let rx = UnboundedReceiverStream::from(rx);
        let splits = try_match_expand!(state, ConnectorStateV2::Splits).map_err(|e| anyhow!(e))?;

        let pulsar_splits: Vec<PulsarSplit> = splits
            .into_iter()
            .map(|split| try_match_expand!(split, SplitImpl::Pulsar).map_err(|e| anyhow!(e)))
            .collect::<Result<Vec<PulsarSplit>>>()?;

        let mut futures = vec![];
        let mut stop_chs = vec![];

        let readers = try_join_all(pulsar_splits.into_iter().map(|split| {
            log::debug!("spawning pulsar split reader for split {:?}", split);
            let props = props.clone();
            async move { PulsarSingleSplitReader::new(&props, split).await }
        }))
        .await?;

        for mut reader in readers {
            let (stop_tx, stop_rx) = oneshot::channel();
            let sender = tx.clone();
            let handler = tokio::spawn(async move { reader.run(stop_rx, sender).await });
            stop_chs.push(stop_tx);
            futures.push(handler);
        }

        let _ = futures;

        Ok(Self {
            stop_chs: Some(stop_chs),
            messages: rx,
        })
    }

    async fn next(&mut self) -> anyhow::Result<Option<Vec<SourceMessage>>> {
        let mut stream = self
            .messages
            .borrow_mut()
            .ready_chunks(PULSAR_MAX_FETCH_MESSAGES as usize);

        let chunk: Vec<Message<Vec<u8>>> = match stream.next().await {
            None => return Ok(None),
            Some(chunk) => chunk,
        };

        let ret = chunk.into_iter().map(SourceMessage::from).collect();

        Ok(Some(ret))
    }
}

impl PulsarSplitReader {}

impl Drop for PulsarSplitReader {
    fn drop(&mut self) {
        let chs = self.stop_chs.take().unwrap();
        for ch in chs {
            let _ = ch.send(());
        }
    }
}
