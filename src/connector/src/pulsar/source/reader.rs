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
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::future::try_join_all;
use futures::{StreamExt, TryStreamExt};
use pulsar::consumer::{InitialPosition, Message};
use pulsar::{Consumer, ConsumerOptions, Pulsar, TokioExecutor};
use risingwave_common::try_match_expand;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::base::{SourceMessage, SplitReader};
use crate::pulsar::split::PulsarSplit;
use crate::pulsar::{PULSAR_CONFIG_SERVICE_URL_KEY, PULSAR_CONFIG_TOPIC_KEY};
use crate::{AnyhowProperties, ConnectorStateV2, Properties, SplitImpl};

struct PulsarSingleSplitReader {
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
}

impl PulsarSingleSplitReader {
    async fn new(properties: &AnyhowProperties, split: PulsarSplit) -> anyhow::Result<Self> {
        let service_url = properties.get(PULSAR_CONFIG_SERVICE_URL_KEY)?;
        let topic = split.topic.to_string();
        log::debug!(
            "creating consumer for pulsar split {:?}, prop topic {:?}",
            split,
            properties.get(PULSAR_CONFIG_TOPIC_KEY)
        );
        let builder = Pulsar::builder(service_url, TokioExecutor);
        let pulsar: Pulsar<_> = builder.build().await.map_err(|e| anyhow!(e))?;
        let consumer: Consumer<Vec<u8>, _> = pulsar
            .consumer()
            .with_topic(topic)
            .with_subscription("test-name-1-y")
            .with_options(
                ConsumerOptions::default().with_initial_position(InitialPosition::Earliest),
            )
            .build()
            .await
            .map_err(|e| anyhow!(e))?;

        // match split.start_offset {
        //     PulsarEnumeratorOffset::Earliest => {}
        //     PulsarEnumeratorOffset::Latest => {}
        //     PulsarEnumeratorOffset::MessageId(_) => {}
        //     PulsarEnumeratorOffset::Timestamp(_) => {}
        // }

        // //
        // // match split.start_offset {
        // //     PulsarOffset::MessageID(msg_id) => {}
        // //     PulsarOffset::Timestamp(_) => {}
        // //     PulsarOffset::None => {}
        // // };
        //
        // // consumer.seek(None, Some(MessageIdData {
        // //     ledger_id: 19,
        // //     entry_id: 3,
        // //     partition: Some(1),
        // //     batch_index: Some(0),
        // //     ack_set: vec![],
        // //     batch_size: None,
        // // }), None, pulsar).await.unwrap();
        //
        // let x = consumer.next().await;
        Ok(Self { pulsar, consumer })
    }

    async fn run(
        &mut self,
        mut stop: oneshot::Receiver<()>,
        output: mpsc::UnboundedSender<Message<Vec<u8>>>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                chunk = self.consumer.try_next() => {
                    let chunk: Option<Message<Vec<u8>>> = chunk.map_err(|e| anyhow!("consume from pulsar failed, err: {}", e.to_string()))?;
                    let msg = match chunk {
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

    async fn new(_props: Properties, _state: ConnectorStateV2) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = mpsc::unbounded_channel();
        let receiver = UnboundedReceiverStream::from(receiver);
        let splits = try_match_expand!(_state, ConnectorStateV2::Splits)
            .map_err(|e| anyhow!(e))
            .unwrap();
        let pulsar_splits: Vec<PulsarSplit> = splits
            .into_iter()
            .map(|split| {
                let split = try_match_expand!(split, SplitImpl::Pulsar);
                split
            })
            .collect::<risingwave_common::error::Result<Vec<PulsarSplit>>>()
            .map_err(|e| anyhow!(e))?;

        let props = Arc::new(AnyhowProperties::new(_props.0));

        let mut futures = vec![];
        let mut stop_chs = vec![];

        let readers = try_join_all(pulsar_splits.into_iter().map(|split| {
            let props = props.clone();
            async move { PulsarSingleSplitReader::new(&props, split).await }
        }))
        .await?;

        for mut reader in readers {
            // log::debug!("spawning pulsar split reader for split {:?}", split);
            // let mut reader = PulsarSingleSplitReader::new(&props, split).await?;
            let (stop_tx, stop_rx) = oneshot::channel();
            let sender = sender.clone();
            let handler = tokio::spawn(async move { reader.run(stop_rx, sender).await });
            stop_chs.push(stop_tx);
            futures.push(handler);
        }

        let _ = futures;

        Ok(Self {
            stop_chs: Some(stop_chs),
            messages: receiver,
        })
    }
}

impl Drop for PulsarSplitReader {
    fn drop(&mut self) {
        let chs = self.stop_chs.take().unwrap();
        for ch in chs {
            let _ = ch.send(());
        }
    }
}
