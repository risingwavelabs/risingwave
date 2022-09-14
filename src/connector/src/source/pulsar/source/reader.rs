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

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use futures::StreamExt;
use itertools::Itertools;
use pulsar::consumer::InitialPosition;
use pulsar::message::proto::MessageIdData;
use pulsar::{Consumer, ConsumerBuilder, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use risingwave_common::try_match_expand;

use crate::source::pulsar::split::PulsarSplit;
use crate::source::pulsar::{PulsarEnumeratorOffset, PulsarProperties};
use crate::source::{Column, ConnectorState, SourceMessage, SplitImpl, SplitReader};

pub struct PulsarSplitReader {
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
        first_chunk_message_id: None,
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

const PULSAR_MAX_FETCH_MESSAGES: usize = 1024;

#[async_trait]
impl SplitReader for PulsarSplitReader {
    type Properties = PulsarProperties;

    async fn new(
        props: PulsarProperties,
        state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let splits = state.ok_or_else(|| anyhow!("no default state for reader"))?;
        ensure!(splits.len() == 1, "only support single split");
        let split = try_match_expand!(splits.into_iter().next().unwrap(), SplitImpl::Pulsar)
            .map_err(|e| anyhow!(e))?;

        let service_url = &props.service_url;
        let topic = split.topic.to_string();

        tracing::debug!("creating consumer for pulsar split topic {}", topic,);

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

            PulsarEnumeratorOffset::Timestamp(_) => builder,
        };

        let consumer: Consumer<Vec<u8>, _> = builder.build().await.map_err(|e| anyhow!(e))?;
        if let PulsarEnumeratorOffset::Timestamp(_ts) = split.start_offset {
            // FIXME: Here we need pulsar-rs to support the send + sync consumer
            // consumer
            //     .seek(None, None, Some(ts as u64), pulsar.clone())
            //     .await
            //     .map_err(|e| anyhow!(e))?;
        }

        Ok(Self {
            pulsar,
            consumer,
            split,
        })
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        let mut stream = self
            .consumer
            .borrow_mut()
            .ready_chunks(PULSAR_MAX_FETCH_MESSAGES);

        let chunk = match stream.next().await {
            None => return Ok(None),
            Some(chunk) => chunk,
        };

        let mut ret = Vec::with_capacity(chunk.len());

        for msg in chunk {
            let msg = msg.map_err(|e| anyhow!(e))?;
            ret.push(SourceMessage::from(msg));
        }

        Ok(Some(ret))
    }
}
