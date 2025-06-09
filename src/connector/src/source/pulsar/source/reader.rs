// Copyright 2025 RisingWave Labs
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

use std::sync::LazyLock;
use std::task::Poll;

use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use pulsar::consumer::InitialPosition;
use pulsar::message::proto::MessageIdData;
use pulsar::{Consumer, ConsumerBuilder, ConsumerOptions, Pulsar, SubType, TokioExecutor};
use risingwave_common::{bail, ensure};

use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::pulsar::split::PulsarSplit;
use crate::source::pulsar::{PulsarEnumeratorOffset, PulsarProperties};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, into_chunk_stream,
};

const PULSAR_DEFAULT_SUBSCRIPTION_PREFIX: &str = "rw-consumer";

pub enum PulsarSplitReader {
    Broker(PulsarBrokerReader),
}

#[async_trait]
impl SplitReader for PulsarSplitReader {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(
        props: PulsarProperties,
        splits: Vec<PulsarSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        ensure!(splits.len() == 1, "only support single split");
        let split = splits.into_iter().next().unwrap();
        let topic = split.topic.to_string();

        tracing::debug!("creating consumer for pulsar split topic {}", topic,);

        if props.iceberg_loader_enabled.unwrap_or(false) {
            bail!("PulsarIcebergReader has already been deprecated");
        } else {
            Ok(Self::Broker(
                PulsarBrokerReader::new(props, vec![split], parser_config, source_ctx, None)
                    .await?,
            ))
        }
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        match self {
            Self::Broker(reader) => {
                let (parser_config, source_context) =
                    (reader.parser_config.clone(), reader.source_ctx.clone());
                Box::pin(into_chunk_stream(
                    reader.into_data_stream(),
                    parser_config,
                    source_context,
                ))
            }
        }
    }
}

/// This reader reads from pulsar broker
pub struct PulsarBrokerReader {
    #[expect(dead_code)]
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    #[expect(dead_code)]
    split: PulsarSplit,
    #[expect(dead_code)]
    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

// {ledger_id}:{entry_id}:{partition}:{batch_index}
fn parse_message_id(id: &str) -> ConnectorResult<MessageIdData> {
    let splits = id.split(':').collect_vec();

    if splits.len() < 2 || splits.len() > 4 {
        bail!("illegal message id string {}", id);
    }

    let ledger_id = splits[0].parse::<u64>().context("illegal ledger id")?;
    let entry_id = splits[1].parse::<u64>().context("illegal entry id")?;

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
        let partition = splits[2].parse::<i32>().context("illegal partition")?;
        message_id.partition = Some(partition);
    }

    if splits.len() == 4 {
        let batch_index = splits[3].parse::<i32>().context("illegal batch index")?;
        message_id.batch_index = Some(batch_index);
    }

    Ok(message_id)
}

#[async_trait]
impl SplitReader for PulsarBrokerReader {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(
        props: PulsarProperties,
        splits: Vec<PulsarSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> ConnectorResult<Self> {
        ensure!(splits.len() == 1, "only support single split");
        let split = splits.into_iter().next().unwrap();
        let pulsar = props
            .common
            .build_client(&props.oauth, &props.aws_auth_props)
            .await?;
        let topic = split.topic.to_string();

        tracing::debug!("creating consumer for pulsar split topic {}", topic,);

        let builder: ConsumerBuilder<TokioExecutor> = pulsar
            .consumer()
            .with_topic(&topic)
            .with_subscription_type(SubType::Exclusive)
            .with_subscription(format!(
                "{}-{}-{}",
                props
                    .subscription_name_prefix
                    .unwrap_or(PULSAR_DEFAULT_SUBSCRIPTION_PREFIX.to_owned()),
                source_ctx.fragment_id,
                source_ctx.actor_id
            ));

        let builder = match split.start_offset.clone() {
            PulsarEnumeratorOffset::Earliest => {
                if topic.starts_with("non-persistent://") {
                    tracing::warn!(
                        "Earliest offset is not supported for non-persistent topic, use Latest instead"
                    );
                    builder.with_options(
                        ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
                    )
                } else {
                    builder.with_options(
                        ConsumerOptions::default()
                            .with_initial_position(InitialPosition::Earliest)
                            .durable(false),
                    )
                }
            }
            PulsarEnumeratorOffset::Latest => builder.with_options(
                ConsumerOptions::default()
                    .with_initial_position(InitialPosition::Latest)
                    .durable(false),
            ),
            PulsarEnumeratorOffset::MessageId(m) => {
                if topic.starts_with("non-persistent://") {
                    tracing::warn!(
                        "MessageId offset is not supported for non-persistent topic, use Latest instead"
                    );
                    builder.with_options(
                        ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
                    )
                } else {
                    builder.with_options(pulsar::ConsumerOptions {
                        durable: Some(false),
                        start_message_id: parse_message_id(m.as_str()).ok(),
                        ..Default::default()
                    })
                }
            }

            PulsarEnumeratorOffset::Timestamp(_) => builder,
        };

        let consumer: Consumer<Vec<u8>, _> = builder.build().await?;
        if let PulsarEnumeratorOffset::Timestamp(_ts) = split.start_offset {
            // FIXME: Here we need pulsar-rs to support the send + sync consumer
            // consumer
            //     .seek(None, None, Some(ts as u64), pulsar.clone())
            //     .await?;
        }

        Ok(Self {
            pulsar,
            consumer,
            split_id: split.id(),
            split,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl PulsarBrokerReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;
        #[for_await]
        for msgs in self.consumer.ready_chunks(max_chunk_size) {
            let mut res = Vec::with_capacity(msgs.len());
            for msg in msgs {
                let msg = SourceMessage::from(msg?);
                res.push(msg);
            }
            yield res;
        }
    }

    async fn to_stream(self) -> PulsarConsumeStream {
        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;
        let x = self.split_id;
        let (ack_tx, ack_rx) = tokio::sync::mpsc::unbounded_channel();

        PULSAR_ACK_CHANNEL
            .entry((self.split.topic.to_string(), x))
            .and_compute_with(|_current| {
                // insert the ack_tx to the moka cache
                moka::ops::compute::Op::Put(ack_tx.clone())
            })
            .await;

        PulsarConsumeStream {
            pulsar_reader: self.consumer,
            ack_rx,
            topic: self.split.topic.to_string(),
            max_chunk_size,
            chunk_cache: vec![],
        }
    }
}

use std::sync::Arc;

use moka::future::Cache as MokaCache;

pub static PULSAR_ACK_CHANNEL: LazyLock<
    MokaCache<(String, Arc<str>), tokio::sync::mpsc::UnboundedSender<i32>>,
> = LazyLock::new(|| moka::future::Cache::builder().build()); // mapping:

struct PulsarConsumeStream {
    pulsar_reader: Consumer<Vec<u8>, TokioExecutor>,
    ack_rx: tokio::sync::mpsc::UnboundedReceiver<i32>,
    topic: String,
    max_chunk_size: usize,
    chunk_cache: Vec<pulsar::consumer::Message<Vec<u8>>>,
}

impl PulsarConsumeStream {
    fn do_ack(&mut self, message_id: MessageIdData) {
        let topic = self.topic.clone();
        let _ = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async { self.pulsar_reader.ack_with_id(&topic, message_id).await })
        })
        .map_err(|e| {
            tracing::warn!(
                error=?e, "meet error when ack message"
            )
        });
    }

    fn do_handle_message(
        &mut self,
        maybe_message: Option<Result<pulsar::consumer::Message<Vec<u8>>, pulsar::error::Error>>,
    ) -> Result<(), pulsar::error::Error> {
        if let Some(poll_message_result) = maybe_message {
            match poll_message_result {
                Err(e) => {
                    return Err(e);
                }
                Ok(one_message) => {
                    self.chunk_cache.push(one_message);
                }
            }
        }
        Ok(())
    }
}

impl futures::Stream for PulsarConsumeStream {
    type Item = Result<Vec<pulsar::consumer::Message<Vec<u8>>>, pulsar::error::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match (
            self.ack_rx.poll_recv(cx),
            self.pulsar_reader.poll_next_unpin(cx),
        ) {
            (Poll::Pending, Poll::Pending) => {}
            (Poll::Ready(_some_ack), Poll::Pending) => {
                self.do_ack(MessageIdData::default());
            }
            (Poll::Pending, Poll::Ready(maybe_message)) => {
                if let Err(e) = self.do_handle_message(maybe_message) {
                    return Poll::Ready(Some(Err(e)));
                }
            }
            (Poll::Ready(_some_ack), Poll::Ready(maybe_message)) => {
                self.do_ack(MessageIdData::default());
                if let Err(e) = self.do_handle_message(maybe_message) {
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        if self.chunk_cache.len() <= self.max_chunk_size {
            let ready_chunk = std::mem::take(&mut self.chunk_cache);
            return Poll::Ready(Some(Ok(ready_chunk)));
        }

        Poll::Pending
    }
}
