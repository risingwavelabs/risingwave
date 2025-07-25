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

/// Filter offset for pulsar messages
#[derive(Debug, Clone)]
pub struct PulsarFilterOffset {
    /// Entry ID of the message
    pub entry_id: u64,
    /// Batch index of the message, if any
    pub batch_index: Option<i32>,
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

    // for filter out already read messages
    already_read_offset: Option<PulsarFilterOffset>,
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

        let subscription_type = match split.start_offset {
            PulsarEnumeratorOffset::Timestamp(_) => SubType::Failover,
            _ => SubType::Exclusive,
        };

        let builder: ConsumerBuilder<TokioExecutor> = pulsar
            .consumer()
            .with_topic(&topic)
            .with_subscription_type(subscription_type)
            .with_subscription(format!(
                "{}-{}-{}",
                props
                    .subscription_name_prefix
                    .unwrap_or(PULSAR_DEFAULT_SUBSCRIPTION_PREFIX.to_owned()),
                source_ctx.fragment_id,
                source_ctx.actor_id
            ));

        let mut already_read_offset = None;

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
                    let start_message_id = parse_message_id(m.as_str())?;
                    already_read_offset = Some(PulsarFilterOffset {
                        entry_id: start_message_id.entry_id,
                        batch_index: start_message_id.batch_index,
                    });
                    builder.with_options(pulsar::ConsumerOptions {
                        durable: Some(false),
                        start_message_id: Some(start_message_id),
                        ..Default::default()
                    })
                }
            }

            PulsarEnumeratorOffset::Timestamp(_) => {
                // For timestamp positioning, start from earliest and seek later
                if topic.starts_with("non-persistent://") {
                    tracing::warn!(
                        "Timestamp offset is not supported for non-persistent topic, use Latest instead"
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
        };

        let mut consumer: Consumer<Vec<u8>, _> = builder.build().await?;

        // Handle timestamp seeking after consumer creation
        if let PulsarEnumeratorOffset::Timestamp(ts) = split.start_offset
            && !topic.starts_with("non-persistent://")
        {
            // according to https://pulsar.apache.org/api/client/3.1.x/org/apache/pulsar/client/api/Reader.html
            // the timestamp is in milliseconds
            consumer
                .seek(None, None, Some(ts as u64), pulsar.clone())
                .await?;
        }

        Ok(Self {
            pulsar,
            consumer,
            split_id: split.id(),
            split,
            parser_config,
            source_ctx,
            already_read_offset,
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
        let mut already_read_offset = self.already_read_offset;
        #[for_await]
        for msgs in self.consumer.ready_chunks(max_chunk_size) {
            let mut res = Vec::with_capacity(msgs.len());
            for msg in msgs {
                let msg = msg?;

                if let Some(PulsarFilterOffset {
                    entry_id,
                    batch_index,
                }) = already_read_offset
                {
                    let message_id = msg.message_id();

                    // for most case, we only compare `entry_id`
                    // but for batch message, we need to compare `batch_index` if the `entry_id` is the same
                    if message_id.entry_id <= entry_id && message_id.batch_index <= batch_index {
                        tracing::info!(
                            "skipping message with entry_id: {}, batch_index: {:?} as expected offset after entry_id {} batch_index {:?}",
                            message_id.entry_id,
                            message_id.batch_index,
                            entry_id,
                            batch_index
                        );
                        continue;
                    } else {
                        already_read_offset = None;
                    }
                }

                let msg = SourceMessage::from(msg);
                res.push(msg);
            }
            yield res;
        }
    }
}
