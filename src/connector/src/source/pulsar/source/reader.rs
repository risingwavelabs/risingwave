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
use futures::TryStreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use pulsar::consumer::InitialPosition;
use pulsar::message::proto::MessageIdData;
use pulsar::reader::Reader;
use pulsar::{ConsumerOptions, Pulsar, TokioExecutor};
use risingwave_common::{bail, ensure};

use crate::error::ConnectorResult;
use crate::parser::ParserConfig;
use crate::source::pulsar::split::PulsarSplit;
use crate::source::pulsar::{PulsarEnumeratorOffset, PulsarProperties};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitId, SplitMetaData,
    SplitReader, into_chunk_stream,
};

const PULSAR_DEFAULT_READER_PREFIX: &str = "rw-reader";

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
    reader: Reader<Vec<u8>, TokioExecutor>,
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

        tracing::debug!("creating reader for pulsar split topic {}", topic,);

        let builder = pulsar
            .reader()
            .with_topic(&topic)
            .with_consumer_name(format!(
                "{}-{}-{}",
                props
                    .subscription_name_prefix
                    .unwrap_or(PULSAR_DEFAULT_READER_PREFIX.to_owned()),
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
                        ConsumerOptions::default().with_initial_position(InitialPosition::Earliest),
                    )
                }
            }
            PulsarEnumeratorOffset::Latest => builder.with_options(
                ConsumerOptions::default().with_initial_position(InitialPosition::Latest),
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
                        start_message_id: Some(start_message_id),
                        ..Default::default()
                    })
                }
            }

            PulsarEnumeratorOffset::Timestamp(_) => builder,
        };

        let mut reader: Reader<Vec<u8>, _> = builder.into_reader().await?;
        if let PulsarEnumeratorOffset::Timestamp(ts) = split.start_offset {
            // Use reader.seek() to seek to a specific timestamp
            reader.seek(None, Some(ts as u64)).await?;
        }

        Ok(Self {
            pulsar,
            reader,
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
        let mut res = Vec::with_capacity(max_chunk_size);
        let mut reader = self.reader;

        loop {
            // Read one message at a time from the reader
            match reader.try_next().await {
                Ok(Some(msg)) => {
                    if let Some(PulsarFilterOffset {
                        entry_id,
                        batch_index,
                    }) = already_read_offset
                    {
                        let message_id = msg.message_id();

                        // for most case, we only compare `entry_id`
                        // but for batch message, we need to compare `batch_index` if the `entry_id` is the same
                        if message_id.entry_id <= entry_id && message_id.batch_index <= batch_index
                        {
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

                    // If we've collected enough messages, yield them
                    if res.len() >= max_chunk_size {
                        yield res;
                        res = Vec::with_capacity(max_chunk_size);
                    }
                }
                Ok(None) => {
                    // No more messages available, yield any remaining messages
                    if !res.is_empty() {
                        yield res;
                        res = Vec::with_capacity(max_chunk_size);
                    }
                    // Reader reached end of topic or no new messages
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                Err(e) => {
                    // Handle error and yield any messages collected so far
                    if !res.is_empty() {
                        yield res;
                    }
                    return Err(e.into());
                }
            }
        }
    }
}
