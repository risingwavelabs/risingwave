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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem::swap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::OwnedMessage;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::Receiver;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;

use crate::connector_common::read_kafka_log_level;
use crate::error::{ConnectorError, ConnectorResult as Result};
use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::kafka::source::mux_reader::KafkaMuxReader;
use crate::source::kafka::{
    KAFKA_ISOLATION_LEVEL, KafkaContextCommon, KafkaProperties, KafkaSplit, RwConsumerContext,
};
use crate::source::{
    BackfillInfo, BoxSourceChunkStream, Column, ReleaseHandle, SourceContextRef, SplitId,
    SplitImpl, SplitMetaData, SplitReader, into_chunk_stream,
};

enum MessageReader {
    Consumer(StreamConsumer<RwConsumerContext>),
    MuxReader {
        reader: Arc<KafkaMuxReader>,
        receiver: Receiver<Vec<OwnedMessage>>,
    },
}

impl MessageReader {
    fn into_owned_stream(
        self,
    ) -> Pin<Box<dyn Stream<Item = Result<OwnedMessage, KafkaError>> + Send>> {
        inner_owned_stream(self)
    }
}

#[try_stream(boxed, ok = OwnedMessage, error = KafkaError)]
async fn inner_owned_stream(reader: MessageReader) {
    match reader {
        MessageReader::Consumer(consumer) => {
            let mut s = consumer.stream();
            while let Some(msg_res) = s.next().await {
                let msg = msg_res?;
                yield msg.detach();
            }
        }
        MessageReader::MuxReader { receiver, .. } => {
            let mut s = ReceiverStream::new(receiver);
            while let Some(batch) = s.next().await {
                for msg in batch {
                    yield msg;
                }
            }
        }
    }
}

pub struct KafkaSplitReader {
    message_reader: MessageReader,
    offsets: HashMap<SplitId, (Option<i64>, Option<i64>)>,
    backfill_info: HashMap<SplitId, BackfillInfo>,
    splits: Vec<KafkaSplit>,
    sync_call_timeout: Duration,
    bytes_per_second: usize,
    max_num_messages: usize,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
pub(crate) trait KafkaMetaFetcher {
    async fn proxy_fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> rdkafka::error::KafkaResult<(i64, i64)>;
}

#[async_trait]
impl<C: rdkafka::consumer::ConsumerContext + 'static> KafkaMetaFetcher for StreamConsumer<C> {
    async fn proxy_fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> rdkafka::error::KafkaResult<(i64, i64)> {
        self.fetch_watermarks(topic, partition, timeout).await
    }
}

async fn get_split_metadata<F: KafkaMetaFetcher + ?Sized>(
    fetcher: &F,
    splits: &[KafkaSplit],
    sync_call_timeout: Duration,
) -> Result<(TopicPartitionList, HashMap<SplitId, BackfillInfo>)> {
    println!("calling get split meta {:#?}", splits);

    let mut tpl = TopicPartitionList::with_capacity(splits.len());
    let mut backfill_info = HashMap::new();

    for split in splits {
        if let Some(offset) = split.start_offset {
            tpl.add_partition_offset(
                split.topic.as_str(),
                split.partition,
                Offset::Offset(offset + 1),
            )?;
        } else {
            tpl.add_partition(split.topic.as_str(), split.partition);
        }

        let (low, high) = fetcher
            .proxy_fetch_watermarks(split.topic.as_str(), split.partition, sync_call_timeout)
            .await?;

        tracing::info!("fetch kafka watermarks: low: {low}, high: {high}, split: {split:?}");

        if low == high || split.start_offset.is_some_and(|offset| offset + 1 >= high) {
            backfill_info.insert(split.id(), BackfillInfo::NoDataToBackfill);
        } else {
            debug_assert!(high > 0);
            backfill_info.insert(
                split.id(),
                BackfillInfo::HasDataToBackfill {
                    latest_offset: (high - 1).to_string(),
                },
            );
        }
    }
    Ok((tpl, backfill_info))
}

#[async_trait]
impl SplitReader for KafkaSplitReader {
    type Properties = KafkaProperties;
    type Split = KafkaSplit;

    async fn new(
        properties: KafkaProperties,
        splits: Vec<KafkaSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let offsets = splits
            .iter()
            .map(|split| (split.id(), (split.start_offset, split.stop_offset)))
            .collect();

        let source_info = &source_ctx.source_info;

        let connection_id = source_info.as_ref().and_then(|s| s.connection_id);
        let mux_enabled = properties.enable_mux_reader.unwrap_or(false);

        let (message_reader, backfill_info) = if mux_enabled {
            let connection_id = connection_id.ok_or_else(|| {
                ConnectorError::from(anyhow!(
                    "Kafka mux reader requires a connection bound via WITH CONNECTION when `enable.mux.reader` is true"
                ))
            })?;
            let reader = KafkaMuxReader::get_or_create(
                connection_id.to_string(),
                properties.clone(),
                source_ctx.clone(),
            )
            .await?;

            let (tpl, backfill_info) =
                get_split_metadata(&*reader, &splits, properties.common.sync_call_timeout).await?;

            let receiver = reader.register_topic_partition_list(tpl).await?;
            let reader = MessageReader::MuxReader { reader, receiver };
            (reader, backfill_info)
        } else {
            let mut config = ClientConfig::new();

            let bootstrap_servers = &properties.connection.brokers;
            let broker_rewrite_map = properties.privatelink_common.broker_rewrite_map.clone();

            // disable partition eof
            config.set("enable.partition.eof", "false");
            config.set("auto.offset.reset", "smallest");
            config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
            config.set("bootstrap.servers", bootstrap_servers);

            properties.connection.set_security_properties(&mut config);
            properties.set_client(&mut config);

            config.set("group.id", properties.group_id(source_ctx.fragment_id));

            let ctx_common = KafkaContextCommon::new(
                broker_rewrite_map,
                Some(format!(
                    "fragment-{}-source-{}-actor-{}",
                    source_ctx.fragment_id, source_ctx.source_id, source_ctx.actor_id
                )),
                // thread consumer will keep polling in the background, we don't need to call `poll`
                // explicitly
                Some(source_ctx.metrics.rdkafka_native_metric.clone()),
                properties.aws_auth_props,
                properties.connection.is_aws_msk_iam(),
            )
            .await?;

            let client_ctx = RwConsumerContext::new(ctx_common);

            if let Some(log_level) = read_kafka_log_level() {
                config.set_log_level(log_level);
            }

            let consumer: StreamConsumer<RwConsumerContext> = config
                .create_with_context(client_ctx)
                .await
                .context("failed to create kafka consumer")?;

            let (tpl, backfill_info) =
                get_split_metadata(&consumer, &splits, properties.common.sync_call_timeout).await?;

            consumer.assign(&tpl)?;
            let reader = MessageReader::Consumer(consumer);
            (reader, backfill_info)
        };

        tracing::info!(
            topic = properties.common.topic,
            source_name = source_ctx.source_name,
            fragment_id = %source_ctx.fragment_id,
            source_id = %source_ctx.source_id,
            actor_id = %source_ctx.actor_id,
            "backfill_info: {:?}",
            backfill_info
        );

        // The two parameters below are only used by developers for performance testing purposes,
        // so we panic here on purpose if the input is not correctly recognized.
        let bytes_per_second = match properties.bytes_per_second {
            None => usize::MAX,
            Some(number) => number
                .parse::<usize>()
                .expect("bytes.per.second expect usize"),
        };
        let max_num_messages = match properties.max_num_messages {
            None => usize::MAX,
            Some(number) => number
                .parse::<usize>()
                .expect("max.num.messages expect usize"),
        };

        Ok(Self {
            message_reader,
            offsets,
            splits,
            backfill_info,
            bytes_per_second,
            sync_call_timeout: properties.common.sync_call_timeout,
            max_num_messages,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_cfg = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        let data_stream = self.into_data_stream();
        into_chunk_stream(data_stream, parser_cfg, source_context)
    }

    fn backfill_info(&self) -> HashMap<SplitId, BackfillInfo> {
        self.backfill_info.clone()
    }

    fn release_handle(&self) -> ReleaseHandle {
        if let MessageReader::MuxReader { reader, .. } = &self.message_reader {
            let reader = Arc::clone(reader);

            let mut tpl = TopicPartitionList::new();
            for split in &self.splits {
                tpl.add_partition(split.topic.as_str(), split.partition);
            }

            ReleaseHandle::new(async move {
                tracing::info!(
                    "Kafka split reader dropping (mux reader mode), unregistering topic partition list: {:?}",
                    tpl
                );
                if let Err(e) = reader.unregister_topic_partition_list(tpl).await {
                    tracing::error!(error = %e.as_report(), "Failed to unregister topic partition list");
                }
            })
        } else {
            ReleaseHandle::noop()
        }
    }

    async fn seek_to_latest(&mut self) -> Result<Vec<SplitImpl>> {
        async fn fetch_latest_splits_meta<F: KafkaMetaFetcher + ?Sized>(
            fetcher: &F,
            splits: &[KafkaSplit],
            sync_call_timeout: Duration,
        ) -> Result<(Vec<SplitImpl>, TopicPartitionList)> {
            let mut latest_splits: Vec<SplitImpl> = Vec::with_capacity(splits.len());
            let mut tpl = TopicPartitionList::with_capacity(splits.len());

            for mut split in splits.iter().cloned() {
                // we can't get latest offset if we use Offset::End, so we just fetch watermark here.
                let (_low, high) = fetcher
                    .proxy_fetch_watermarks(
                        split.topic.as_str(),
                        split.partition,
                        sync_call_timeout,
                    )
                    .await?;

                tpl.add_partition_offset(
                    split.topic.as_str(),
                    split.partition,
                    Offset::Offset(high),
                )?;
                split.start_offset = Some(high - 1);
                latest_splits.push(split.into());
            }

            Ok((latest_splits, tpl))
        }

        match &mut self.message_reader {
            MessageReader::Consumer(consumer) => {
                let (latest_splits, tpl) =
                    fetch_latest_splits_meta(consumer, &self.splits, self.sync_call_timeout)
                        .await?;
                // replace the previous assignment
                consumer.assign(&tpl)?;
                Ok(latest_splits)
            }
            MessageReader::MuxReader { reader, .. } => {
                let (latest_splits, tpl) =
                    fetch_latest_splits_meta(&**reader, &self.splits, self.sync_call_timeout)
                        .await?;
                reader
                    .reassign_partitions(tpl)
                    .await
                    .context("failed to reassign mux reader partitions")?;
                Ok(latest_splits)
            }
        }
    }
}

impl KafkaSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        if self.offsets.values().all(|(start_offset, stop_offset)| {
            match (start_offset, stop_offset) {
                (Some(start), Some(stop)) if (*start + 1) >= *stop => true,
                (_, Some(stop)) if *stop == 0 => true,
                _ => false,
            }
        }) {
            yield Vec::new();
            return Ok(());
        };

        let mut stop_offsets: HashMap<_, _> = self
            .offsets
            .iter()
            .flat_map(|(split_id, (_, stop_offset))| {
                stop_offset.map(|offset| (split_id.clone() as SplitId, offset))
            })
            .collect();

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;
        let mut bytes_current_second = 0;
        let mut num_messages = 0;
        let max_chunk_size = self.source_ctx.source_ctrl_opts.chunk_size;
        let mut res = Vec::with_capacity(max_chunk_size);
        // ingest kafka message header can be expensive, do it only when required
        let require_message_header = self.parser_config.common.rw_columns.iter().any(|col_desc| {
            matches!(
                col_desc.additional_column.column_type,
                Some(AdditionalColumnType::Headers(_) | AdditionalColumnType::HeaderInner(_))
            )
        });

        let mut latest_message_id_metrics: HashMap<String, LabelGuardedIntGauge> = HashMap::new();

        let owned_stream = self.message_reader.into_owned_stream();

        #[for_await]
        'for_outer_loop: for msgs in owned_stream.ready_chunks(max_chunk_size) {
            let msgs: Vec<_> = msgs
                .into_iter()
                .collect::<std::result::Result<_, KafkaError>>()?;

            let mut split_msg_offsets = HashMap::new();

            for msg in &msgs {
                split_msg_offsets.insert(msg.partition(), msg.offset());
            }

            for (partition, offset) in split_msg_offsets {
                let split_id = partition.to_string();
                latest_message_id_metrics
                    .entry(split_id.clone())
                    .or_insert_with(|| {
                        self.source_ctx
                            .metrics
                            .latest_message_id
                            .with_guarded_label_values(&[
                                // source name is not available here
                                &self.source_ctx.source_id.to_string(),
                                &self.source_ctx.actor_id.to_string(),
                                &split_id,
                            ])
                    })
                    .set(offset);
            }

            for msg in msgs {
                let cur_offset = msg.offset();
                bytes_current_second += match &msg.payload() {
                    None => 0,
                    Some(payload) => payload.len(),
                };
                num_messages += 1;
                let source_message =
                    SourceMessage::from_kafka_message(&msg, require_message_header);
                let split_id = source_message.split_id.clone();
                res.push(source_message);

                if let Entry::Occupied(o) = stop_offsets.entry(split_id) {
                    let stop_offset = *o.get();

                    if cur_offset == stop_offset - 1 {
                        tracing::debug!(
                            "stop offset reached for split {}, stop reading, offset: {}, stop offset: {}",
                            o.key(),
                            cur_offset,
                            stop_offset
                        );

                        o.remove();

                        if stop_offsets.is_empty() {
                            yield res;
                            break 'for_outer_loop;
                        }

                        continue;
                    }
                }

                if bytes_current_second > self.bytes_per_second {
                    // swap to make compiler happy
                    let mut cur = Vec::with_capacity(res.capacity());
                    swap(&mut cur, &mut res);
                    yield cur;
                    interval.tick().await;
                    bytes_current_second = 0;
                    res.clear();
                }
                if num_messages >= self.max_num_messages {
                    yield res;
                    break 'for_outer_loop;
                }
            }
            let mut cur = Vec::with_capacity(res.capacity());
            swap(&mut cur, &mut res);
            yield cur;
            // don't clear `bytes_current_second` here as it is only related to `.tick()`.
            // yield in the outer loop so that we can always guarantee that some messages are read
            // every `MAX_CHUNK_SIZE`.
        }
        tracing::info!("kafka reader finished");
    }
}
