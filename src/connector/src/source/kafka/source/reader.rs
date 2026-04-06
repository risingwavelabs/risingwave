// Copyright 2022 RisingWave Labs
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
use std::mem::swap;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;

use crate::connector_common::read_kafka_log_level;
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::kafka::{
    KAFKA_ISOLATION_LEVEL, KafkaContextCommon, KafkaProperties, KafkaSplit, RwConsumerContext,
};
use crate::source::{
    BackfillInfo, BoxSourceChunkStream, Column, SourceChunkEvent, SourceContextRef, SplitId,
    SplitImpl, SplitMetaData, SplitReader, into_chunk_stream,
};

pub struct KafkaSplitReader {
    consumer: StreamConsumer<RwConsumerContext>,
    offsets: HashMap<SplitId, (Option<i64>, Option<i64>)>,
    backfill_info: HashMap<SplitId, BackfillInfo>,
    splits: Vec<KafkaSplit>,
    sync_call_timeout: Duration,
    bytes_per_second: usize,
    max_num_messages: usize,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[derive(Clone, Copy, Debug)]
enum SplitReadState {
    StopOffset(i64),
    PartitionEof,
    Completed,
    Unbounded,
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
        let mut config = ClientConfig::new();

        let bootstrap_servers = &properties.connection.brokers;
        let broker_rewrite_map = properties.privatelink_common.broker_rewrite_map.clone();

        let enable_partition_eof = source_ctx.source_ctrl_opts.enable_partition_eof;
        config.set(
            // Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition.
            // The signal is triggered each time reaching the end of a partition, even if multiple partitions are being consumed.
            "enable.partition.eof",
            if enable_partition_eof {
                "true"
            } else {
                "false"
            },
        );
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

        let mut tpl = TopicPartitionList::with_capacity(splits.len());

        let mut offsets = HashMap::new();
        let mut backfill_info = HashMap::new();
        for split in splits.clone() {
            let (low, high) = consumer
                .fetch_watermarks(
                    split.topic.as_str(),
                    split.partition,
                    properties.common.sync_call_timeout,
                )
                .await?;
            tracing::info!("fetch kafka watermarks: low: {low}, high: {high}, split: {split:?}");

            // note: low is inclusive, high is exclusive, start_offset is exclusive
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

            offsets.insert(split.id(), (split.start_offset, split.stop_offset));

            if let Some(offset) = split.start_offset {
                tpl.add_partition_offset(
                    split.topic.as_str(),
                    split.partition,
                    Offset::Offset(offset + 1),
                )?;
            } else {
                tpl.add_partition(split.topic.as_str(), split.partition);
            }
        }
        tracing::info!(
            topic = properties.common.topic,
            source_name = source_ctx.source_name,
            fragment_id = %source_ctx.fragment_id,
            source_id = %source_ctx.source_id,
            actor_id = %source_ctx.actor_id,
            "backfill_info: {:?}",
            backfill_info
        );

        consumer.assign(&tpl)?;

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
            consumer,
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
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }

    fn backfill_info(&self) -> HashMap<SplitId, BackfillInfo> {
        self.backfill_info.clone()
    }

    async fn seek_to_latest(&mut self) -> Result<Vec<SplitImpl>> {
        let mut latest_splits: Vec<SplitImpl> = Vec::new();
        let mut tpl = TopicPartitionList::with_capacity(self.splits.len());
        for mut split in self.splits.clone() {
            // we can't get latest offset if we use Offset::End, so we just fetch watermark here.
            let (_low, high) = self
                .consumer
                .fetch_watermarks(
                    split.topic.as_str(),
                    split.partition,
                    self.sync_call_timeout,
                )
                .await?;
            tpl.add_partition_offset(split.topic.as_str(), split.partition, Offset::Offset(high))?;
            split.start_offset = Some(high - 1);
            latest_splits.push(split.into());
        }
        // replace the previous assignment
        self.consumer.assign(&tpl)?;
        Ok(latest_splits)
    }
}

impl KafkaSplitReader {
    async fn complete_split_with_handoff(
        source_ctx: &SourceContextRef,
        split_states: &mut HashMap<SplitId, SplitReadState>,
        remaining_bounded_splits: &mut usize,
        last_emitted_offsets: &HashMap<SplitId, String>,
        split_id: &SplitId,
    ) {
        let need_complete = split_states.get(split_id.as_ref()).is_some_and(|state| {
            matches!(
                state,
                SplitReadState::StopOffset(_) | SplitReadState::PartitionEof
            )
        });
        if !need_complete {
            return;
        }

        if let Some(source_event_tx) = &source_ctx.source_event_tx {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            let backfill_offset = last_emitted_offsets.get(split_id.as_ref()).cloned();
            if source_event_tx
                .send(SourceChunkEvent::SplitHandoff {
                    split_id: split_id.clone(),
                    backfill_offset,
                    ack_tx,
                })
                .is_ok()
            {
                let _ = ack_rx.await;
            }
        }

        if let Some(state) = split_states.get_mut(split_id.as_ref())
            && matches!(
                *state,
                SplitReadState::StopOffset(_) | SplitReadState::PartitionEof
            )
        {
            *state = SplitReadState::Completed;
            *remaining_bounded_splits = remaining_bounded_splits
                .checked_sub(1)
                .expect("remaining bounded splits should not underflow");
        }
    }

    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        // enable_eof is set by bounded readers (backfill, batch) to:
        // 1. Handle rdkafka PartitionEOF events for invisible-tail detection
        // 2. Bound each split by "current head" when explicit stop_offset is absent.
        let enable_eof = self.source_ctx.source_ctrl_opts.enable_partition_eof;
        let has_explicit_stop_offset = self
            .offsets
            .values()
            .any(|(_, stop_offset)| stop_offset.is_some());
        let bounded_mode = enable_eof || has_explicit_stop_offset;

        let mut split_states: HashMap<SplitId, SplitReadState> =
            HashMap::with_capacity(self.offsets.len());
        let mut remaining_bounded_splits = 0usize;
        for (split_id, (start_offset, stop_offset)) in &self.offsets {
            match stop_offset {
                Some(stop_offset) => {
                    // stop_offset is exclusive.
                    let already_done = match start_offset {
                        Some(start_offset) => (*start_offset + 1) >= *stop_offset,
                        None => *stop_offset <= 0,
                    };
                    if !already_done {
                        split_states
                            .insert(split_id.clone(), SplitReadState::StopOffset(*stop_offset));
                        remaining_bounded_splits += 1;
                    } else {
                        split_states.insert(split_id.clone(), SplitReadState::Completed);
                    }
                }
                None if enable_eof => {
                    split_states.insert(split_id.clone(), SplitReadState::PartitionEof);
                    remaining_bounded_splits += 1;
                }
                None => {
                    split_states.insert(split_id.clone(), SplitReadState::Unbounded);
                }
            }
        }

        if self.offsets.is_empty() || (bounded_mode && remaining_bounded_splits == 0) {
            yield Vec::new();
            return Ok(());
        };

        let max_num_messages = if enable_eof && self.max_num_messages != usize::MAX {
            tracing::warn!(
                source_id = %self.source_ctx.source_id,
                actor_id = %self.source_ctx.actor_id,
                configured_max_num_messages = self.max_num_messages,
                "ignore max.num.messages for bounded EOF-aware reader to preserve completion semantics"
            );
            usize::MAX
        } else {
            self.max_num_messages
        };

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
        let mut last_emitted_offsets: HashMap<SplitId, String> =
            HashMap::with_capacity(self.offsets.len());

        #[for_await]
        'for_outer_loop: for chunk in self.consumer.stream().ready_chunks(max_chunk_size) {
            let mut has_data_message = false;
            for item in chunk {
                match item {
                    Ok(msg) => {
                        has_data_message = true;
                        let split_id: SplitId = msg.partition().to_string().into();
                        let cur_offset = msg.offset();
                        bytes_current_second += match &msg.payload() {
                            None => 0,
                            Some(payload) => payload.len(),
                        };
                        num_messages += 1;

                        let split_id_str = split_id.to_string();
                        latest_message_id_metrics
                            .entry(split_id_str.clone())
                            .or_insert_with(|| {
                                self.source_ctx
                                    .metrics
                                    .latest_message_id
                                    .with_guarded_label_values(&[
                                        // source name is not available here
                                        &self.source_ctx.source_id.to_string(),
                                        &self.source_ctx.actor_id.to_string(),
                                        &split_id_str,
                                    ])
                            })
                            .set(cur_offset);

                        let split_state = split_states
                            .get(split_id.as_ref())
                            .copied()
                            .unwrap_or(SplitReadState::Unbounded);

                        if let SplitReadState::StopOffset(stop_offset) = split_state {
                            // stop_offset is exclusive.
                            // Also handles log compaction where the exact stop_offset may be missing.
                            if cur_offset >= stop_offset {
                                tracing::debug!(
                                    "bounded stop offset reached split {}, offset {}",
                                    split_id,
                                    cur_offset
                                );
                                Self::complete_split_with_handoff(
                                    &self.source_ctx,
                                    &mut split_states,
                                    &mut remaining_bounded_splits,
                                    &last_emitted_offsets,
                                    &split_id,
                                )
                                .await;
                                if bounded_mode && remaining_bounded_splits == 0 {
                                    if !res.is_empty() {
                                        yield std::mem::take(&mut res);
                                    }
                                    break 'for_outer_loop;
                                }
                                continue;
                            }
                        } else if matches!(split_state, SplitReadState::Completed) {
                            // The split has already reached completion for this bounded reader.
                            continue;
                        }

                        let source_message =
                            SourceMessage::from_kafka_message(&msg, require_message_header);
                        res.push(source_message);
                        last_emitted_offsets.insert(split_id.clone(), cur_offset.to_string());

                        if let SplitReadState::StopOffset(stop_offset) = split_state
                            && cur_offset == stop_offset - 1
                        {
                            tracing::debug!(
                                "stop offset reached for split {}, stop reading, offset: {}, stop offset: {}",
                                split_id,
                                cur_offset,
                                stop_offset
                            );
                            Self::complete_split_with_handoff(
                                &self.source_ctx,
                                &mut split_states,
                                &mut remaining_bounded_splits,
                                &last_emitted_offsets,
                                &split_id,
                            )
                            .await;
                            if bounded_mode && remaining_bounded_splits == 0 {
                                yield std::mem::take(&mut res);
                                break 'for_outer_loop;
                            }
                        }

                        // Rate limiting
                        if bytes_current_second > self.bytes_per_second {
                            let mut cur = Vec::with_capacity(res.capacity());
                            swap(&mut cur, &mut res);
                            yield cur;
                            interval.tick().await;
                            bytes_current_second = 0;
                            res.clear();
                        }
                        if num_messages >= max_num_messages {
                            yield res;
                            break 'for_outer_loop;
                        }
                    }
                    Err(KafkaError::PartitionEOF(partition)) if enable_eof => {
                        let split_id: SplitId = partition.to_string().into();
                        let need_complete =
                            split_states.get(split_id.as_ref()).is_some_and(|state| {
                                matches!(
                                    state,
                                    SplitReadState::StopOffset(_) | SplitReadState::PartitionEof
                                )
                            });
                        if need_complete {
                            tracing::debug!(
                                "partition EOF for split {} in bounded reader",
                                split_id
                            );
                            Self::complete_split_with_handoff(
                                &self.source_ctx,
                                &mut split_states,
                                &mut remaining_bounded_splits,
                                &last_emitted_offsets,
                                &split_id,
                            )
                            .await;
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            if !has_data_message {
                // Chunk contained only PartitionEOF events, no data messages.
                // If all tracked splits are done, finish; otherwise keep waiting.
                if bounded_mode && remaining_bounded_splits == 0 {
                    if !res.is_empty() {
                        yield std::mem::take(&mut res);
                    }
                    break 'for_outer_loop;
                }
                continue;
            }

            // After processing the whole chunk, check split-level completion.
            // We intentionally check only after processing messages so that data in the same
            // ready_chunks batch as PartitionEOF are not dropped.
            if bounded_mode && remaining_bounded_splits == 0 {
                if !res.is_empty() {
                    yield std::mem::take(&mut res);
                }
                break 'for_outer_loop;
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
