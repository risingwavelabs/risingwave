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
use std::collections::hash_map::Entry;
use std::future::Future;
use std::mem::swap;
use std::time::Duration;

use anyhow::{Context, anyhow};
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
    BackfillInfo, BoxSourceChunkStream, Column, SourceContextRef, SplitId, SplitImpl,
    SplitMetaData, SplitReader, into_chunk_stream,
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

        config.set(
            "enable.partition.eof",
            if properties.enable_partition_eof {
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
            properties.aws_auth_props.clone(),
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

            let (low, high) = consumer
                .fetch_watermarks(
                    split.topic.as_str(),
                    split.partition,
                    properties.common.sync_call_timeout,
                )
                .await?;
            tracing::info!("fetch kafka watermarks: low: {low}, high: {high}, split: {split:?}");
            // note: low is inclusive, high is exclusive, start_offset is exclusive
            let search_start = split.start_offset.map_or(low, |offset| offset + 1).max(low);
            if search_start >= high {
                backfill_info.insert(split.id(), BackfillInfo::NoDataToBackfill);
            } else if !properties.enable_partition_eof {
                backfill_info.insert(
                    split.id(),
                    BackfillInfo::HasDataToBackfill {
                        latest_offset: (high - 1).to_string(),
                    },
                );
            } else {
                let latest_offset = find_latest_readable_offset(
                    &consumer,
                    split.topic.as_str(),
                    split.partition,
                    search_start,
                    high,
                    properties.common.sync_call_timeout,
                )
                .await?;
                if let Some(latest_offset) = latest_offset {
                    backfill_info.insert(
                        split.id(),
                        BackfillInfo::HasDataToBackfill {
                            latest_offset: latest_offset.to_string(),
                        },
                    );
                } else {
                    tracing::warn!(
                        topic = %split.topic,
                        partition = split.partition,
                        high_watermark = high,
                        split_id = %split.id(),
                        "kafka watermark range has no readable record under read_committed isolation; skip backfill for this range"
                    );
                    backfill_info.insert(split.id(), BackfillInfo::NoDataToBackfill);
                }
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

async fn find_latest_readable_offset(
    consumer: &StreamConsumer<RwConsumerContext>,
    topic: &str,
    partition: i32,
    search_start: i64,
    search_end_exclusive: i64,
    sync_call_timeout: Duration,
) -> Result<Option<i64>> {
    find_latest_offset_with_data(search_start, search_end_exclusive, |offset| {
        has_readable_data_from(
            consumer,
            topic,
            partition,
            offset,
            sync_call_timeout,
            search_end_exclusive,
        )
    })
    .await
}

async fn find_latest_offset_with_data<E, F, Fut>(
    search_start: i64,
    search_end_exclusive: i64,
    mut has_data_from: F,
) -> std::result::Result<Option<i64>, E>
where
    F: FnMut(i64) -> Fut,
    Fut: Future<Output = std::result::Result<bool, E>>,
{
    if search_start >= search_end_exclusive || !has_data_from(search_start).await? {
        return Ok(None);
    }

    let mut left = search_start;
    let mut right = search_end_exclusive - 1;

    while left < right {
        let mid = left + (right - left + 1) / 2;
        if has_data_from(mid).await? {
            left = mid;
        } else {
            right = mid - 1;
        }
    }

    Ok(Some(left))
}

async fn has_readable_data_from(
    consumer: &StreamConsumer<RwConsumerContext>,
    topic: &str,
    partition: i32,
    start_offset: i64,
    sync_call_timeout: Duration,
    search_end_exclusive: i64,
) -> Result<bool> {
    Ok(first_readable_offset(
        consumer,
        topic,
        partition,
        start_offset,
        sync_call_timeout,
        search_end_exclusive,
    )
    .await?
    .is_some())
}

async fn first_readable_offset(
    consumer: &StreamConsumer<RwConsumerContext>,
    topic: &str,
    partition: i32,
    start_offset: i64,
    sync_call_timeout: Duration,
    search_end_exclusive: i64,
) -> Result<Option<i64>> {
    let mut tpl = TopicPartitionList::with_capacity(1);
    tpl.add_partition_offset(topic, partition, Offset::Offset(start_offset))?;
    consumer.assign(&tpl)?;

    let deadline = tokio::time::Instant::now() + sync_call_timeout;
    loop {
        let timeout = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .ok_or_else(|| anyhow!("timeout while probing readable kafka offset"))?;

        // `recv` may return records after `search_end_exclusive` when new records are appended while
        // we probe. We clamp to the watermark snapshot to keep a stable target offset.
        #[cfg(madsim)]
        let recv_result = tokio::time::timeout(timeout, consumer.stream().next()).await;
        #[cfg(not(madsim))]
        let recv_result = tokio::time::timeout(timeout, async {
            match consumer.recv().await {
                Ok(msg) => Some(Ok(msg)),
                Err(err) => Some(Err(err)),
            }
        })
        .await;
        match recv_result {
            Ok(Some(Ok(msg))) if msg.offset() < start_offset => continue,
            Ok(Some(Ok(msg))) if msg.offset() < search_end_exclusive => {
                return Ok(Some(msg.offset()));
            }
            Ok(Some(Ok(_))) => return Ok(None),
            Ok(Some(Err(KafkaError::PartitionEOF(_)))) => return Ok(None),
            Ok(Some(Err(err))) => return Err(err.into()),
            Ok(None) => return Ok(None),
            Err(_elapsed) => {
                return Err(anyhow!("timeout while probing readable kafka offset").into());
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

        #[for_await]
        'for_outer_loop: for polled in self.consumer.stream().ready_chunks(max_chunk_size) {
            let mut msgs = Vec::new();
            for msg in polled {
                match msg {
                    Ok(msg) => msgs.push(msg),
                    Err(KafkaError::PartitionEOF(_)) => {
                        // `PartitionEOF` is used as a backfill-only control signal to detect that
                        // there is currently no readable data in this partition.
                    }
                    Err(err) => return Err(err.into()),
                }
            }
            if msgs.is_empty() {
                continue;
            }

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

#[cfg(test)]
mod tests {
    use super::find_latest_offset_with_data;

    #[tokio::test]
    async fn test_find_latest_offset_with_data() {
        let visible_offsets = [0, 2, 5, 8];
        let latest = find_latest_offset_with_data(0, 9, |start| async move {
            Ok::<_, ()>(visible_offsets.into_iter().any(|offset| offset >= start))
        })
        .await
        .unwrap();
        assert_eq!(latest, Some(8));
    }

    #[tokio::test]
    async fn test_find_latest_offset_with_data_no_data() {
        let latest = find_latest_offset_with_data(3, 7, |_start| async move { Ok::<_, ()>(false) })
            .await
            .unwrap();
        assert_eq!(latest, None);
    }
}
