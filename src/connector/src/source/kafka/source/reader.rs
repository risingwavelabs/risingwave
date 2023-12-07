// Copyright 2023 RisingWave Labs
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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem::swap;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::kafka::{
    KafkaProperties, KafkaSplit, PrivateLinkConsumerContext, KAFKA_ISOLATION_LEVEL,
};
use crate::source::{
    into_chunk_stream, BoxSourceWithStateStream, Column, CommonSplitReader, SourceContextRef,
    SplitId, SplitMetaData, SplitReader,
};

pub struct KafkaSplitReader {
    consumer: StreamConsumer<PrivateLinkConsumerContext>,
    offsets: HashMap<SplitId, (Option<i64>, Option<i64>)>,
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

        let bootstrap_servers = &properties.common.brokers;
        let broker_rewrite_map = properties.common.broker_rewrite_map.clone();

        // disable partition eof
        config.set("enable.partition.eof", "false");
        // change to `RdKafkaPropertiesConsumer::enable_auto_commit` to enable auto commit
        // config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", "smallest");
        config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
        config.set("bootstrap.servers", bootstrap_servers);

        properties.common.set_security_properties(&mut config);
        properties.set_client(&mut config);

        config.set(
            "group.id",
            format!(
                "rw-consumer-{}-{}",
                source_ctx.source_info.fragment_id, source_ctx.source_info.actor_id
            ),
        );

        let client_ctx = PrivateLinkConsumerContext::new(
            broker_rewrite_map,
            Some(format!(
                "fragment-{}-source-{}-actor-{}",
                source_ctx.source_info.fragment_id,
                source_ctx.source_info.source_id,
                source_ctx.source_info.actor_id
            )),
            // thread consumer will keep polling in the background, we don't need to call `poll`
            // explicitly
            Some(source_ctx.metrics.rdkafka_native_metric.clone()),
        )?;
        let consumer: StreamConsumer<PrivateLinkConsumerContext> = config
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(client_ctx)
            .await
            .map_err(|e| anyhow!("failed to create kafka consumer: {}", e))?;

        let mut tpl = TopicPartitionList::with_capacity(splits.len());

        let mut offsets = HashMap::new();

        for split in splits {
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
            bytes_per_second,
            max_num_messages,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self, parser_config, source_context)
    }
}

impl KafkaSplitReader {
    fn report_latest_message_id(&self, split_id: &str, offset: i64) {
        self.source_ctx
            .metrics
            .latest_message_id
            .with_label_values(&[
                // source name is not available here
                &self.source_ctx.source_info.source_id.to_string(),
                &self.source_ctx.source_info.actor_id.to_string(),
                split_id,
            ])
            .set(offset);
    }
}

impl CommonSplitReader for KafkaSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
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
        #[for_await]
        'for_outer_loop: for msgs in self.consumer.stream().ready_chunks(max_chunk_size) {
            let msgs: Vec<_> = msgs
                .into_iter()
                .collect::<std::result::Result<_, KafkaError>>()?;

            let mut split_msg_offsets = HashMap::new();

            for msg in &msgs {
                split_msg_offsets.insert(msg.partition(), msg.offset());
            }

            for (partition, offset) in split_msg_offsets {
                let split_id = partition.to_string();
                self.report_latest_message_id(&split_id, offset);
            }

            for msg in msgs {
                let cur_offset = msg.offset();
                bytes_current_second += match &msg.payload() {
                    None => 0,
                    Some(payload) => payload.len(),
                };
                num_messages += 1;
                let source_message = SourceMessage::from_kafka_message(&msg);
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
    }
}
