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

use std::mem::swap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use crate::impl_common_split_reader_logic;
use crate::parser::ParserConfig;
use crate::source::base::{SourceMessage, MAX_CHUNK_SIZE};
use crate::source::kafka::{KafkaProperties, PrivateLinkConsumerContext, KAFKA_ISOLATION_LEVEL};
use crate::source::{
    BoxSourceWithStateStream, Column, SourceContextRef, SplitId, SplitImpl, SplitMetaData,
    SplitReader,
};

impl_common_split_reader_logic!(KafkaSplitReader, KafkaProperties);

pub struct KafkaSplitReader {
    consumer: StreamConsumer<PrivateLinkConsumerContext>,
    start_offset: Option<i64>,
    stop_offset: Option<i64>,
    bytes_per_second: usize,
    max_num_messages: usize,
    enable_upsert: bool,

    split_id: SplitId,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for KafkaSplitReader {
    type Properties = KafkaProperties;

    async fn new(
        properties: KafkaProperties,
        splits: Vec<SplitImpl>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let mut config = ClientConfig::new();

        let bootstrap_servers = &properties.common.brokers;
        let broker_rewrite_map = properties.common.broker_rewrite_map.clone();

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", "smallest");
        config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
        config.set("bootstrap.servers", bootstrap_servers);

        properties.common.set_security_properties(&mut config);

        if config.get("group.id").is_none() {
            config.set(
                "group.id",
                format!(
                    "consumer-{}",
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros()
                ),
            );
        }

        let client_ctx = PrivateLinkConsumerContext::new(broker_rewrite_map)?;
        let consumer: StreamConsumer<PrivateLinkConsumerContext> = config
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(client_ctx)
            .await
            .map_err(|e| anyhow!("failed to create kafka consumer: {}", e))?;

        let mut start_offset = None;

        assert_eq!(splits.len(), 1);
        let mut tpl = TopicPartitionList::with_capacity(splits.len());

        let split = splits.into_iter().next().unwrap().into_kafka().unwrap();

        let split_id = split.id();

        if let Some(offset) = split.start_offset {
            start_offset = Some(offset);
            tpl.add_partition_offset(
                split.topic.as_str(),
                split.partition,
                Offset::Offset(offset + 1),
            )?;
        } else {
            tpl.add_partition(split.topic.as_str(), split.partition);
        }
        let stop_offset = split.stop_offset;

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
            start_offset,
            stop_offset,
            bytes_per_second,
            max_num_messages,
            split_id,
            enable_upsert: parser_config.specific.is_upsert(),
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceWithStateStream {
        self.into_chunk_stream()
    }
}

impl KafkaSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn into_data_stream(self) {
        if let Some(stop_offset) = self.stop_offset {
            if let Some(start_offset) = self.start_offset && (start_offset+1) >= stop_offset {
                yield Vec::new();
                return Ok(());
            } else if stop_offset == 0 {
                yield Vec::new();
                return Ok(());
            }
        }
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.tick().await;
        let mut bytes_current_second = 0;
        let mut num_messages = 0;
        let mut res = Vec::with_capacity(MAX_CHUNK_SIZE);
        #[for_await]
        'for_outer_loop: for msgs in self.consumer.stream().ready_chunks(MAX_CHUNK_SIZE) {
            for msg in msgs {
                let msg = msg?;
                let cur_offset = msg.offset();
                bytes_current_second += match &msg.payload() {
                    None => 0,
                    Some(payload) => payload.len(),
                };
                num_messages += 1;
                if self.enable_upsert {
                    res.push(SourceMessage::from_kafka_message_upsert(msg));
                } else {
                    res.push(SourceMessage::from(msg));
                }

                if let Some(stop_offset) = self.stop_offset {
                    if cur_offset == stop_offset - 1 {
                        tracing::debug!(
                            "stop offset reached, stop reading, offset: {}, stop offset: {}",
                            cur_offset,
                            stop_offset
                        );
                        yield res;
                        break 'for_outer_loop;
                    }
                }
                // This judgement has to be put in the inner loop as `msgs` can be multiple ones.
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
