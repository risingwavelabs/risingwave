// Copyright 2023 Singularity Data
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

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};

use crate::source::base::{SourceMessage, SplitReader, MAX_CHUNK_SIZE};
use crate::source::kafka::KafkaProperties;
use crate::source::{BoxSourceStream, Column, ConnectorState, SplitImpl};

pub struct KafkaSplitReader {
    consumer: StreamConsumer<DefaultConsumerContext>,
    start_offset: Option<i64>,
    stop_offset: Option<i64>,
    bytes_per_second: usize,
    max_num_messages: usize,
}

#[async_trait]
impl SplitReader for KafkaSplitReader {
    type Properties = KafkaProperties;

    async fn new(
        properties: KafkaProperties,
        state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let bootstrap_servers = &properties.brokers;

        let mut config = ClientConfig::new();

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", "smallest");
        config.set("bootstrap.servers", bootstrap_servers);

        properties.set_security_properties(&mut config);

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

        let consumer: StreamConsumer = config
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(DefaultConsumerContext)
            .await
            .context("failed to create kafka consumer")?;

        let mut start_offset = None;
        let mut stop_offset = None;
        if let Some(splits) = state {
            assert_eq!(splits.len(), 1);
            let mut tpl = TopicPartitionList::with_capacity(splits.len());

            for split in &splits {
                if let SplitImpl::Kafka(k) = split {
                    if let Some(offset) = k.start_offset {
                        start_offset = Some(offset);
                        tpl.add_partition_offset(
                            k.topic.as_str(),
                            k.partition,
                            Offset::Offset(offset + 1),
                        )?;
                    } else {
                        tpl.add_partition(k.topic.as_str(), k.partition);
                    }
                    stop_offset = k.stop_offset;
                }
            }

            consumer.assign(&tpl)?;
        }

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
        })
    }

    fn into_stream(self) -> BoxSourceStream {
        self.into_stream()
    }
}

impl KafkaSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn into_stream(self) {
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
                res.push(SourceMessage::from(msg));
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
