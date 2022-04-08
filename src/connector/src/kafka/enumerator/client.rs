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

use std::collections::HashMap;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::{Offset, TopicPartitionList};

use crate::base::SplitEnumerator;
use crate::kafka::split::{KafkaOffset, KafkaSplit};
use crate::kafka::{KAFKA_CONFIG_BROKER_KEY, KAFKA_CONFIG_TOPIC_KEY, KAFKA_SYNC_CALL_TIMEOUT};

pub struct KafkaSplitEnumerator {
    broker_address: String,
    topic: String,
    admin_client: BaseConsumer,
    start_offset: KafkaOffset,
    stop_offset: KafkaOffset,
}

impl KafkaSplitEnumerator {
    pub fn new(properties: &HashMap<String, String>) -> Result<KafkaSplitEnumerator> {
        let broker_address = properties
            .get(KAFKA_CONFIG_BROKER_KEY)
            .ok_or_else(|| anyhow!("broker_address not found"))?;
        let topic = properties
            .get(KAFKA_CONFIG_TOPIC_KEY)
            .ok_or_else(|| anyhow!("topic not found"))?;

        let client: BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", broker_address)
            .create_with_context(DefaultConsumerContext)
            .unwrap();

        Ok(Self {
            broker_address: broker_address.clone(),
            topic: topic.clone(),
            admin_client: client,
            // todo
            start_offset: KafkaOffset::Earliest,
            stop_offset: KafkaOffset::None,
        })
    }
}

#[async_trait]
impl SplitEnumerator for KafkaSplitEnumerator {
    type Split = KafkaSplit;

    async fn list_splits(&mut self) -> anyhow::Result<Vec<KafkaSplit>> {
        let topic_partitions = self.fetch_topic_partition()?;

        let mut start_offsets = self
            .fetch_start_offset(topic_partitions.as_ref())
            .map_err(|e| anyhow!("{}", e))?;

        let mut stop_offsets = self
            .fetch_stop_offset(topic_partitions.as_ref())
            .map_err(|e| anyhow!("{}", e))?;

        let ret = topic_partitions
            .into_iter()
            .map(|partition| KafkaSplit {
                topic: self.topic.clone(),
                partition,
                start_offset: start_offsets.remove(&partition).unwrap(),
                stop_offset: stop_offsets.remove(&partition).unwrap(),
            })
            .collect();

        Ok(ret)
    }
}

impl KafkaSplitEnumerator {
    fn fetch_stop_offset(&self, partitions: &[i32]) -> KafkaResult<HashMap<i32, KafkaOffset>> {
        match self.stop_offset {
            KafkaOffset::Earliest => unreachable!(),
            KafkaOffset::Latest => partitions
                .iter()
                .map(|partition| {
                    self.admin_client
                        .fetch_watermarks(self.topic.as_str(), *partition, KAFKA_SYNC_CALL_TIMEOUT)
                        .map(|watermark| (*partition, KafkaOffset::Offset(watermark.1)))
                })
                .collect(),
            KafkaOffset::Offset(offset) => partitions
                .iter()
                .map(|partition| Ok((*partition, KafkaOffset::Offset(offset))))
                .collect(),
            KafkaOffset::Timestamp(time) => self.fetch_offset_for_time(partitions, time),
            KafkaOffset::None => partitions
                .iter()
                .map(|partition| Ok((*partition, KafkaOffset::None)))
                .collect(),
        }
    }

    fn fetch_start_offset(&self, partitions: &[i32]) -> KafkaResult<HashMap<i32, KafkaOffset>> {
        match self.start_offset {
            KafkaOffset::Earliest | KafkaOffset::Latest => partitions
                .iter()
                .map(|partition| {
                    self.admin_client
                        .fetch_watermarks(self.topic.as_str(), *partition, KAFKA_SYNC_CALL_TIMEOUT)
                        .map(|watermark| match self.start_offset {
                            KafkaOffset::Earliest => KafkaOffset::Offset(watermark.0),
                            KafkaOffset::Latest => KafkaOffset::Offset(watermark.1),
                            _ => unreachable!(),
                        })
                        .map(|offset| (*partition, offset))
                })
                .collect(),

            KafkaOffset::Offset(offset) => partitions
                .iter()
                .map(|partition| Ok((*partition, KafkaOffset::Offset(offset))))
                .collect(),

            KafkaOffset::Timestamp(time) => self.fetch_offset_for_time(partitions, time),

            KafkaOffset::None => partitions
                .iter()
                .map(|partition| Ok((*partition, KafkaOffset::None)))
                .collect(),
        }
    }

    fn fetch_offset_for_time(
        &self,
        partitions: &[i32],
        time: i64,
    ) -> KafkaResult<HashMap<i32, KafkaOffset>> {
        let mut tpl = TopicPartitionList::new();

        for partition in partitions {
            tpl.add_partition_offset(self.topic.as_str(), *partition, Offset::Offset(time))?;
        }

        let offsets = self
            .admin_client
            .offsets_for_times(tpl, KAFKA_SYNC_CALL_TIMEOUT)?;

        let mut result = HashMap::with_capacity(partitions.len());

        for elem in offsets.elements_for_topic(self.topic.as_str()) {
            match elem.offset() {
                Offset::Offset(offset) => {
                    result.insert(elem.partition(), KafkaOffset::Offset(offset));
                }
                _ => {
                    let (_, high_watermark) = self.admin_client.fetch_watermarks(
                        self.topic.as_str(),
                        elem.partition(),
                        KAFKA_SYNC_CALL_TIMEOUT,
                    )?;
                    result.insert(elem.partition(), KafkaOffset::Offset(high_watermark));
                }
            }
        }

        Ok(result)
    }

    fn fetch_topic_partition(&mut self) -> anyhow::Result<Vec<i32>> {
        // for now, we only support one topic
        let metadata = self
            .admin_client
            .fetch_metadata(Some(self.topic.as_str()), KAFKA_SYNC_CALL_TIMEOUT)?;

        let topic_meta = match metadata.topics() {
            [meta] => meta,
            _ => return Err(anyhow!("topic {} not found", self.topic)),
        };

        Ok(topic_meta
            .partitions()
            .iter()
            .map(|partition| partition.id())
            .collect())
    }
}
