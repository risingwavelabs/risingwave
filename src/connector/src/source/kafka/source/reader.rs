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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use crate::source::base::{SourceMessage, SplitReader};
use crate::source::kafka::split::KafkaSplit;
use crate::source::kafka::KafkaProperties;
use crate::source::{Column, ConnectorState, SplitImpl};

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

pub struct KafkaSplitReader {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    assigned_splits: HashMap<String, Vec<KafkaSplit>>,
}

#[async_trait]
impl SplitReader for KafkaSplitReader {
    type Properties = KafkaProperties;

    async fn new(
        properties: KafkaProperties,
        state: ConnectorState,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let bootstrap_servers = properties.brokers;

        let mut config = ClientConfig::new();

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", "smallest");
        config.set("bootstrap.servers", bootstrap_servers);

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
            .map_err(|e| anyhow!("consumer creation failed {}", e))?;

        if let Some(splits) = state {
            tracing::debug!("Splits for kafka found! {:?}", splits);
            let mut tpl = TopicPartitionList::with_capacity(splits.len());

            for split in splits {
                if let SplitImpl::Kafka(k) = split {
                    if let Some(offset) = k.start_offset {
                        tpl.add_partition_offset(
                            k.topic.as_str(),
                            k.partition,
                            Offset::Offset(offset),
                        )
                        .map_err(|e| anyhow!(e.to_string()))?;
                    } else {
                        tpl.add_partition(k.topic.as_str(), k.partition);
                    }
                }
            }

            consumer.assign(&tpl).map_err(|e| anyhow!(e.to_string()))?;
        }

        Ok(Self {
            consumer: Arc::new(consumer),
            assigned_splits: HashMap::new(),
        })
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        let mut stream = self
            .consumer
            .stream()
            .ready_chunks(KAFKA_MAX_FETCH_MESSAGES);

        let chunk = match stream.next().await {
            None => return Ok(None),
            Some(chunk) => chunk,
        };

        chunk
            .into_iter()
            .map(|msg| msg.map_err(|e| anyhow!(e)).map(SourceMessage::from))
            .collect::<Result<Vec<SourceMessage>>>()
            .map(Some)
    }
}
