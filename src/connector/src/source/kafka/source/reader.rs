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
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Offset, TopicPartitionList};

use crate::source::base::{SourceMessage, SplitReader, MAX_CHUNK_SIZE};
use crate::source::kafka::split::KafkaSplit;
use crate::source::kafka::KafkaProperties;
use crate::source::{BoxSourceStream, Column, ConnectorState, SplitImpl};

pub struct KafkaSplitReader {
    consumer: StreamConsumer<DefaultConsumerContext>,
    assigned_splits: HashMap<String, Vec<KafkaSplit>>,
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

        if let Some(splits) = state {
            let mut tpl = TopicPartitionList::with_capacity(splits.len());

            for split in &splits {
                if let SplitImpl::Kafka(k) = split {
                    if let Some(offset) = k.start_offset {
                        tpl.add_partition_offset(
                            k.topic.as_str(),
                            k.partition,
                            Offset::Offset(offset + 1),
                        )?;
                    } else {
                        tpl.add_partition(k.topic.as_str(), k.partition);
                    }
                }
            }

            consumer.assign(&tpl)?;
        }

        Ok(Self {
            consumer,
            assigned_splits: HashMap::new(),
        })
    }

    fn into_stream(self) -> BoxSourceStream {
        self.into_stream()
    }
}

impl KafkaSplitReader {
    #[try_stream(boxed, ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn into_stream(self) {
        #[for_await]
        for msgs in self.consumer.stream().ready_chunks(MAX_CHUNK_SIZE) {
            let mut res = Vec::with_capacity(msgs.len());
            for msg in msgs {
                res.push(SourceMessage::from(msg?));
            }
            yield res;
        }
    }
}
