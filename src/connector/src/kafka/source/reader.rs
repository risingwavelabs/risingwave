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
use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::RwError;

use crate::base::{InnerMessage, SourceReader};
use crate::kafka::split::{KafkaOffset, KafkaSplit};
use crate::ConnectorState;

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

const KAFKA_CONFIG_TOPIC_KEY: &str = "kafka.topic";
const KAFKA_CONFIG_BOOTSTRAP_SERVER_KEY: &str = "kafka.bootstrap.servers";

pub struct KafkaSplitReader {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    assigned_splits: HashMap<String, Vec<KafkaSplit>>,
    // partition_queue: StreamPartitionQueue<DefaultConsumerContext>,
    // topic: String,
    // assigned_split: KafkaSplit,
}

#[async_trait]
impl SourceReader for KafkaSplitReader {
    async fn next(&mut self) -> Result<Option<Vec<InnerMessage>>> {
        let mut stream = self
            .consumer
            .stream()
            .ready_chunks(KAFKA_MAX_FETCH_MESSAGES);

        let chunk = match stream.next().await {
            None => return Ok(None),
            Some(chunk) => chunk,
        };

        let mut ret = Vec::with_capacity(chunk.len());

        for msg in chunk {
            let msg = msg.map_err(|e| anyhow!(e))?;

            // let offset = msg.offset();
            //
            // let topic =
            //
            // if let KafkaOffset::Offset(stopping_offset) = self.assigned_split.stop_offset {
            //     if offset >= stopping_offset {
            //         // `self.partition_queue` will expire when it's done
            //         // FIXME(chen): error handling
            //         self.consumer
            //             .assign(&TopicPartitionList::new())
            //             .map_err(|e| anyhow!(e))?;
            //
            //         break;
            //     }
            // }

            ret.push(InnerMessage::from(msg));
        }

        Ok(Some(ret))
    }

    async fn new(
        properties: HashMap<String, String>,
        state: Option<crate::ConnectorState>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let bootstrap_servers =
            properties
                .get(KAFKA_CONFIG_BOOTSTRAP_SERVER_KEY)
                .ok_or(RwError::from(ProtocolError(format!(
                    "could not found config {}",
                    KAFKA_CONFIG_BOOTSTRAP_SERVER_KEY
                ))))?;

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

        let consumer = config
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(DefaultConsumerContext)
            .map_err(|e| RwError::from(InternalError(format!("consumer creation failed {}", e))))?;

        // if let Some(state) = state {
        //     let identifier = state.identifier;
        //     serde_json::from_str()
        // }

        Ok(Self {
            consumer: Arc::new(consumer),
            assigned_splits: HashMap::new(),
        })
    }
}

// impl KafkaSplitReader {
//     fn create_consumer(&self) -> Result<StreamConsumer<DefaultConsumerContext>> {
//         let mut config = ClientConfig::new();
//
//         config.set("topic.metadata.refresh.interval.ms", "30000");
//         config.set("fetch.message.max.bytes", "134217728");
//         config.set("auto.offset.reset", "earliest");
//
//         if config.get("group.id").is_none() {
//             config.set(
//                 "group.id",
//                 format!(
//                     "consumer-{}",
//                     SystemTime::now()
//                         .duration_since(UNIX_EPOCH)
//                         .unwrap()
//                         .as_micros()
//                 ),
//             );
//         }
//
//         // disable partition eof
//         config.set("enable.partition.eof", "false");
//         config.set("enable.auto.commit", "false");
//         // config.set("bootstrap.servers", self.bootstrap_servers.join(","));
//
//         config
//             .set_log_level(RDKafkaLogLevel::Debug)
//             .create_with_context(DefaultConsumerContext)
//             .map_err(|e| anyhow!(e))
//     }
// }
