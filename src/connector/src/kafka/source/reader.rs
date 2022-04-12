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
use rdkafka::consumer::{DefaultConsumerContext, StreamConsumer};
use rdkafka::ClientConfig;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::RwError;

use crate::base::{InnerMessage, SourceReader};
use crate::kafka::split::KafkaSplit;
use crate::kafka::KAFKA_CONFIG_BROKER_KEY;

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

pub struct KafkaSplitReader {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
    assigned_splits: HashMap<String, Vec<KafkaSplit>>,
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

        chunk
            .into_iter()
            .map(|msg| msg.map_err(|e| anyhow!(e)).map(InnerMessage::from))
            .collect::<Result<Vec<InnerMessage>>>()
            .map(Some)
    }

    async fn new(
        properties: HashMap<String, String>,
        _state: Option<crate::ConnectorState>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let bootstrap_servers = properties.get(KAFKA_CONFIG_BROKER_KEY).ok_or_else(|| {
            RwError::from(ProtocolError(format!(
                "could not found config {}",
                KAFKA_CONFIG_BROKER_KEY
            )))
        })?;

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
