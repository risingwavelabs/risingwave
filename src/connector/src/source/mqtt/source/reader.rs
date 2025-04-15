// Copyright 2025 RisingWave Labs
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

use async_trait::async_trait;
use futures_async_stream::try_stream;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::Filter;
use rumqttc::v5::{ConnectionError, Event, Incoming};
use thiserror_ext::AsReport;

use super::MqttSplit;
use super::message::MqttMessage;
use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::common::into_chunk_stream;
use crate::source::mqtt::MqttProperties;
use crate::source::{BoxSourceChunkStream, Column, SourceContextRef, SourceMessage, SplitReader};

pub struct MqttSplitReader {
    eventloop: rumqttc::v5::EventLoop,
    #[expect(dead_code)]
    client: rumqttc::v5::AsyncClient,
    #[expect(dead_code)]
    qos: QoS,
    #[expect(dead_code)]
    splits: Vec<MqttSplit>,
    #[expect(dead_code)]
    properties: MqttProperties,
    parser_config: ParserConfig,
    source_ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for MqttSplitReader {
    type Properties = MqttProperties;
    type Split = MqttSplit;

    async fn new(
        properties: MqttProperties,
        splits: Vec<MqttSplit>,
        parser_config: ParserConfig,
        source_ctx: SourceContextRef,
        _columns: Option<Vec<Column>>,
    ) -> Result<Self> {
        let (client, eventloop) = properties
            .common
            .build_client(source_ctx.actor_id, source_ctx.fragment_id as u64)
            .inspect_err(|e| tracing::error!("Failed to build mqtt client: {}", e.as_report()))?;

        let qos = properties.common.qos();

        client
            .subscribe_many(
                splits
                    .iter()
                    .cloned()
                    .map(|split| Filter::new(split.topic, qos)),
            )
            .await?;

        Ok(Self {
            eventloop,
            client,
            qos,
            splits,
            properties,
            parser_config,
            source_ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser_config.clone();
        let source_context = self.source_ctx.clone();
        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }
}

impl MqttSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        let mut eventloop = self.eventloop;
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    let msg = MqttMessage::new(p);
                    yield vec![SourceMessage::from(msg)];
                }
                Ok(_) => (),
                Err(ConnectionError::Timeout(_) | ConnectionError::RequestsDone) => {
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}
