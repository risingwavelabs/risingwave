// Copyright 2024 RisingWave Labs
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

use std::collections::HashSet;

use async_trait::async_trait;
use risingwave_common::bail;
use rumqttc::v5::{Event, Incoming};
use rumqttc::Outgoing;

use super::source::MqttSplit;
use super::MqttProperties;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub struct MqttSplitEnumerator {
    topic: String,
    client: rumqttc::v5::AsyncClient,
    eventloop: rumqttc::v5::EventLoop,
}

#[async_trait]
impl SplitEnumerator for MqttSplitEnumerator {
    type Properties = MqttProperties;
    type Split = MqttSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<MqttSplitEnumerator> {
        let (client, eventloop) = properties.common.build_client(context.info.source_id)?;

        Ok(Self {
            topic: properties.common.topic,
            client,
            eventloop,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<MqttSplit>> {
        if !self.topic.contains('#') && !self.topic.contains('+') {
            self.client
                .subscribe(self.topic.clone(), rumqttc::v5::mqttbytes::QoS::AtLeastOnce)
                .await?;

            let start = std::time::Instant::now();
            loop {
                match self.eventloop.poll().await {
                    Ok(Event::Outgoing(Outgoing::Subscribe(_))) => {
                        break;
                    }
                    _ => {
                        if start.elapsed().as_secs() > 5 {
                            bail!("Failed to subscribe to topic {}", self.topic);
                        }
                    }
                }
            }
            self.client.unsubscribe(self.topic.clone()).await?;

            return Ok(vec![MqttSplit::new(self.topic.clone())]);
        }

        self.client
            .subscribe(self.topic.clone(), rumqttc::v5::mqttbytes::QoS::AtLeastOnce)
            .await?;

        let start = std::time::Instant::now();
        let mut topics = HashSet::new();
        loop {
            match self.eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    topics.insert(String::from_utf8_lossy(&p.topic).to_string());
                }
                _ => {
                    if start.elapsed().as_secs() > 15 {
                        self.client.unsubscribe(self.topic.clone()).await?;
                        if topics.is_empty() {
                            tracing::warn!(
                                "Failed to find any topics for pattern {}, using a single split",
                                self.topic
                            );
                            return Ok(vec![MqttSplit::new(self.topic.clone())]);
                        }
                        return Ok(topics.into_iter().map(MqttSplit::new).collect());
                    }
                }
            }
        }
    }
}
