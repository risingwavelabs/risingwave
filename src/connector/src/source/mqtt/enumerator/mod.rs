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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use async_trait::async_trait;
use risingwave_common::bail;
use rumqttc::Outgoing;
use rumqttc::v5::{ConnectionError, Event, Incoming};
use thiserror_ext::AsReport;
use tokio::sync::RwLock;

use super::MqttProperties;
use super::source::MqttSplit;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub struct MqttSplitEnumerator {
    #[expect(dead_code)]
    topic: String,
    #[expect(dead_code)]
    client: rumqttc::v5::AsyncClient,
    topics: Arc<RwLock<HashSet<String>>>,
    connected: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}

#[async_trait]
impl SplitEnumerator for MqttSplitEnumerator {
    type Properties = MqttProperties;
    type Split = MqttSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<MqttSplitEnumerator> {
        let (client, mut eventloop) = properties.common.build_client(context.info.source_id, 0)?;

        let topic = properties.topic.clone();
        let mut topics = HashSet::new();
        if !topic.contains('#') && !topic.contains('+') {
            topics.insert(topic.clone());
        }

        client
            .subscribe(topic.clone(), rumqttc::v5::mqttbytes::QoS::AtMostOnce)
            .await?;

        let topics = Arc::new(RwLock::new(topics));

        let connected = Arc::new(AtomicBool::new(false));
        let connected_clone = connected.clone();

        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_clone = stopped.clone();

        let topics_clone = topics.clone();
        tokio::spawn(async move {
            while !stopped_clone.load(std::sync::atomic::Ordering::Relaxed) {
                match eventloop.poll().await {
                    Ok(Event::Outgoing(Outgoing::Subscribe(_))) => {
                        connected_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        let topic = String::from_utf8_lossy(&p.topic).to_string();
                        let exist = {
                            let topics = topics_clone.read().await;
                            topics.contains(&topic)
                        };

                        if !exist {
                            let mut topics = topics_clone.write().await;
                            topics.insert(topic);
                        }
                    }
                    Ok(_)
                    | Err(ConnectionError::Timeout(_))
                    | Err(ConnectionError::RequestsDone) => {}
                    Err(err) => {
                        tracing::error!(
                            "Failed to fetch splits to topic {}: {}",
                            topic,
                            err.as_report(),
                        );
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await
                    }
                }
            }
        });

        Ok(Self {
            client,
            topics,
            topic: properties.topic,
            connected,
            stopped,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<MqttSplit>> {
        if !self.connected.load(std::sync::atomic::Ordering::Relaxed) {
            let start = std::time::Instant::now();
            loop {
                if self.connected.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                if start.elapsed().as_secs() > 10 {
                    bail!("Failed to connect to mqtt broker");
                }

                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }

        let topics = self.topics.read().await;
        Ok(topics.iter().cloned().map(MqttSplit::new).collect())
    }
}

impl Drop for MqttSplitEnumerator {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
