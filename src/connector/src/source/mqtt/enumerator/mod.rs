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

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock, Weak};

use async_trait::async_trait;
use moka::future::Cache as MokaCache;
use moka::ops::compute::Op;
use risingwave_common::bail;
use rumqttc::v5::{AsyncClient, ConnectionError, Event, EventLoop, Incoming};
use thiserror_ext::AsReport;

use super::MqttProperties;
use super::source::MqttSplit;
use crate::error::{ConnectorError, ConnectorResult};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

/// Consumer client is shared, and the cache doesn't manage the lifecycle, so we store `Weak` and no eviction.
static SHARED_MQTT_CLIENT: LazyLock<MokaCache<String, Weak<MqttConnectionCheck>>> =
    LazyLock::new(|| moka::future::Cache::builder().build());

pub struct MqttSplitEnumerator {
    topic: String,
    broker: String,
    connection_check: Arc<MqttConnectionCheck>,
}

struct MqttConnectionCheck {
    _client: AsyncClient,
    connected: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}

impl MqttConnectionCheck {
    fn new(client: AsyncClient, event_loop: EventLoop, topic: String) -> Self {
        let this = Self {
            _client: client,
            connected: Arc::new(AtomicBool::new(false)),
            stopped: Arc::new(AtomicBool::new(false)),
        };
        this.spawn_client_loop(event_loop, topic);
        this
    }

    fn is_connected(&self) -> bool {
        self.connected.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn spawn_client_loop(&self, mut event_loop: EventLoop, topic: String) {
        let connected_clone = self.connected.clone();
        let stopped_clone = self.stopped.clone();
        tokio::spawn(async move {
            while !stopped_clone.load(std::sync::atomic::Ordering::Relaxed) {
                match event_loop.poll().await {
                    Ok(Event::Incoming(Incoming::ConnAck(_)))
                        if !connected_clone.load(std::sync::atomic::Ordering::Relaxed) =>
                    {
                        connected_clone.store(true, std::sync::atomic::Ordering::Relaxed);
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
    }
}

impl Drop for MqttConnectionCheck {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

#[async_trait]
impl SplitEnumerator for MqttSplitEnumerator {
    type Properties = MqttProperties;
    type Split = MqttSplit;

    async fn new(
        properties: Self::Properties,
        context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<MqttSplitEnumerator> {
        let broker_url = properties.common.url.clone();
        let mut connection_check: Option<Arc<MqttConnectionCheck>> = None;

        SHARED_MQTT_CLIENT
            .entry_by_ref(&properties.common.url)
            .and_try_compute_with::<_, _, ConnectorError>(|entry| async {
                if let Some(_connection_check) = entry.and_then(|e| e.into_value().upgrade()) {
                    // return if the client is already built
                    tracing::debug!("reuse existing mqtt client for {}", broker_url);
                    connection_check = Some(_connection_check);
                    return Ok(Op::Nop);
                } else {
                    tracing::debug!("build new mqtt client for {}", broker_url);
                    let (new_client, event_loop) =
                        properties.common.build_client(context.info.source_id, 0)?;
                    let _connection_check = Arc::new(MqttConnectionCheck::new(new_client, event_loop, properties.topic.clone()));
                    connection_check = Some(_connection_check.clone());
                    Ok(Op::Put(Arc::downgrade(&_connection_check)))
                }
            })
            .await?;

        let Some(client_wrapper) = connection_check else {
            bail!("failed to create or get mqtt client for {}", broker_url);
        };

        Ok(Self {
            topic: properties.topic,
            broker: broker_url,
            connection_check: client_wrapper,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<MqttSplit>> {
        if !self.connection_check.is_connected() {
            let start = std::time::Instant::now();
            loop {
                if self.connection_check.is_connected() {
                    break;
                };
                if start.elapsed().as_secs() > 10 {
                    bail!("Failed to connect to mqtt broker");
                }

                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
        tracing::debug!("found new splits {} for broker {}", self.topic, self.broker);
        Ok(vec![MqttSplit::new(self.topic.clone())])
    }
}
