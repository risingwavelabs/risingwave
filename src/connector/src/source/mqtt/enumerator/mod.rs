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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use async_trait::async_trait;
use risingwave_common::bail;
use rumqttc::v5::{ConnectionError, Event, Incoming};
use thiserror_ext::AsReport;

use super::MqttProperties;
use super::source::MqttSplit;
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

pub struct MqttSplitEnumerator {
    topic: String,
    #[expect(dead_code)]
    client: rumqttc::v5::AsyncClient,
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

        let connected = Arc::new(AtomicBool::new(false));
        let connected_clone = connected.clone();

        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_clone = stopped.clone();

        tokio::spawn(async move {
            while !stopped_clone.load(std::sync::atomic::Ordering::Relaxed) {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::ConnAck(_))) => {
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
        Ok(Self {
            topic: properties.topic,
            client,
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
        Ok(vec![MqttSplit::new(self.topic.clone())])
    }
}

impl Drop for MqttSplitEnumerator {
    fn drop(&mut self) {
        self.stopped
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
