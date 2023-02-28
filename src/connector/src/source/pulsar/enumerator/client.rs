// Copyright 2023 RisingWave Labs
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

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use itertools::Itertools;
use pulsar::{Authentication, Pulsar, TokioExecutor};
use serde::{Deserialize, Serialize};

use crate::source::pulsar::split::PulsarSplit;
use crate::source::pulsar::topic::{parse_topic, Topic};
use crate::source::pulsar::PulsarProperties;
use crate::source::SplitEnumerator;

pub struct PulsarSplitEnumerator {
    client: Pulsar<TokioExecutor>,
    topic: Topic,
    start_offset: PulsarEnumeratorOffset,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum PulsarEnumeratorOffset {
    Earliest,
    Latest,
    MessageId(String),
    Timestamp(i64),
}

#[async_trait]
impl SplitEnumerator for PulsarSplitEnumerator {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(properties: PulsarProperties) -> Result<PulsarSplitEnumerator> {
        let topic = properties.topic;
        let parsed_topic = parse_topic(&topic)?;

        let mut scan_start_offset = match properties
            .scan_startup_mode
            .map(|s| s.to_lowercase())
            .as_deref()
        {
            Some("earliest") => PulsarEnumeratorOffset::Earliest,
            Some("latest") => PulsarEnumeratorOffset::Latest,
            None => PulsarEnumeratorOffset::Earliest,
            _ => {
                bail!(
                    "properties `startup_mode` only support earliest and latest or leave it empty"
                );
            }
        };

        if let Some(s) = properties.time_offset {
            let time_offset = s.parse::<i64>().map_err(|e| anyhow!(e))?;
            scan_start_offset = PulsarEnumeratorOffset::Timestamp(time_offset)
        }

        let mut pulsar_builder = Pulsar::builder(properties.service_url, TokioExecutor);
        if let Some(auth_token) = properties.auth_token {
            pulsar_builder = pulsar_builder.with_auth(Authentication {
                name: "token".to_string(),
                data: Vec::from(auth_token),
            });
        }

        let pulsar = pulsar_builder.build().await.map_err(|e| anyhow!(e))?;

        Ok(PulsarSplitEnumerator {
            client: pulsar,
            topic: parsed_topic,
            start_offset: scan_start_offset,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<PulsarSplit>> {
        let offset = self.start_offset.clone();
        // MessageId is only used when recovering from a State
        assert!(!matches!(offset, PulsarEnumeratorOffset::MessageId(_)));

        let topic_partitions = self
            .client
            .lookup_partitioned_topic_number(&self.topic.to_string())
            .await
            .map_err(|e| anyhow!(e))?;

        let splits = if topic_partitions > 0 {
            // partitioned topic
            (0..topic_partitions as i32)
                .map(|p| PulsarSplit {
                    topic: self.topic.sub_topic(p).unwrap(),
                    start_offset: offset.clone(),
                })
                .collect_vec()
        } else {
            // non partitioned topic
            vec![PulsarSplit {
                topic: self.topic.clone(),
                start_offset: offset.clone(),
            }]
        };

        Ok(splits)
    }
}
