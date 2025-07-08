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

use std::fmt::Debug;

use anyhow::anyhow;
use async_trait::async_trait;
use itertools::Itertools;
use pulsar::proto::command_get_topics_of_namespace::Mode as LookupMode;
use pulsar::{Pulsar, TokioExecutor};
use risingwave_common::bail;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::pulsar::PulsarProperties;
use crate::source::pulsar::split::PulsarSplit;
use crate::source::pulsar::topic::{Topic, parse_topic};
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator};

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

    async fn new(
        properties: PulsarProperties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<PulsarSplitEnumerator> {
        let pulsar = properties
            .common
            .build_client(&properties.oauth, &properties.aws_auth_props)
            .await?;
        let topic = properties.common.topic;
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
                    "properties `startup_mode` only supports earliest and latest or leaving it empty"
                );
            }
        };

        if let Some(s) = properties.time_offset {
            let time_offset = s.parse::<i64>().map_err(|e| anyhow!(e))?;
            scan_start_offset = PulsarEnumeratorOffset::Timestamp(time_offset)
        }

        Ok(PulsarSplitEnumerator {
            client: pulsar,
            topic: parsed_topic,
            start_offset: scan_start_offset,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<PulsarSplit>> {
        let offset = self.start_offset.clone();
        // MessageId is only used when recovering from a State
        assert!(!matches!(offset, PulsarEnumeratorOffset::MessageId(_)));

        let topics_on_broker = self
            .client
            .get_topics_of_namespace(self.topic.namespace.clone(), LookupMode::All)
            .await?;
        if !topics_on_broker.contains(&self.topic.to_string()) {
            bail!("topic {} not found on broker, available topics: {:?}", self.topic, topics_on_broker);
        }

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::source::SourceEnumeratorContext;

    #[ignore]
    #[tokio::test]
    async fn test_list_splits() {
        let props = serde_json::from_str::<PulsarProperties>(
            r#"
            {
                "service.url": "pulsar://127.0.0.1:6650",
                "topic": "persistent://public/default/test_topic",
                "scan.startup.mode": "earliest"
            }
            "#,
        )
        .unwrap();
        let mut enumerator =
            PulsarSplitEnumerator::new(props, Arc::new(SourceEnumeratorContext::dummy()))
                .await
                .unwrap();
        let splits = enumerator.list_splits().await.unwrap();
        println!("{:?}", splits);

        panic!()
    }
}
