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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use itertools::Itertools;

use crate::base::SplitEnumerator;
use crate::pulsar::admin::PulsarAdminClient;
use crate::pulsar::split::{PulsarSplit};
use crate::pulsar::topic::{parse_topic, ParsedTopic};
use crate::pulsar::{PULSAR_CONFIG_ADMIN_URL_KEY, PULSAR_CONFIG_SCAN_STARTUP_MODE, PULSAR_CONFIG_TOPIC_KEY};
use crate::utils::AnyhowProperties;
use serde::{Serialize, Deserialize}; // 1.0.124

pub struct PulsarSplitEnumerator {
    admin_client: PulsarAdminClient,
    topic: ParsedTopic,
    start_offset: PulsarEnumeratorOffset,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum PulsarEnumeratorOffset {
    Earliest,
    Latest,
    MessageId(String),
    Timestamp(u64),
}

impl PulsarSplitEnumerator {
    pub(crate) fn new(properties: &AnyhowProperties) -> Result<PulsarSplitEnumerator> {
        let topic = properties.get_pulsar(PULSAR_CONFIG_TOPIC_KEY)?;
        let admin_url = properties.get_pulsar(PULSAR_CONFIG_ADMIN_URL_KEY)?;
        let parsed_topic = parse_topic(&topic)?;

        let mut scan_start_offset = match properties
            .0
            .get(PULSAR_CONFIG_SCAN_STARTUP_MODE)
            .map(String::as_str)
        {
            Some("earliest") => PulsarEnumeratorOffset::Earliest,
            Some("latest") => PulsarEnumeratorOffset::Latest,
            None => PulsarEnumeratorOffset::Earliest,
            _ => {
                return Err(anyhow!(
                    "properties {} only support earliest and latest or leave it empty",
                    PULSAR_CONFIG_SCAN_STARTUP_MODE
                ));
            }
        };

        // if let Some(s) = properties.0.get(KAFKA_CONFIG_TIME_OFFSET) {
        //     let time_offset = s.parse::<i64>().map_err(|e| anyhow!(e))?;
        //     scan_start_offset = KafkaEnumeratorOffset::Timestamp(time_offset)
        // }

        // todo handle offset init
        Ok(PulsarSplitEnumerator {
            admin_client: PulsarAdminClient::new(admin_url),
            topic: parsed_topic,
            start_offset: scan_start_offset,
        })
    }
}

impl PulsarSplitEnumerator {}

#[async_trait]
impl SplitEnumerator for PulsarSplitEnumerator {
    type Split = PulsarSplit;

    async fn list_splits(&mut self) -> anyhow::Result<Vec<PulsarSplit>> {
        match self.topic.partition_index {
            // partitioned topic
            None => {
                self.admin_client.get_topic_metadata(&self.topic).await.and_then(|meta| {
                    Ok((0..meta.partitions)
                        .into_iter()
                        .map(|p| {
                            PulsarSplit {
                                topic: self.topic.sub_topic(p as i32),
                                start_offset: self.start_offset.clone(),
                            }
                        })
                        .collect_vec())
                })
            }
            // non partitioned topic
            Some(_) => {
                // we need to check topic exists
                self.admin_client.get_topic_metadata(&self.topic).await.and_then(|_| {
                    Ok(vec![PulsarSplit {
                        topic: self.topic.clone(),
                        start_offset: self.start_offset.clone(),
                    }])
                })
            }
        }
    }
}


#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use crate::{AnyhowProperties, SplitEnumerator};
    use crate::pulsar::{PULSAR_CONFIG_ADMIN_URL_KEY, PULSAR_CONFIG_TOPIC_KEY, PulsarEnumeratorOffset, PulsarSplitEnumerator};

    #[tokio::test]
    async fn test_tmp() {
        let offset = vec![PulsarEnumeratorOffset::Latest, PulsarEnumeratorOffset::Earliest, PulsarEnumeratorOffset::MessageId("1:2:3:4".to_string())];

        let s = serde_json::to_string(&offset).unwrap();

        println!("{}", s);

        let x: Vec<PulsarEnumeratorOffset> = serde_json::from_str(s.as_str()).unwrap();
        println!("{:?}", x);

        // let mut prop = HashMap::new();
        // prop.insert(PULSAR_CONFIG_ADMIN_URL_KEY.to_string(), "http://localhost:8080".to_string());
        // prop.insert(PULSAR_CONFIG_TOPIC_KEY.to_string(), "t2".to_string());
        // let properties = AnyhowProperties::new(prop);
        // let mut enumerator = PulsarSplitEnumerator::new(&properties).unwrap();
        // let _ = enumerator.list_splits().await.unwrap();
        // println!("hello");
    }
}