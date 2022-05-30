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

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::base::SplitEnumerator;
use crate::pulsar::admin::PulsarAdminClient;
use crate::pulsar::split::PulsarSplit;
use crate::pulsar::topic::{parse_topic, Topic};
use crate::pulsar::PulsarProperties;

pub struct PulsarSplitEnumerator {
    admin_client: PulsarAdminClient,
    topic: Topic,
    start_offset: PulsarEnumeratorOffset,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum PulsarEnumeratorOffset {
    Earliest,
    Latest,
    MessageId(String),
    Timestamp(i64),
}

impl PulsarSplitEnumerator {}

#[async_trait]
impl SplitEnumerator for PulsarSplitEnumerator {
    type Properties = PulsarProperties;
    type Split = PulsarSplit;

    async fn new(properties: PulsarProperties) -> Result<PulsarSplitEnumerator> {
        let topic = properties.topic;
        let admin_url = properties.admin_url;
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
                return Err(anyhow!(
                    "properties `startup_mode` only support earliest and latest or leave it empty"
                ));
            }
        };

        if let Some(s) = properties.time_offset {
            let time_offset = s.parse::<i64>().map_err(|e| anyhow!(e))?;
            scan_start_offset = PulsarEnumeratorOffset::Timestamp(time_offset)
        }

        Ok(PulsarSplitEnumerator {
            admin_client: PulsarAdminClient::new(admin_url),
            topic: parsed_topic,
            start_offset: scan_start_offset,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<PulsarSplit>> {
        let offset = self.start_offset.clone();
        // MessageId is only used when recovering from a State
        assert!(!matches!(offset, PulsarEnumeratorOffset::MessageId(_)));
        match self.topic.partition_index {
            // partitioned topic
            None => self
                .admin_client
                .get_topic_metadata(&self.topic)
                .await
                .and_then(|meta| {
                    if meta.partitions < 0 {
                        Err(anyhow!(
                            "metadata illegal for topic {}",
                            self.topic.to_string()
                        ))
                    } else {
                        Ok(meta)
                    }
                })
                .map(|meta| {
                    (0..meta.partitions)
                        .into_iter()
                        .map(|p| PulsarSplit {
                            topic: self.topic.sub_topic(p as i32).unwrap(),
                            start_offset: offset.clone(),
                        })
                        .collect_vec()
                }),
            // non partitioned topic
            Some(_) => {
                // we need to check topic exists
                self.admin_client
                    .get_topic_metadata(&self.topic)
                    .await
                    .map(|_| {
                        vec![PulsarSplit {
                            topic: self.topic.clone(),
                            start_offset: offset.clone(),
                        }]
                    })
            }
        }
    }
}

#[cfg(test)]
mod test {
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::pulsar::{PulsarEnumeratorOffset, PulsarProperties, PulsarSplitEnumerator};
    use crate::SplitEnumerator;

    async fn mock_server(web_path: &str, body: &str) -> MockServer {
        let mock_server = MockServer::start().await;
        use wiremock::matchers::{method, path};

        let response = ResponseTemplate::new(200)
            .set_body_string(body)
            .append_header("content-type", "application/json");

        Mock::given(method("GET"))
            .and(path(web_path))
            .respond_with(response)
            .mount(&mock_server)
            .await;

        mock_server
    }

    #[tokio::test]
    async fn test_list_splits() {
        let server = mock_server(
            "/admin/v2/persistent/public/default/t/partitions",
            "{\"partitions\":3}",
        )
        .await;

        let prop = PulsarProperties {
            topic: "t".to_string(),
            admin_url: server.uri(),
            service_url: "".to_string(),
            scan_startup_mode: Some("earliest".to_string()),
            time_offset: None,
        };
        let mut enumerator = PulsarSplitEnumerator::new(prop).await.unwrap();

        let splits = enumerator.list_splits().await.unwrap();
        assert_eq!(splits.len(), 3);

        (0..3).for_each(|i| {
            assert_eq!(splits[i].start_offset, PulsarEnumeratorOffset::Earliest);
            assert_eq!(splits[i].topic.partition_index, Some(i as i32));
        });
    }
}
