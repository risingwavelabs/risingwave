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
//
use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

use crate::base::SplitEnumerator;
use crate::pulsar::admin::PulsarAdminClient;
use crate::pulsar::split::{PulsarOffset, PulsarSplit};
use crate::pulsar::topic::{parse_topic, ParsedTopic};
use crate::pulsar::{PULSAR_CONFIG_ADMIN_URL_KEY, PULSAR_CONFIG_TOPIC_KEY};

pub struct PulsarSplitEnumerator {
    admin_client: PulsarAdminClient,
    topic: ParsedTopic,
    stop_offset: PulsarOffset,
    start_offset: PulsarOffset,
}

macro_rules! extract {
    ($node_type:expr, $source:expr) => {
        $node_type
            .get($source)
            .ok_or_else(|| anyhow::anyhow!("property {} not found", $source))?
    };
}

impl PulsarSplitEnumerator {
    pub(crate) fn new(properties: &HashMap<String, String>) -> Result<PulsarSplitEnumerator> {
        let topic = extract!(properties, PULSAR_CONFIG_TOPIC_KEY);
        let admin_url = extract!(properties, PULSAR_CONFIG_ADMIN_URL_KEY);
        let parsed_topic = parse_topic(topic)?;

        // todo handle offset init
        Ok(PulsarSplitEnumerator {
            admin_client: PulsarAdminClient::new(admin_url.clone()),
            topic: parsed_topic,
            stop_offset: PulsarOffset::None,
            start_offset: PulsarOffset::None,
        })
    }
}

#[async_trait]
impl SplitEnumerator for PulsarSplitEnumerator {
    type Split = PulsarSplit;
    async fn list_splits(&mut self) -> anyhow::Result<Vec<PulsarSplit>> {
        let meta = self.admin_client.get_topic_metadata(&self.topic).await?;

        let ret = (0..meta.partitions)
            .into_iter()
            .map(|p| {
                let sub_topic = self.topic.sub_topic(p as i32);
                PulsarSplit {
                    sub_topic: sub_topic.to_string(),
                    start_offset: self.start_offset,
                    stop_offset: self.stop_offset,
                }
            })
            .collect();

        Ok(ret)
    }
}

impl PulsarSplitEnumerator {
    fn fetch_start_offset(&self, _partitions: &[i32]) -> Result<HashMap<i32, PulsarOffset>> {
        todo!()
    }
}
