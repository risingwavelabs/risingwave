use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

use crate::base::SplitEnumerator;
use crate::pulsar::admin::PulsarAdminClient;
use crate::pulsar::split::{PulsarOffset, PulsarSplit};
use crate::pulsar::topic::ParsedTopic;

pub struct PulsarSplitEnumerator {
    admin_client: PulsarAdminClient,
    topic: ParsedTopic,
    stop_offset: PulsarOffset,
    start_offset: PulsarOffset,
}

#[async_trait]
impl SplitEnumerator for PulsarSplitEnumerator {
    type Split = PulsarSplit;

    async fn list_splits(&mut self) -> anyhow::Result<Vec<Self::Split>> {
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
