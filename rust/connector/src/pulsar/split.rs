use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::base::SourceSplit;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum PulsarOffset {
    MessageID(u64),
    Timestamp(u64),
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarSplit {
    pub(crate) sub_topic: String,
    pub(crate) start_offset: PulsarOffset,
    pub(crate) stop_offset: PulsarOffset,
}

impl PulsarSplit {
    pub fn new(sub_topic: String, start_offset: PulsarOffset, stop_offset: PulsarOffset) -> Self {
        Self {
            sub_topic,
            start_offset,
            stop_offset,
        }
    }
}

impl SourceSplit for PulsarSplit {
    fn id(&self) -> String {
        self.sub_topic.clone()
    }

    fn to_string(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }
}
