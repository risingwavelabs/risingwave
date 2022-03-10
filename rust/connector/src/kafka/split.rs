use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::base::SourceSplit;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum KafkaOffset {
    Earliest,
    Latest,
    Offset(i64),
    Timestamp(i64),
    None,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct KafkaSplit {
    pub(crate) partition: i32,
    pub(crate) start_offset: KafkaOffset,
    pub(crate) stop_offset: KafkaOffset,
}

impl SourceSplit for KafkaSplit {
    fn id(&self) -> String {
        format!("{}", self.partition)
    }

    fn to_string(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }
}

impl KafkaSplit {
    pub fn new(partition: i32, start_offset: KafkaOffset, stop_offset: KafkaOffset) -> KafkaSplit {
        KafkaSplit {
            partition,
            start_offset,
            stop_offset,
        }
    }
}
