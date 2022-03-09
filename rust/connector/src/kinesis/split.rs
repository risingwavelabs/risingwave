use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::base::SourceSplit;

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum KinesisOffset {
    Earliest,
    Latest,
    SequenceNumber(String),
    Timestamp(i64),
    None,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KinesisSplit {
    pub(crate) shard_id: String,
    pub(crate) start_position: KinesisOffset,
    pub(crate) end_position: KinesisOffset,
}

impl SourceSplit for KinesisSplit {
    fn id(&self) -> String {
        self.shard_id.to_string()
    }

    fn to_string(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }
}

impl KinesisSplit {
    pub fn new(
        shard_id: String,
        start_position: KinesisOffset,
        end_position: KinesisOffset,
    ) -> KinesisSplit {
        KinesisSplit {
            shard_id,
            start_position,
            end_position,
        }
    }
}
