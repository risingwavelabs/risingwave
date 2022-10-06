use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::source::{SplitId, SplitMetaData};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PubsubSplit {
    pub(crate) index: u32,
}

impl PubsubSplit {
    pub fn copy_with_offset(&self, _start_offset: String) -> Self {
        todo!();
    }
}

impl SplitMetaData for PubsubSplit {
    fn id(&self) -> SplitId {
        format!("{}", self.index).into()
    }

    fn encode_to_bytes(&self) -> bytes::Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}
