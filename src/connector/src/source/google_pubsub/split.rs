use anyhow::anyhow;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::source::{SplitMetaData, SplitId};

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct PubsubSplit {
    pub(crate) topic: String,
    pub(crate) subscription: String,
}

impl PubsubSplit {}

impl SplitMetaData for PubsubSplit {
    fn id(&self) -> SplitId {
        // for now, until we figure out how we take care of IDs here
        format!("{}", 0).into()
    }

    fn encode_to_bytes(&self) -> bytes::Bytes {
        Bytes::from(serde_json::to_string(self).unwrap())
    }

    fn restore_from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}
