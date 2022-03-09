use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::base::{SourceMessage, SourceOffset};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulsarMessage {
    pub payload: Option<Vec<u8>>,
    pub offset: i64,
}

impl SourceMessage for PulsarMessage {
    fn payload(&self) -> anyhow::Result<Option<&[u8]>> {
        Ok(Some(self.payload.as_ref().unwrap().as_ref()))
    }

    fn offset(&self) -> anyhow::Result<Option<SourceOffset>> {
        Ok(Some(SourceOffset::Number(self.offset)))
    }

    fn serialize(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }
}

impl PulsarMessage {
    pub fn new(message: pulsar::consumer::Message<Vec<u8>>) -> PulsarMessage {
        PulsarMessage {
            payload: Some(message.payload.data),
            offset: message.message_id.id.entry_id as i64,
        }
    }
}
