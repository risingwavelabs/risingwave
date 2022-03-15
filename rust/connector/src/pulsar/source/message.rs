use anyhow::anyhow;
use pulsar::consumer::Message;
use serde::{Deserialize, Serialize};

use crate::base::{InnerMessage, SourceMessage, SourceOffset};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PulsarMessage {
    pub payload: Option<Vec<u8>>,
    pub offset: i64,
    pub split_id: String,
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

impl From<Message<Vec<u8>>> for InnerMessage {
    fn from(msg: Message<Vec<u8>>) -> Self {
        InnerMessage {
            payload: Some(bytes::Bytes::from(msg.payload.data)),
            offset: msg.message_id.id.entry_id.to_string(),
            split_id: msg.topic,
        }
    }
}
