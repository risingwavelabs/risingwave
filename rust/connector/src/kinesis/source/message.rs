use anyhow::anyhow;
use aws_sdk_kinesis::model::Record;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::base::{InnerMessage, SourceMessage, SourceOffset};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KinesisMessage {
    pub shard_id: String,
    pub sequence_number: String,
    pub partition_key: String,
    pub payload: Option<Vec<u8>>,
}

impl SourceMessage for KinesisMessage {
    fn payload(&self) -> anyhow::Result<Option<&[u8]>> {
        Ok(self.payload.as_ref().map(|payload| payload.as_ref()))
    }

    fn offset(&self) -> anyhow::Result<Option<SourceOffset>> {
        Ok(Some(SourceOffset::String(self.sequence_number.clone())))
    }

    fn serialize(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(|e| anyhow!(e))
    }
}

impl From<KinesisMessage> for InnerMessage {
    fn from(msg: KinesisMessage) -> Self {
        InnerMessage {
            payload: msg
                .payload
                .as_ref()
                .map(|payload| Bytes::copy_from_slice(payload)),
            offset: msg.sequence_number.clone(),
            split_id: msg.shard_id,
        }
    }
}

impl KinesisMessage {
    pub fn new(shard_id: String, message: Record) -> Self {
        KinesisMessage {
            shard_id,
            sequence_number: message.sequence_number.unwrap(),
            partition_key: message.partition_key.unwrap(),
            payload: Some(message.data.unwrap().into_inner()),
        }
    }
}
