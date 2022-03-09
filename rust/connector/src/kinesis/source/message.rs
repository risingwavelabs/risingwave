use anyhow::anyhow;
use aws_sdk_kinesis::model::Record;
use serde::{Deserialize, Serialize};

use crate::base::{SourceMessage, SourceOffset};

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
