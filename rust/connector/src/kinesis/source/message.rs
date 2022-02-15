// use aws_sdk_kinesis::{Client as KinesisClient, SdkError};
// use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::model::Record;

// use aws_smithy_types::Blob;
// use aws_sdk_kinesis::error::GetRecordsError;
use crate::base::SourceMessage;

#[derive(Clone, Debug)]
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

    // TODO(tabVersion) remove after debug
    pub fn default() -> Self {
        KinesisMessage {
            shard_id: "shard_id".to_string(),
            sequence_number: "sequence number".to_string(),
            partition_key: "partition key".to_string(),
            payload: None,
        }
    }
}
