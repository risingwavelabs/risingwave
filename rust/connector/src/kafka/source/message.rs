use rdkafka::message::BorrowedMessage;
use rdkafka::Message;

use crate::base::SourceMessage;

#[derive(Clone)]
pub struct KafkaMessage {
    partition: i32,
    offset: i64,
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
}

impl SourceMessage for KafkaMessage {
    fn payload(&self) -> anyhow::Result<Option<&[u8]>> {
        Ok(self.payload.as_ref().map(|payload| payload.as_ref()))
    }
}

impl<'a> From<BorrowedMessage<'a>> for KafkaMessage {
    fn from(message: BorrowedMessage<'a>) -> Self {
        KafkaMessage {
            partition: message.partition(),
            offset: message.offset(),
            key: message.key().map(|key| key.to_vec()),
            payload: message.payload().map(|m| m.to_vec()),
        }
    }
}
