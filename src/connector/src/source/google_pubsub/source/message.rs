use bytes::Bytes;
use google_cloud_googleapis::pubsub::v1::ReceivedMessage;
use anyhow::Result;

use crate::source::SourceMessage;

impl From<ReceivedMessage> for SourceMessage {
    fn from(message: ReceivedMessage) -> Self {
        Self {
            payload: message.message.map(|m| Bytes::from(m.data)),
            offset: message.ack_id,
            // ? what are the ramifications
            split_id: 0.to_string().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_conv() -> Result<()> {
        let message = ReceivedMessage::default();
        let _source_message: SourceMessage = message.into();
        Ok(())
    }
}
