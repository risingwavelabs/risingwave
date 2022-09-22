use bytes::Bytes;
use google_cloud_pubsub::subscriber::ReceivedMessage;

use crate::source::SourceMessage;

impl From<ReceivedMessage> for SourceMessage {
    fn from(message: ReceivedMessage) -> Self {
        Self {
            payload: {
                let load = message.message.data;
                match load.len() {
                    0 => None,
                    _ => Some(Bytes::from(load)),
                }
            },
            // ! won't work yet -- ack_id is private
            // reference: https://github.com/yoshidan/google-cloud-rust/issues/49
            // ! offset: message.ack_id,
            offset: 0.to_string(),

            // ? what are the ramifications
            split_id: 0.to_string().into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;

    fn test_conv() -> Result<()> {
        let message = ReceivedMessage::default();
        let _source_message: SourceMessage = message.into();
        Ok(())
    }
}
