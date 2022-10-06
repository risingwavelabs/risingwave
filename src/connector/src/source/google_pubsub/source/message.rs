use bytes::Bytes;
use chrono::{TimeZone, Utc};
use google_cloud_pubsub::subscriber::ReceivedMessage;

use crate::source::SourceMessage;

pub(crate) struct TaggedReceivedMessage(pub(crate) String, pub(crate) ReceivedMessage);

impl From<TaggedReceivedMessage> for SourceMessage {
    fn from(tagged_message: TaggedReceivedMessage) -> Self {
        let TaggedReceivedMessage(split_id, message) = tagged_message;

        let timestamp = message
            .message
            .publish_time
            .map(|t| Utc.timestamp(t.seconds, u32::try_from(t.nanos).unwrap_or_default()))
            .unwrap_or_default();

        Self {
            payload: {
                let payload = message.message.data;
                match payload.len() {
                    0 => None,
                    _ => Some(Bytes::from(payload)),
                }
            },

            offset: timestamp.to_string(),

            split_id: split_id.into(),
        }
    }
}
