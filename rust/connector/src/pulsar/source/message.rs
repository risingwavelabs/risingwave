use crate::base::{SourceMessage, SourceOffset};

pub struct PulsarMessage {
    pub(crate) message: pulsar::consumer::Message<Vec<u8>>,
}

impl SourceMessage for PulsarMessage {
    fn payload(&self) -> anyhow::Result<Option<&[u8]>> {
        Ok(Some(self.message.payload.data.as_ref()))
    }

    fn offset(&self) -> anyhow::Result<Option<SourceOffset>> {
        Ok(Some(SourceOffset::Number(
            self.message.message_id.id.entry_id as i64,
        )))
    }
}
