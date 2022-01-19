use crate::base::SourceMessage;

#[derive(Copy, Clone, Debug)]
pub struct PulsarMessage {}

impl SourceMessage for PulsarMessage {
    fn payload(&self) -> anyhow::Result<Option<&[u8]>> {
        todo!()
    }
}
