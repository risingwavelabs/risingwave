use super::{Message, Result, StreamOperator};
use async_trait::async_trait;
use futures::channel::mpsc::Receiver;
use futures::StreamExt;

/// `ReceiverOperator` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverOperator` running in the background, so as to push
/// messages down to the operators.
pub struct ReceiverOperator {
    receiver: Receiver<Message>,
}

impl ReceiverOperator {
    pub fn new(receiver: Receiver<Message>) -> Self {
        Self { receiver }
    }
}

#[async_trait]
impl StreamOperator for ReceiverOperator {
    async fn next(&mut self) -> Result<Message> {
        let msg = self.receiver.next().await.unwrap(); // TODO: remove unwrap
        Ok(msg)
    }
}
