use super::{Message, Output, Result};
use async_trait::async_trait;
use futures::channel::mpsc::Sender;
use futures::SinkExt;

/// `ChannelOutput` takes a mpsc sender as construction parameter. Upon receiving
/// a message, the message will be put into the mpsc channel.
pub struct ChannelOutput {
    sender: Sender<Message>,
}

impl ChannelOutput {
    pub(crate) fn new(sender: Sender<Message>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl Output for ChannelOutput {
    async fn collect(&mut self, msg: Message) -> Result<()> {
        self.sender.send(msg).await.unwrap();
        Ok(())
    }
}
