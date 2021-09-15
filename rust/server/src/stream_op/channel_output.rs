use super::{Message, OperatorHead, Output, Result, UnaryStreamOperator};
use async_trait::async_trait;
use futures::channel::mpsc::{Receiver, Sender};
use futures::{SinkExt, StreamExt};

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

/// `ChannelConsumer` is used along with `ChannelOutput`. After creating a
/// mpsc channel, there should be a `ChannelConsumer` running in the background,
/// so as to push messages down to the operators.
pub struct ChannelConsumer {
    receiver: Receiver<Message>,
    operator_head: Box<dyn UnaryStreamOperator>,
}

impl ChannelConsumer {
    pub fn new(receiver: Receiver<Message>, operator_head: Box<dyn UnaryStreamOperator>) -> Self {
        Self {
            receiver,
            operator_head,
        }
    }
}

#[async_trait]
impl OperatorHead for ChannelConsumer {
    async fn run(mut self) -> Result<()> {
        while let Some(msg) = self.receiver.next().await {
            match msg {
                Message::Chunk(chunk) => {
                    self.operator_head.consume_chunk(chunk).await?;
                }
                Message::Barrier(epoch) => {
                    self.operator_head.consume_barrier(epoch).await?;
                }
                Message::Terminate => {
                    self.operator_head.consume_terminate().await?;
                    break;
                }
            }
        }
        Ok(())
    }
}
