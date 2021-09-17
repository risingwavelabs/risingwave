use super::{Message, Processor, Result, UnaryStreamOperator};
use async_trait::async_trait;
use futures::channel::mpsc::Receiver;
use futures::prelude::*;

/// `UnarySimpleProcessor` is used along with a channel, or `ChannelOutput`. After creating a
/// mpsc channel, there should be a `UnarySimpleProcessor` running in the background, so as
/// to push messages down to the operators.
pub struct UnarySimpleProcessor {
    receiver: Receiver<Message>,
    operator_head: Box<dyn UnaryStreamOperator>,
}

impl UnarySimpleProcessor {
    pub fn new(receiver: Receiver<Message>, operator_head: Box<dyn UnaryStreamOperator>) -> Self {
        Self {
            receiver,
            operator_head,
        }
    }
}

#[async_trait]
impl Processor for UnarySimpleProcessor {
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
