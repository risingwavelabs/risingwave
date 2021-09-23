use super::{Message, Output, Result, UnaryStreamOperator};
use async_trait::async_trait;

pub struct OperatorOutput {
    next: Box<dyn UnaryStreamOperator>,
}

impl OperatorOutput {
    pub(crate) fn new(next: Box<dyn UnaryStreamOperator>) -> Self {
        Self { next }
    }
}

#[async_trait]
impl Output for OperatorOutput {
    async fn collect(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Chunk(chunk) => self.next.consume_chunk(chunk).await,
            Message::Barrier(epoch) => self.next.consume_barrier(epoch).await,
            Message::Terminate => self.next.consume_terminate().await,
        }
    }
}
