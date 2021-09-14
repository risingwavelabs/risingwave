use super::{Message, Output, Result, UnaryStreamOperator};
use async_trait::async_trait;

pub struct LocalOutput {
    next: Box<dyn UnaryStreamOperator>,
}

impl LocalOutput {
    pub(crate) fn new(next: Box<dyn UnaryStreamOperator>) -> Self {
        Self { next }
    }
}

#[async_trait]
impl Output for LocalOutput {
    async fn collect(&mut self, msg: Message) -> Result<()> {
        match msg {
            Message::Chunk(chunk) => self.next.consume_chunk(chunk).await,
            Message::Barrier(epoch) => self.next.consume_barrier(epoch).await,
            _ => todo!(),
        }
    }
}
