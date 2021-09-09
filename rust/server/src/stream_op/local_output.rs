use super::{Output, Result, StreamChunk, UnaryStreamOperator};
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
    async fn collect(&mut self, chunk: StreamChunk) -> Result<()> {
        self.next.consume(chunk).await
    }
}
