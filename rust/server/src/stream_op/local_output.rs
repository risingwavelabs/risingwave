use super::{Output, Result, StreamChunk, UnaryStreamOperator};

pub struct LocalOutput {
    next: Box<dyn UnaryStreamOperator>,
}

impl LocalOutput {
    pub(crate) fn new(next: Box<dyn UnaryStreamOperator>) -> Self {
        Self { next }
    }
}

impl Output for LocalOutput {
    fn collect(&mut self, chunk: StreamChunk) -> Result<()> {
        self.next.consume(chunk)
    }
}
