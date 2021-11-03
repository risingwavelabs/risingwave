use crate::stream_op::*;
use std::collections::VecDeque;

pub struct MockSource {
    schema: Schema,
    chunks: VecDeque<StreamChunk>,
}

impl MockSource {
    pub fn new(schema: Schema, chunks: Vec<StreamChunk>) -> Self {
        Self {
            schema,
            chunks: chunks.into_iter().collect(),
        }
    }
}

#[async_trait]
impl Executor for MockSource {
    async fn next(&mut self) -> Result<Message> {
        match self.chunks.pop_front() {
            Some(chunk) => Ok(Message::Chunk(chunk)),
            None => Ok(Message::Terminate),
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
