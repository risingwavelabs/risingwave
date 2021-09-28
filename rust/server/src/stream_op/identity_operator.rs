//! This module implements `IdentityOperator`.

use super::*;

/// `IdentityOperator` doesn't do any data transform, except that it passes a message down to
/// the output. This is the minumum working example of an streaming operator.
pub struct IdentityOperator {
    /// The output of the current operator
    output: Box<dyn Output>,
}

impl IdentityOperator {
    pub fn new(output: Box<dyn Output>) -> Self {
        Self { output }
    }
}

use crate::impl_consume_barrier_default;

impl_consume_barrier_default!(IdentityOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for IdentityOperator {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        self.output.collect(Message::Chunk(chunk)).await
    }
}
