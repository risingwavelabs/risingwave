//! This module implements `IdentityOperator`.

use super::*;

/// `IdentityOperator` doesn't do any data transform, except that it passes a message down to
/// the output. This is the minumum working example of an streaming operator.
pub struct IdentityOperator {
    /// The output of the current operator
    input: Box<dyn StreamOperator>,
}

impl IdentityOperator {
    pub fn new(input: Box<dyn StreamOperator>) -> Self {
        Self { input }
    }
}

use crate::impl_consume_barrier_default;

impl_consume_barrier_default!(IdentityOperator, StreamOperator);

impl SimpleStreamOperator for IdentityOperator {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        Ok(Message::Chunk(chunk))
    }
}
