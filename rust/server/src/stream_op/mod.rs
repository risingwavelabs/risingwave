use crate::array2::column::Column;
use crate::array2::DataChunk;
use crate::{buffer::Bitmap, error::Result};

mod actor;
mod aggregation;
mod aggregation_operator;
mod channel_output;
mod data_source;
mod dispatcher;
mod filter_operator;
mod global_hash_aggregation_operator;
mod identity_operator;
mod local_hash_aggregation_operator;
mod mem_table_mv_operator;
mod merge_processor;
mod operator_output;
mod processor;
mod projection_operator;
mod simple_processor;
mod source_processor;
mod table_data_source;

pub use actor::Actor;
pub use aggregation_operator::*;
pub use channel_output::ChannelOutput;
pub use data_source::DataSource;
pub use dispatcher::*;
pub use filter_operator::FilterOperator;
pub use global_hash_aggregation_operator::*;
pub use identity_operator::IdentityOperator;
pub use local_hash_aggregation_operator::*;
pub use mem_table_mv_operator::MemTableMVOperator;
pub use merge_processor::*;
pub use operator_output::OperatorOutput;
pub use processor::*;
pub use projection_operator::ProjectionOperator;
pub use simple_processor::*;
pub use source_processor::*;
pub use table_data_source::*;

use async_trait::async_trait;

#[cfg(test)]
mod integration_tests;

#[cfg(test)]
mod tests;

pub trait ExprFn = Fn(&DataChunk) -> Result<Bitmap> + Send + Sync + 'static;

/// `Op` represents three operations in `StreamChunk`.
/// `UpdateDelete` and `UpdateInsert` always appear in pairs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Insert,
    Delete,
    UpdateDelete,
    UpdateInsert,
}

pub type Ops<'a> = &'a [Op];

/// `StreamChunk` is used to pass data between operators.
#[derive(Default, Debug, Clone)]
pub struct StreamChunk {
    // TODO: Optimize using bitmap
    ops: Vec<Op>,
    columns: Vec<Column>,
    visibility: Option<Bitmap>,
}

impl StreamChunk {
    pub fn new(ops: Vec<Op>, columns: Vec<Column>, visibility: Option<Bitmap>) -> Self {
        StreamChunk {
            ops,
            columns,
            visibility,
        }
    }

    /// return the number of visible tuples
    pub fn cardinality(&self) -> usize {
        if let Some(bitmap) = &self.visibility {
            bitmap.iter().map(|visible| visible as usize).sum()
        } else {
            self.capacity()
        }
    }

    /// return physical length of any chunk column
    pub fn capacity(&self) -> usize {
        self.columns
            .first()
            .map(|col| col.array_ref().len())
            .unwrap_or(0)
    }
}

#[derive(Debug)]
pub enum Message {
    Chunk(StreamChunk),
    Barrier(u64),
    Terminate,
    // TODO: Watermark
}

/// `StreamOperator` is an operator which supports handling of control messages.
#[async_trait]
pub trait StreamOperator: Send + Sync + 'static {
    async fn consume_barrier(&mut self, epoch: u64) -> Result<()>;
    async fn consume_terminate(&mut self) -> Result<()>;
    // TODO: watermark and state management
}

/// `UnaryStreamOperator` accepts a single chunk as input.
#[async_trait]
pub trait UnaryStreamOperator: StreamOperator {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()>;
}

/// Most operators don't care about the control messages, and therefore
/// this macro provides a default implementation for them. The operator
/// must have a field named `output`, so as to pass along messages.
#[macro_export]
macro_rules! impl_consume_barrier_default {
    ($type:ident, $trait: ident) => {
        #[async_trait]
        impl $trait for $type {
            async fn consume_barrier(&mut self, epoch: u64) -> Result<()> {
                self.output.collect(Message::Barrier(epoch)).await
            }

            async fn consume_terminate(&mut self) -> Result<()> {
                self.output.collect(Message::Terminate).await
            }
        }
    };
}

/// `BinaryStreamOperator` accepts two chunks as input.
#[async_trait]
pub trait BinaryStreamOperator: StreamOperator {
    async fn consume_chunk_first(&mut self, chunk: StreamChunk) -> Result<()>;
    async fn consume_chunk_second(&mut self, chunk: StreamChunk) -> Result<()>;
}

/// Output message could be written into a `Output`.
#[async_trait]
pub trait Output: Send + Sync + 'static {
    async fn collect(&mut self, msg: Message) -> Result<()>;
}
