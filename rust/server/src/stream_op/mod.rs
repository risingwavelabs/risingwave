use crate::array2::DataChunk;
use crate::{buffer::Bitmap, error::Result};

mod aggregation_operator;
pub use aggregation_operator::*;

mod filter_operator;
pub use filter_operator::FilterOperator;

mod local_output;
pub use local_output::LocalOutput;

mod processor;
pub use processor::Processor;

mod actor;
pub use actor::Actor;

mod data_source;
pub use data_source::DataSource;

use crate::array2::column::Column;
use async_trait::async_trait;

pub trait ExprFn = Fn(&DataChunk) -> Result<Bitmap> + Send + Sync + 'static;

/// `Op` represents three operations in StreamChunk.
/// `UpdateDelete` and `UpdateInsert` always appear in pairs.
#[derive(Clone, Copy, Debug)]
pub enum Op {
    Insert,
    Delete,
    UpdateDelete,
    UpdateInsert,
}

/// `StreamChunk` is used to pass data between operators.
#[derive(Default, Debug)]
pub struct StreamChunk {
    // TODO: Optimize using bitmap
    ops: Vec<Op>,
    columns: Vec<Column>,
    visibility: Option<Bitmap>,
    cardinality: usize,
}

pub enum Message {
    Chunk(StreamChunk),
    Barrier,
    Terminate,
    // TODO: Watermark
}

pub trait StreamOperator: Send + Sync + 'static {
    // TODO: watermark and state management
}

#[async_trait]
pub trait UnaryStreamOperator: StreamOperator {
    async fn consume(&mut self, chunk: StreamChunk) -> Result<()>;
}

#[async_trait]
pub trait BinaryStreamOperator: StreamOperator {
    async fn consume_first(&mut self, chunk: StreamChunk) -> Result<()>;
    async fn consume_second(&mut self, chunk: StreamChunk) -> Result<()>;
}

#[async_trait]
pub trait Output: Send + Sync + 'static {
    async fn collect(&mut self, chunk: StreamChunk) -> Result<()>;
}
