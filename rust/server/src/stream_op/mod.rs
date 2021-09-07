use crate::{array2::ArrayImpl, buffer::Bitmap, error::RwError};

type Result<T> = std::result::Result<T, RwError>;

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
    arrays: Vec<ArrayImpl>,
    visibility: Option<Bitmap>,
    cardinality: usize,
}

pub enum Message {
    Chunk(StreamChunk),
    Barrier,
    Terminate,
    // TODO: Watermark
}

pub trait StreamOperator {
    // TODO: watermark and state management
}

pub trait UnaryStreamOperator: StreamOperator {
    fn consume(&mut self, chunk: StreamChunk) -> Result<()>;
}

pub trait BinaryStreamOperator: StreamOperator {
    fn consume_first(&mut self, chunk: StreamChunk) -> Result<()>;
    fn consume_second(&mut self, chunk: StreamChunk) -> Result<()>;
}

pub trait Output {
    fn collect(&mut self, chunk: StreamChunk) -> Result<()>;
}
