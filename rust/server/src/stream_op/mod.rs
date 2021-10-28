use crate::array::column::Column;
use crate::array::DataChunk;
use crate::{buffer::Bitmap, error::Result};

mod actor;
mod aggregation;
mod dispatch;
mod filter;
mod hash_agg;
mod kafka_source;
mod merge;
mod mview_sink;
mod project;
mod simple_agg;
mod table_source;

pub use actor::Actor;
pub use aggregation::*;
pub use dispatch::*;
pub use filter::*;
pub use hash_agg::*;
pub use kafka_source::*;
pub use merge::*;
pub use mview_sink::*;
pub use project::*;
pub use simple_agg::*;
pub use table_source::*;

use async_trait::async_trait;
use std::sync::Arc;

#[cfg(test)]
mod integration_tests;

#[cfg(test)]
mod test_utils;

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

/// `StreamChunk` is used to pass data between executors.
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

    /// compact the `StreamChunck` with its visibility map
    pub fn compact(self) -> Result<Self> {
        match &self.visibility {
            None => Ok(self),
            Some(visibility) => {
                let cardinality = visibility
                    .iter()
                    .fold(0, |vis_cnt, vis| vis_cnt + vis as usize);
                let columns = self
                    .columns
                    .into_iter()
                    .map(|col| {
                        let array = col.array();
                        let data_type = col.data_type();
                        array
                            .compact(visibility, cardinality)
                            .map(|array| Column::new(Arc::new(array), data_type))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let mut ops = Vec::with_capacity(cardinality);
                for (op, visible) in self.ops.into_iter().zip(visibility.iter()) {
                    if visible {
                        ops.push(op);
                    }
                }
                Ok(StreamChunk {
                    ops,
                    columns,
                    visibility: None,
                })
            }
        }
    }
}

#[derive(Debug)]
pub enum Message {
    Chunk(StreamChunk),
    Barrier(u64),
    // Note(eric): consider remove this. A stream is always terminated by an error or dropped by user
    Terminate,
    // TODO: Watermark
}

/// `Executor` supports handling of control messages.
#[async_trait]
pub trait Executor: Send + Sync + 'static {
    async fn next(&mut self) -> Result<Message>;
}

/// `SimpleExecutor` accepts a single chunk as input.
pub trait SimpleExecutor: Executor {
    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message>;
}

/// Most executors don't care about the control messages, and therefore
/// this macro provides a default implementation for them. The executor
/// must have a field named `input`, so as to pass along messages, and
/// implement the `SimpleExecutor` trait to provide a `consume_chunk`
/// function
#[macro_export]
macro_rules! impl_consume_barrier_default {
    ($type:ident, $trait: ident) => {
        #[async_trait]
        impl $trait for $type {
            async fn next(&mut self) -> Result<Message> {
                match self.input.next().await {
                    Ok(message) => match message {
                        Message::Chunk(chunk) => self.consume_chunk(chunk),
                        Message::Barrier(epoch) => Ok(Message::Barrier(epoch)),
                        Message::Terminate => Ok(Message::Terminate),
                    },
                    Err(e) => Err(e),
                }
            }
        }
    };
}

/// `StreamConsumer` is the last step in a fragment
#[async_trait]
pub trait StreamConsumer: Send + Sync + 'static {
    /// Run next stream chunk. returns whether the stream is terminated
    async fn next(&mut self) -> Result<bool>;
}
