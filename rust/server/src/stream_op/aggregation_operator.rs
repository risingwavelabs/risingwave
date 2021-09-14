//! Streaming Aggregators

mod foldable;
use std::sync::Arc;

pub use foldable::*;

use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl};
use crate::buffer::Bitmap;
use crate::error::Result;
use crate::impl_consume_barrier_default;
use crate::types::DataTypeRef;
use async_trait::async_trait;

pub type Ops<'a> = &'a [Op];

/// `StreamingAggState` records a state of streaming expression. For example,
/// there will be `StreamingAggCompare` and `StreamingAggSum`.
pub trait StreamingAggState<A: Array> {
    fn apply_batch_concrete(&mut self, ops: Ops<'_>, skip: Option<&Bitmap>, data: &A)
        -> Result<()>;
}

/// `StreamingAggFunction` allows us to get output from a streaming state.
pub trait StreamingAggFunction<B: ArrayBuilder> {
    fn get_output_concrete(&self, builder: &mut B) -> Result<()>;
}

/// `StreamingAggStateImpl` erases the associated type information of
/// `StreamingAggState` and `StreamingAggFunction`. You should manually
/// implement this trait for necessary types.
pub trait StreamingAggStateImpl: Send + Sync + 'static {
    fn apply_batch(&mut self, ops: Ops<'_>, skip: Option<&Bitmap>, data: &ArrayImpl) -> Result<()>;

    fn get_output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;

    fn new_builder(&self) -> ArrayBuilderImpl;
}

/// `AggregationOperator` is the aggregation operator for streaming system.
/// To create an aggregation operator, a state should be passed along the
/// constructor.
///
/// As the engine processes data in chunks, it is possible that multiple update
/// messages could consolidate to a single row update. For example, our source
/// emits 1000 inserts in one chunk, and we aggregates count function on that.
/// Current `AggregationOperator` will only emit one row for a whole chunk.
/// Therefore, we "automatically" implemented a window function inside
/// `AggregationOperator`.
pub struct AggregationOperator {
    /// Aggregation state of the current operator
    state: Box<dyn StreamingAggStateImpl>,
    /// The output of the current operator
    output: Box<dyn Output>,
    /// Whether this is the first time of consuming data
    first_data: bool,
    /// Return type of current aggregator
    return_type: DataTypeRef,
}

impl AggregationOperator {
    pub fn new(
        state: Box<dyn StreamingAggStateImpl>,
        output: Box<dyn Output>,
        return_type: DataTypeRef,
    ) -> Self {
        Self {
            state,
            output,
            first_data: false,
            return_type,
        }
    }
}

impl_consume_barrier_default!(AggregationOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for AggregationOperator {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
            cardinality: _,
        } = chunk;

        let mut builder = self.state.new_builder();

        if !self.first_data {
            // record the last state into builder
            self.state.get_output(&mut builder)?;
        }
        self.state
            .apply_batch(&ops, visibility.as_ref(), arrays[0].array_ref())?;
        // output the current state into builder
        self.state.get_output(&mut builder)?;

        let chunk;
        let array = Arc::new(builder.finish()?);
        let column = Column::new(array, self.return_type.clone());
        let columns = vec![column];

        // There should be only one column in output. Meanwhile, for the first update,
        // cardinality is 1. For the rest, cardinalty is 2, which includes a deletion and
        // a update.
        if self.first_data {
            chunk = StreamChunk {
                ops: vec![Op::Insert],
                visibility: None,
                cardinality: 1,
                columns,
            };
        } else {
            chunk = StreamChunk {
                ops: vec![Op::UpdateDelete, Op::UpdateInsert],
                visibility: None,
                cardinality: 2,
                columns,
            };
        }

        self.output.collect(Message::Chunk(chunk)).await?;
        Ok(())
    }
}
