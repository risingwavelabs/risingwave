//! Streaming Aggregators

use std::sync::Arc;

use super::aggregation::*;
use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::Array;
use crate::error::Result;
use crate::impl_consume_barrier_default;
use crate::types::DataTypeRef;
use async_trait::async_trait;

/// `StreamingSumAgg` sums data of the same type.
pub type StreamingSumAgg<R> = StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingFloatSumAgg` sums data of the same float type.
pub type StreamingFloatSumAgg<R> =
    StreamingFoldAgg<R, R, FloatPrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingCountAgg` counts data of any type.
pub type StreamingCountAgg<R> = StreamingFoldAgg<R, R, Countable<<R as Array>::OwnedItem>>;

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
            first_data: true,
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

        self.first_data = false;

        self.output.collect(Message::Chunk(chunk)).await?;
        Ok(())
    }
}
