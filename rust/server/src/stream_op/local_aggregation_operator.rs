//! Streaming Aggregators

use std::sync::Arc;

use super::aggregation::*;
use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::*;
use crate::error::Result;
use crate::impl_consume_barrier_default;
use crate::types::DataTypeRef;
use crate::types::Int64Type;

use async_trait::async_trait;

/// `StreamingSumAgg` sums data of the same type.
pub type StreamingSumAgg<R> = StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingFloatSumAgg` sums data of the same float type.
pub type StreamingFloatSumAgg<R> =
    StreamingFoldAgg<R, R, FloatPrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingCountAgg` counts data of any type.
pub type StreamingCountAgg<R> = StreamingFoldAgg<R, R, Countable<<R as Array>::OwnedItem>>;

/// `LocalAggregationOperator` is the aggregation operator for streaming system.
/// To create an aggregation operator, a state should be passed along the
/// constructor.
///
/// `LocalAggregationOperator` counts rows and outputs two columns, one is concrete
/// data stored by state, and one is the count. These data will be further sent
/// to `GlobalAggregationOperator` and become the real aggregated data.
///
/// As the engine processes data in chunks, it is possible that multiple update
/// messages could consolidate to a single row update. For example, our source
/// emits 1000 inserts in one chunk, and we aggregates count function on that.
/// Current `LocalAggregationOperator` will only emit one row for a whole chunk.
/// Therefore, we "automatically" implemented a window function inside
/// `LocalAggregationOperator`.
pub struct LocalAggregationOperator {
    /// Aggregation state of the current operator
    state: Box<dyn StreamingAggStateImpl>,
    /// The output of the current operator
    output: Box<dyn Output>,
    /// Whether this is the first time of consuming data
    first_data: bool,
    /// Number of rows processed by this operator
    row_state: StreamingRowCountAgg,
    /// Return type of current aggregator
    return_type: DataTypeRef,
    /// Column to process
    col_idx: usize,
}

impl LocalAggregationOperator {
    pub fn new(
        state: Box<dyn StreamingAggStateImpl>,
        output: Box<dyn Output>,
        return_type: DataTypeRef,
        col_idx: usize,
    ) -> Self {
        Self {
            state,
            output,
            first_data: true,
            row_state: StreamingRowCountAgg::new(),
            return_type,
            col_idx,
        }
    }
}

impl_consume_barrier_default!(LocalAggregationOperator, StreamOperator);

#[async_trait]
impl UnaryStreamOperator for LocalAggregationOperator {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
            cardinality: _,
        } = chunk;

        let mut builder = self.state.new_builder();
        let mut row_count_builder = self.row_state.new_builder();

        if !self.first_data {
            // record the last state into builder
            self.state.get_output(&mut builder)?;
            self.row_state.get_output(&mut row_count_builder)?;
        }

        self.state
            .apply_batch(&ops, visibility.as_ref(), arrays[self.col_idx].array_ref())?;

        self.row_state
            .apply_batch(&ops, visibility.as_ref(), arrays[self.col_idx].array_ref())?;

        // output the current state into builder
        self.state.get_output(&mut builder)?;
        self.row_state.get_output(&mut row_count_builder)?;

        let chunk;

        let array = Arc::new(builder.finish()?);
        let column = Column::new(array, self.return_type.clone());

        let array_rowcnt = Arc::new(row_count_builder.finish()?);
        let column_rowcnt = Column::new(array_rowcnt, Arc::new(Int64Type::new(true)));

        let columns = vec![column, column_rowcnt];

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
