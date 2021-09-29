//! Streaming Aggregators

use std::sync::Arc;

use super::aggregation::*;
use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::*;
use crate::error::{Result, RwError};
use crate::impl_consume_barrier_default;
use crate::types::DataTypeRef;
use itertools::Itertools;

use async_trait::async_trait;

/// `StreamingSumAgg` sums data of the same type.
pub type StreamingSumAgg<R> = StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingFloatSumAgg` sums data of the same float type.
pub type StreamingFloatSumAgg<R> =
    StreamingFoldAgg<R, R, FloatPrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingCountAgg` counts data of any type.
pub type StreamingCountAgg<S> = StreamingFoldAgg<I64Array, S, Countable<<S as Array>::OwnedItem>>;

pub use super::aggregation::StreamingRowCountAgg;

/// `AggregationOperator` is the aggregation operator for streaming system.
/// To create an aggregation operator, states and expressions should be passed along the
/// constructor.
///
/// `AggregationOperator` maintain multiple states together. If there are `n`
/// states and `n` expressions, there will be `n` columns as output.
///
/// As the engine processes data in chunks, it is possible that multiple update
/// messages could consolidate to a single row update. For example, our source
/// emits 1000 inserts in one chunk, and we aggregates count function on that.
/// Current `AggregationOperator` will only emit one row for a whole chunk.
/// Therefore, we "automatically" implemented a window function inside
/// `AggregationOperator`.
pub struct AggregationOperator {
    /// Aggregation states of the current operator
    states: Vec<Box<dyn StreamingAggStateImpl>>,

    /// The output of the current operator
    output: Box<dyn Output>,

    /// Whether this is the first time of consuming data.
    ///
    /// Note that this is also part of the operator state, and should be
    /// persisted in the future.
    first_data: bool,

    /// Return type of current aggregator.
    ///
    /// TODO: After we add expression support, these should be part of the expression
    /// instead of aggregator.
    return_types: Vec<DataTypeRef>,

    /// The column to process.
    ///
    /// TODO: Will be removed after expression is supported.
    col_idx: Vec<usize>,
}

impl AggregationOperator {
    pub fn new(
        states: Vec<Box<dyn StreamingAggStateImpl>>,
        output: Box<dyn Output>,
        return_types: Vec<DataTypeRef>,
        col_idx: Vec<usize>,
    ) -> Self {
        Self {
            states,
            output,
            first_data: true,
            return_types,
            col_idx,
        }
    }

    /// Record current states into a group of builders
    fn record_states(&mut self, builders: &mut [ArrayBuilderImpl]) -> Result<()> {
        for (state, builder) in self.states.iter().zip(builders.iter_mut()) {
            state.get_output(builder)?;
        }
        Ok(())
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

        let mut builders = self
            .states
            .iter()
            .map(|state| state.new_builder())
            .collect_vec();

        if !self.first_data {
            // record the last state into builder
            self.record_states(&mut builders)?;
        }

        // apply chunk to states
        for (state, col_idx) in self.states.iter_mut().zip(self.col_idx.iter()) {
            state.apply_batch(&ops, visibility.as_ref(), arrays[*col_idx].array_ref())?;
        }

        // output the current state into builder
        self.record_states(&mut builders)?;

        let chunk;

        let columns = builders
            .into_iter()
            .zip(self.return_types.iter())
            .map(|(builder, return_type)| {
                Ok::<_, RwError>(Column::new(
                    Arc::new(builder.finish()?),
                    return_type.clone(),
                ))
            })
            .try_collect()?;

        // For the first update, cardinality is 1. For the rest, cardinalty is 2,
        // which includes a deletion and a update.
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
