//! Global Streaming Aggregators

use super::{aggregation::*, StreamingSumAgg};
use super::{Message, Op, Output, StreamChunk, StreamOperator, UnaryStreamOperator};
use crate::array2::column::Column;
use crate::array2::*;
use crate::error::Result;
use crate::expr::AggKind;
use crate::impl_consume_barrier_default;
use crate::types::DataType;
use crate::types::DataTypeRef;

use async_trait::async_trait;
use std::sync::Arc;

/// `GlobalStreamingSumAgg` sums data of the same type. It sums values from
/// `StreamingSumAgg`.
pub type GlobalStreamingSumAgg<R> =
    StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem>>;

/// `GloabalStreamingFloatSumAgg` sums data of the same float type. It sums
/// values from `StreamingFloatSumAgg`.
pub type GlobalStreamingFloatSumAgg<R> =
    StreamingFoldAgg<R, R, FloatPrimitiveSummable<<R as Array>::OwnedItem>>;

/// `GlobalStreamingCountAgg` counts data of any type. It sums values from
/// `StreamingCountAgg`.
pub type GlobalStreamingCountAgg<R> =
    StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem>>;

/// `GlobalAggregationOperator` takes data from `LocalAggregationOperator`,
/// which generally have two columns: aggregated data and rows. When there
/// is zero rows in aggregator, `None` will be the output.
pub struct GlobalAggregationOperator {
    /// Aggregation state of the current operator
    state: Box<dyn StreamingAggStateImpl>,
    /// Number of rows in total
    row_state: StreamingSumAgg<I64Array>,
    /// The output of the current operator
    output: Box<dyn Output>,
    /// Whether this is the first time of consuming data
    first_data: bool,
    /// Return type of current aggregator
    return_type: DataTypeRef,
}

impl GlobalAggregationOperator {
    pub fn new(
        state: Box<dyn StreamingAggStateImpl>,
        output: Box<dyn Output>,
        return_type: DataTypeRef,
    ) -> Self {
        Self {
            state,
            output,
            first_data: true,
            row_state: StreamingSumAgg::<I64Array>::new(),
            return_type,
        }
    }
}

impl_consume_barrier_default!(GlobalAggregationOperator, StreamOperator);

impl GlobalAggregationOperator {
    fn get_output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        let rows = self.row_state.get_state();
        match rows {
            None | Some(0) => {
                // If there are no rows, output a `None`.
                builder.append_null()?;
            }
            Some(_) => {
                // Otherwise, output the concrete datum
                self.state.get_output(builder)?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl UnaryStreamOperator for GlobalAggregationOperator {
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
            self.get_output(&mut builder)?;
        }

        self.state
            .apply_batch(&ops, visibility.as_ref(), arrays[0].array_ref())?;

        self.row_state
            .apply_batch(&ops, visibility.as_ref(), arrays[1].array_ref())?;

        // output the current state into builder
        self.get_output(&mut builder)?;

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

pub fn create_streaming_global_agg_state(
    _input_type: &dyn DataType,
    _agg_type: &AggKind,
    _return_type: &dyn DataType,
) -> Result<Box<dyn StreamingAggStateImpl>> {
    todo!();
}
