//! Streaming Aggregators

use std::sync::Arc;

use super::aggregation::*;
use super::{Executor, Message, SimpleExecutor};
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::*;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};

use async_trait::async_trait;

/// `StreamingSumAgg` sums data of the same type.
pub type StreamingSumAgg<R, I> =
    StreamingFoldAgg<R, I, PrimitiveSummable<<R as Array>::OwnedItem, <I as Array>::OwnedItem>>;

/// `StreamingFloatSumAgg` sums data of the same float type.
pub type StreamingFloatSumAgg<R> =
    StreamingFoldAgg<R, R, FloatPrimitiveSummable<<R as Array>::OwnedItem>>;

/// `StreamingCountAgg` counts data of any type.
pub type StreamingCountAgg<S> = StreamingFoldAgg<I64Array, S, Countable<<S as Array>::OwnedItem>>;

/// `StreamingMinAgg` get minimum data of the same type.
pub type StreamingMinAgg<S> = StreamingFoldAgg<S, S, Minimizable<<S as Array>::OwnedItem>>;

/// `StreamingFloatMinAgg` get minimum data of the same float type.
pub type StreamingFloatMinAgg<S> =
    StreamingFoldAgg<S, S, FloatMinimizable<<S as Array>::OwnedItem>>;

/// `StreamingMaxAgg` get maximum data of the same type.
pub type StreamingMaxAgg<S> = StreamingFoldAgg<S, S, Maximizable<<S as Array>::OwnedItem>>;

/// `StreamingFloatMaxAgg` get maximum data of the same float type.
pub type StreamingFloatMaxAgg<S> =
    StreamingFoldAgg<S, S, FloatMaximizable<<S as Array>::OwnedItem>>;

pub use super::aggregation::StreamingRowCountAgg;

/// `SimpleAggExecutor` is the aggregation operator for streaming system.
/// To create an aggregation operator, states and expressions should be passed along the
/// constructor.
///
/// `SimpleAggExecutor` maintain multiple states together. If there are `n`
/// states and `n` expressions, there will be `n` columns as output.
///
/// As the engine processes data in chunks, it is possible that multiple update
/// messages could consolidate to a single row update. For example, our source
/// emits 1000 inserts in one chunk, and we aggregates count function on that.
/// Current `SimpleAggExecutor` will only emit one row for a whole chunk.
/// Therefore, we "automatically" implemented a window function inside
/// `SimpleAggExecutor`.
pub struct SimpleAggExecutor {
    schema: Schema,

    /// Aggregation states of the current operator
    states: Vec<Box<dyn StreamingAggStateImpl>>,

    /// The input of the current operator
    input: Box<dyn Executor>,

    /// Whether this is the first time of consuming data.
    ///
    /// Note that this is also part of the operator state, and should be
    /// persisted in the future.
    first_data: bool,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,
}

impl SimpleAggExecutor {
    pub fn new(input: Box<dyn Executor>, agg_calls: Vec<AggCall>) -> Result<Self> {
        let states: Vec<_> = agg_calls
            .iter()
            .map(|agg| {
                create_streaming_agg_state(agg.args.arg_types(), &agg.kind, &agg.return_type, None)
            })
            .try_collect()?;
        let schema = Schema {
            fields: agg_calls
                .iter()
                .map(|agg| Field {
                    data_type: agg.return_type.clone(),
                })
                .collect_vec(),
        };
        Ok(Self {
            schema,
            states,
            input,
            first_data: true,
            agg_calls,
        })
    }

    /// Record current states into a group of builders
    fn record_states(&mut self, builders: &mut [ArrayBuilderImpl]) -> Result<()> {
        for (state, builder) in self.states.iter().zip(builders.iter_mut()) {
            builder.append_datum(&state.get_output()?)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Executor for SimpleAggExecutor {
    async fn next(&mut self) -> Result<Message> {
        super::simple_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl SimpleExecutor for SimpleAggExecutor {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            ops,
            columns: arrays,
            visibility,
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

        self.states
            .iter_mut()
            .zip(self.agg_calls.iter())
            .try_for_each(|(state, agg)| match agg.args {
                AggArgs::None => state.apply_batch(&ops, visibility.as_ref(), &[]),
                AggArgs::Unary(_, col_idx) => {
                    state.apply_batch(&ops, visibility.as_ref(), &[arrays[col_idx].array_ref()])
                }
                AggArgs::Binary(_, indices) => state.apply_batch(
                    &ops,
                    visibility.as_ref(),
                    &[
                        arrays[indices[0]].array_ref(),
                        arrays[indices[1]].array_ref(),
                    ],
                ),
            })?;

        // output the current state into builder
        self.record_states(&mut builders)?;

        let chunk;

        let columns = builders
            .into_iter()
            .zip(self.agg_calls.iter())
            .map(|(builder, agg)| {
                Ok::<_, RwError>(Column::new(
                    Arc::new(builder.finish()?),
                    agg.return_type.clone(),
                ))
            })
            .try_collect()?;

        // For the first update, cardinality is 1. For the rest, cardinalty is 2,
        // which includes a deletion and a update.
        if self.first_data {
            chunk = StreamChunk {
                ops: vec![Op::Insert],
                visibility: None,
                columns,
            };
        } else {
            chunk = StreamChunk {
                ops: vec![Op::UpdateDelete, Op::UpdateInsert],
                visibility: None,
                columns,
            };
        }

        self.first_data = false;

        Ok(Message::Chunk(chunk))
    }
}
