// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

pub use agg_call::*;
pub use agg_state::*;
pub use agg_state_table::*;
use anyhow::anyhow;
use dyn_clone::{self, DynClone};
pub use foldable::*;
use risingwave_common::array::column::Column;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl::Bool;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, BoolArray, DataChunk, DecimalArray, F32Array,
    F64Array, I16Array, I32Array, I64Array, IntervalArray, ListArray, NaiveDateArray,
    NaiveDateTimeArray, NaiveTimeArray, Row, StructArray, Utf8Array, Vis,
};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::AggKind;
use risingwave_expr::*;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;
pub use row_count::*;
use static_assertions::const_assert_eq;

use super::{ActorContextRef, PkIndices};
use crate::common::InfallibleExpression;
use crate::executor::aggregation::approx_count_distinct::StreamingApproxCountDistinct;
use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::managed_state::aggregation::ManagedStateImpl;
use crate::executor::Executor;

mod agg_call;
mod agg_state;
mod agg_state_table;
mod approx_count_distinct;
mod foldable;
mod row_count;

/// `StreamingSumAgg` sums data of the same type.
pub type StreamingSumAgg<R, I> =
    StreamingFoldAgg<R, I, PrimitiveSummable<<R as Array>::OwnedItem, <I as Array>::OwnedItem>>;

/// `StreamingCountAgg` counts data of any type.
pub type StreamingCountAgg<S> = StreamingFoldAgg<I64Array, S, Countable<<S as Array>::OwnedItem>>;

/// `StreamingMinAgg` get minimum data of the same type.
pub type StreamingMinAgg<S> = StreamingFoldAgg<S, S, Minimizable<<S as Array>::OwnedItem>>;

/// `StreamingMaxAgg` get maximum data of the same type.
pub type StreamingMaxAgg<S> = StreamingFoldAgg<S, S, Maximizable<<S as Array>::OwnedItem>>;

/// `StreamingAggState` records a state of streaming expression. For example,
/// there will be `StreamingAggCompare` and `StreamingAggSum`.
pub trait StreamingAggState<A: Array>: Send + Sync + 'static {
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &A,
    ) -> StreamExecutorResult<()>;
}

/// `StreamingAggFunction` allows us to get output from a streaming state.
pub trait StreamingAggFunction<B: ArrayBuilder>: Send + Sync + 'static {
    fn get_output_concrete(
        &self,
    ) -> StreamExecutorResult<Option<<B::ArrayType as Array>::OwnedItem>>;
}

/// `StreamingAggStateImpl` erases the associated type information of
/// `StreamingAggState` and `StreamingAggFunction`. You should manually
/// implement this trait for necessary types.
pub trait StreamingAggStateImpl: Any + std::fmt::Debug + DynClone + Send + Sync + 'static {
    /// Apply a batch to the state
    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> StreamExecutorResult<()>;

    /// Get the output value
    fn get_output(&self) -> StreamExecutorResult<Datum>;

    /// Get the builder of the state output
    fn new_builder(&self) -> ArrayBuilderImpl;

    /// Reset to initial state
    fn reset(&mut self);
}

dyn_clone::clone_trait_object!(StreamingAggStateImpl);

/// [postgresql specification of aggregate functions](https://www.postgresql.org/docs/13/functions-aggregate.html)
/// Most of the general-purpose aggregate functions have one input except for:
/// 1. `count(*) -> bigint`. The input type of count(*)
/// 2. `json_object_agg ( key "any", value "any" ) -> json`
/// 3. `jsonb_object_agg ( key "any", value "any" ) -> jsonb`
/// 4. `string_agg ( value text, delimiter text ) -> text`
/// 5. `string_agg ( value bytea, delimiter bytea ) -> bytea`
/// We remark that there is difference between `count(*)` and `count(any)`:
/// 1. `count(*)` computes the number of input rows. And the semantics of row count is equal to the
/// semantics of `count(*)` 2. `count("any")` computes the number of input rows in which the input
/// value is not null.
pub fn create_streaming_agg_state(
    input_types: &[DataType],
    agg_type: &AggKind,
    return_type: &DataType,
    datum: Option<Datum>,
) -> StreamExecutorResult<Box<dyn StreamingAggStateImpl>> {
    macro_rules! gen_unary_agg_state_match {
        ($agg_type_expr:expr, $input_type_expr:expr, $return_type_expr:expr, $datum: expr,
            [$(($agg_type:ident, $input_type:ident, $return_type:ident, $state_impl:ty)),*$(,)?]) => {
            match (
                $agg_type_expr,
                $input_type_expr,
                $return_type_expr,
                $datum,
            ) {
                $(
                    (AggKind::$agg_type, $input_type! { type_match_pattern }, $return_type! { type_match_pattern }, Some(datum)) => {
                        Box::new(<$state_impl>::with_datum(datum)?)
                    }
                    (AggKind::$agg_type, $input_type! { type_match_pattern }, $return_type! { type_match_pattern }, None) => {
                        Box::new(<$state_impl>::new())
                    }
                )*
                (AggKind::ApproxCountDistinct, _, DataType::Int64, Some(datum)) => {
                    Box::new(StreamingApproxCountDistinct::<{approx_count_distinct::DENSE_BITS_DEFAULT}>::with_datum(datum))
                }
                (AggKind::ApproxCountDistinct, _, DataType::Int64, None) => {
                    Box::new(StreamingApproxCountDistinct::<{approx_count_distinct::DENSE_BITS_DEFAULT}>::new())
                }
                (other_agg, other_input, other_return, _) => panic!(
                    "streaming agg state not implemented: {:?} {:?} {:?}",
                    other_agg, other_input, other_return
                )
            }
        }
    }

    let state: Box<dyn StreamingAggStateImpl> = match input_types {
        [input_type] => {
            gen_unary_agg_state_match!(
                agg_type,
                input_type,
                return_type,
                datum,
                [
                    // Count
                    (Count, int64, int64, StreamingCountAgg::<I64Array>),
                    (Count, int32, int64, StreamingCountAgg::<I32Array>),
                    (Count, int16, int64, StreamingCountAgg::<I16Array>),
                    (Count, float64, int64, StreamingCountAgg::<F64Array>),
                    (Count, float32, int64, StreamingCountAgg::<F32Array>),
                    (Count, decimal, int64, StreamingCountAgg::<DecimalArray>),
                    (Count, boolean, int64, StreamingCountAgg::<BoolArray>),
                    (Count, varchar, int64, StreamingCountAgg::<Utf8Array>),
                    (Count, interval, int64, StreamingCountAgg::<IntervalArray>),
                    (Count, date, int64, StreamingCountAgg::<NaiveDateArray>),
                    (
                        Count,
                        timestamp,
                        int64,
                        StreamingCountAgg::<NaiveDateTimeArray>
                    ),
                    (Count, time, int64, StreamingCountAgg::<NaiveTimeArray>),
                    (Count, struct_type, int64, StreamingCountAgg::<StructArray>),
                    (Count, list, int64, StreamingCountAgg::<ListArray>),
                    // Sum
                    (Sum, int64, int64, StreamingSumAgg::<I64Array, I64Array>),
                    (
                        Sum,
                        int64,
                        decimal,
                        StreamingSumAgg::<DecimalArray, I64Array>
                    ),
                    (Sum, int32, int64, StreamingSumAgg::<I64Array, I32Array>),
                    (Sum, int16, int64, StreamingSumAgg::<I64Array, I16Array>),
                    (Sum, int32, int32, StreamingSumAgg::<I32Array, I32Array>),
                    (Sum, int16, int16, StreamingSumAgg::<I16Array, I16Array>),
                    (Sum, float32, float64, StreamingSumAgg::<F64Array, F32Array>),
                    (Sum, float32, float32, StreamingSumAgg::<F32Array, F32Array>),
                    (Sum, float64, float64, StreamingSumAgg::<F64Array, F64Array>),
                    (
                        Sum,
                        decimal,
                        decimal,
                        StreamingSumAgg::<DecimalArray, DecimalArray>
                    ),
                    (
                        Sum,
                        interval,
                        interval,
                        StreamingSumAgg::<IntervalArray, IntervalArray>
                    ),
                    // Min
                    (Min, int16, int16, StreamingMinAgg::<I16Array>),
                    (Min, int32, int32, StreamingMinAgg::<I32Array>),
                    (Min, int64, int64, StreamingMinAgg::<I64Array>),
                    (Min, decimal, decimal, StreamingMinAgg::<DecimalArray>),
                    (Min, float32, float32, StreamingMinAgg::<F32Array>),
                    (Min, float64, float64, StreamingMinAgg::<F64Array>),
                    (Min, interval, interval, StreamingMinAgg::<IntervalArray>),
                    // Max
                    (Max, int16, int16, StreamingMaxAgg::<I16Array>),
                    (Max, int32, int32, StreamingMaxAgg::<I32Array>),
                    (Max, int64, int64, StreamingMaxAgg::<I64Array>),
                    (Max, decimal, decimal, StreamingMaxAgg::<DecimalArray>),
                    (Max, float32, float32, StreamingMaxAgg::<F32Array>),
                    (Max, float64, float64, StreamingMaxAgg::<F64Array>),
                    (Max, interval, interval, StreamingMaxAgg::<IntervalArray>),
                ]
            )
        }
        [] => {
            match (agg_type, return_type, datum) {
                // According to the function header comments and the link, Count(*) == RowCount
                // `StreamingCountAgg` does not count `NULL`, so we use `StreamingRowCountAgg` here.
                (AggKind::Count, DataType::Int64, Some(datum)) => {
                    Box::new(StreamingRowCountAgg::with_row_cnt(datum))
                }
                (AggKind::Count, DataType::Int64, None) => Box::new(StreamingRowCountAgg::new()),
                _ => {
                    return Err(StreamExecutorError::not_implemented(
                        "unsupported aggregate type",
                        None,
                    ))
                }
            }
        }
        _ => todo!(),
    };
    Ok(state)
}

/// Generate [`crate::executor::HashAggExecutor`]'s schema from `input`, `agg_calls` and
/// `group_key_indices`. For [`crate::executor::HashAggExecutor`], the group key indices should
/// be provided.
pub fn generate_agg_schema(
    input: &dyn Executor,
    agg_calls: &[AggCall],
    group_key_indices: Option<&[usize]>,
) -> Schema {
    let aggs = agg_calls
        .iter()
        .map(|agg| Field::unnamed(agg.return_type.clone()));

    let fields = if let Some(key_indices) = group_key_indices {
        let keys = key_indices
            .iter()
            .map(|idx| input.schema().fields[*idx].clone());

        keys.chain(aggs).collect()
    } else {
        aggs.collect()
    };

    Schema { fields }
}

/// Generate initial [`AggState`] from `agg_calls`. For [`crate::executor::HashAggExecutor`], the
/// group key should be provided.
pub async fn generate_managed_agg_state<S: StateStore>(
    group_key: Option<Row>,
    agg_calls: &[AggCall],
    agg_state_tables: &[Option<AggStateTable<S>>],
    result_table: &StateTable<S>,
    pk_indices: PkIndices,
    extreme_cache_size: usize,
) -> StreamExecutorResult<AggState<S>> {
    let group_key_len = group_key.as_ref().map_or(0, |row| row.size());

    let prev_result: Option<Row> = result_table
        .get_row(group_key.as_ref().unwrap_or(&Row::empty()))
        .await?;
    let prev_outputs: Option<Vec<_>> =
        prev_result.map(|row| row.0.into_iter().skip(group_key_len).collect());
    if let Some(prev_outputs) = prev_outputs.as_ref() {
        assert_eq!(prev_outputs.len(), agg_calls.len());
    }

    // Currently the loop here only works if `ROW_COUNT_COLUMN` is 0.
    const_assert_eq!(ROW_COUNT_COLUMN, 0);
    let row_count = prev_outputs
        .as_ref()
        .map(|outputs| {
            outputs[ROW_COUNT_COLUMN]
                .clone()
                .map(|x| x.into_int64() as usize)
        })
        .flatten()
        .unwrap_or(0);

    let managed_states = agg_calls
        .iter()
        .enumerate()
        .map(|(idx, agg_call)| {
            ManagedStateImpl::create_managed_state(
                agg_call.clone(), // TODO(rc): `clone` can be removed
                agg_state_tables[idx].as_ref(),
                Some(row_count), // TODO(rc): may remove the `Some`
                prev_outputs.as_ref().map(|outputs| outputs[idx].clone()),
                pk_indices.clone(),
                idx == ROW_COUNT_COLUMN,
                group_key.as_ref(),
                extreme_cache_size,
            )
        })
        .try_collect()?;

    Ok(AggState::new(group_key, managed_states, prev_outputs))
}

pub fn agg_call_filter_res(
    ctx: &ActorContextRef,
    identity: &str,
    agg_call: &AggCall,
    columns: &Vec<Column>,
    vis_map: Option<&Bitmap>,
    capacity: usize,
) -> StreamExecutorResult<Option<Bitmap>> {
    if let Some(ref filter) = agg_call.filter {
        let vis = Vis::from(
            vis_map
                .cloned()
                .unwrap_or_else(|| Bitmap::all_high_bits(capacity)),
        );
        let data_chunk = DataChunk::new(columns.to_owned(), vis);
        if let Bool(filter_res) = filter
            .eval_infallible(&data_chunk, |err| ctx.on_compute_error(err, identity))
            .as_ref()
        {
            Ok(Some(filter_res.to_bitmap()))
        } else {
            Err(StreamExecutorError::from(anyhow!(
                "Filter can only receive bool array"
            )))
        }
    } else {
        Ok(vis_map.cloned())
    }
}
