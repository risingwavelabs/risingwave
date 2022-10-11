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
use anyhow::anyhow;
use async_trait::async_trait;
pub use cache::*;
use dyn_clone::{self, DynClone};
pub use foldable::*;
use itertools::Itertools;
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
pub use state_manager::*;
pub use value::*;

use self::array_agg::ManagedArrayAggState;
use self::extreme::GenericExtremeState;
use self::string_agg::ManagedStringAggState;
use super::{ActorContextRef, PkIndices};
use crate::common::{InfallibleExpression, StateTableColumnMapping};
use crate::executor::aggregation::approx_count_distinct::StreamingApproxCountDistinct;
use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::Executor;

mod agg_call;
mod approx_count_distinct;
mod array_agg;
mod cache;
mod extreme;
mod foldable;
mod row_count;
mod state_manager;
mod string_agg;
mod value;

/// `StreamingSumAgg` sums data of the same type.
pub type StreamingSumAgg<R, I> =
    StreamingFoldAgg<R, I, PrimitiveSummable<<R as Array>::OwnedItem, <I as Array>::OwnedItem>>;

/// `StreamingCountAgg` counts data of any type.
pub type StreamingCountAgg<S> = StreamingFoldAgg<I64Array, S, Countable<<S as Array>::OwnedItem>>;

/// `StreamingMinAgg` get minimum data of the same type.
pub type StreamingMinAgg<S> = StreamingFoldAgg<S, S, Minimizable<<S as Array>::OwnedItem>>;

/// `StreamingMaxAgg` get maximum data of the same type.
pub type StreamingMaxAgg<S> = StreamingFoldAgg<S, S, Maximizable<<S as Array>::OwnedItem>>;

/// `StreamingAggInput` describes the functions needed to feed input data to aggregators.
pub trait StreamingAggInput<A: Array>: Send + Sync + 'static {
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &A,
    ) -> StreamExecutorResult<()>;
}

/// `StreamingAggOutput` allows us to get output from a streaming aggregators.
pub trait StreamingAggOutput<B: ArrayBuilder>: Send + Sync + 'static {
    fn get_output_concrete(
        &self,
    ) -> StreamExecutorResult<Option<<B::ArrayType as Array>::OwnedItem>>;
}

/// `StreamingAggImpl` erases the associated type information of `StreamingAggInput` and
/// `StreamingAggOutput`. You should manually implement this trait for necessary types.
pub trait StreamingAggImpl: Any + std::fmt::Debug + DynClone + Send + Sync + 'static {
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

dyn_clone::clone_trait_object!(StreamingAggImpl);

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
pub fn create_streaming_aggregator(
    input_types: &[DataType],
    agg_type: &AggKind,
    return_type: &DataType,
    datum: Option<Datum>,
) -> StreamExecutorResult<Box<dyn StreamingAggImpl>> {
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

    let state: Box<dyn StreamingAggImpl> = match input_types {
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

// TODO(RC): move this to `AggStateManager::create`
/// Create [`crate::executor::aggregation::state_manager::AggStateManager`] for `agg_calls`.
/// For [`crate::executor::HashAggExecutor`], the group key should be provided.
pub async fn create_agg_state_manager<S: StateStore>(
    group_key: Option<Row>,
    agg_calls: &[AggCall],
    agg_state_tables: &[Option<AggStateTable<S>>],
    result_table: &StateTable<S>,
    pk_indices: &PkIndices,
    extreme_cache_size: usize,
    input_schema: &Schema,
) -> StreamExecutorResult<AggStateManager<S>> {
    let prev_result: Option<Row> = result_table
        .get_row(group_key.as_ref().unwrap_or_else(Row::empty))
        .await?;
    let prev_outputs: Option<Vec<_>> = prev_result.map(|row| row.0);
    if let Some(prev_outputs) = prev_outputs.as_ref() {
        assert_eq!(prev_outputs.len(), agg_calls.len());
    }

    let row_count = prev_outputs
        .as_ref()
        .and_then(|outputs| {
            outputs[ROW_COUNT_COLUMN]
                .clone()
                .map(|x| x.into_int64() as usize)
        })
        .unwrap_or(0);

    let managed_states = agg_calls
        .iter()
        .enumerate()
        .map(|(idx, agg_call)| {
            ManagedAggState::create(
                agg_call,
                agg_state_tables[idx].as_ref(),
                row_count,
                prev_outputs.as_ref().map(|outputs| &outputs[idx]),
                pk_indices,
                group_key.as_ref(),
                extreme_cache_size,
                input_schema,
            )
        })
        .try_collect()?;

    Ok(AggStateManager::new(
        group_key,
        managed_states,
        prev_outputs,
    ))
}

pub fn agg_call_filter_res(
    ctx: &ActorContextRef,
    identity: &str,
    agg_call: &AggCall,
    columns: &Vec<Column>,
    visibility: Option<&Bitmap>,
    capacity: usize,
) -> StreamExecutorResult<Option<Bitmap>> {
    if let Some(ref filter) = agg_call.filter {
        let vis = Vis::from(
            visibility
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
        Ok(visibility.cloned())
    }
}

/// State table and column mapping for `MaterializedState` variant of `AggCallState`.
pub struct AggStateTable<S: StateStore> {
    pub table: StateTable<S>,
    pub mapping: StateTableColumnMapping,
}

pub fn for_each_agg_state_table<S: StateStore, F: Fn(&mut AggStateTable<S>)>(
    agg_state_tables: &mut [Option<AggStateTable<S>>],
    f: F,
) {
    agg_state_tables
        .iter_mut()
        .filter_map(Option::as_mut)
        .for_each(|state_table| {
            f(state_table);
        });
}

/// Verify if the data going through the state is valid by checking if `ops.len() ==
/// visibility.len() == data[x].len()`.
pub fn verify_batch(
    ops: risingwave_common::array::stream_chunk::Ops<'_>,
    visibility: Option<&risingwave_common::buffer::Bitmap>,
    data: &[&risingwave_common::array::ArrayImpl],
) -> bool {
    let mut all_lengths = vec![ops.len()];
    if let Some(visibility) = visibility {
        all_lengths.push(visibility.len());
    }
    all_lengths.extend(data.iter().map(|x| x.len()));
    all_lengths.iter().min() == all_lengths.iter().max()
}

// TODO(RC): rename
/// A trait over all table-structured states.
///
/// It is true that this interface also fits to value managed state, but we won't implement
/// `ManagedTableState` for them. We want to reduce the overhead of `BoxedFuture`. For
/// `ManagedValueState`, we can directly forward its async functions to `ManagedStateImpl`, instead
/// of adding a layer of indirection caused by async traits.
#[async_trait]
pub trait ManagedTableState<S: StateStore>: Send + Sync + 'static {
    async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()>;

    /// Get the output of the state. Must flush before getting output.
    async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum>;
}

/// All managed state for aggregation. The managed state will manage the cache and integrate
/// the state with the underlying state store. Managed states can only be evicted from outer cache
/// when they are not dirty.
pub enum ManagedAggState<S: StateStore> {
    /// States as single scalar value e.g. `COUNT`, `SUM`
    InMemoryValueState(InMemoryValueState),

    /// States as table structure e.g. `MAX`, `STRING_AGG`
    MaterializedInputState(Box<dyn ManagedTableState<S>>),
}

impl<S: StateStore> ManagedAggState<S> {
    pub async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        agg_state_table: Option<&mut AggStateTable<S>>,
    ) -> StreamExecutorResult<()> {
        match self {
            Self::InMemoryValueState(state) => state.apply_chunk(ops, visibility, columns),
            Self::MaterializedInputState(state) => {
                state
                    .apply_chunk(
                        ops,
                        visibility,
                        columns,
                        &mut agg_state_table
                            .expect("managed table state must have state table")
                            .table,
                    )
                    .await
            }
        }
    }

    /// Get the output of the state. Must flush before getting output.
    pub async fn get_output(
        &mut self,
        agg_state_table: Option<&AggStateTable<S>>,
    ) -> StreamExecutorResult<Datum> {
        match self {
            Self::InMemoryValueState(state) => Ok(state.get_output()),
            Self::MaterializedInputState(state) => {
                state
                    .get_output(
                        &agg_state_table
                            .expect("managed table state must have state table")
                            .table,
                    )
                    .await
            }
        }
    }

    /// Create a managed agg state for the given `agg_call`.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        agg_call: &AggCall,
        agg_state_table: Option<&AggStateTable<S>>,
        row_count: usize,
        prev_output: Option<&Datum>,
        pk_indices: &PkIndices,
        group_key: Option<&Row>,
        extreme_cache_size: usize,
        input_schema: &Schema,
    ) -> StreamExecutorResult<Self> {
        match agg_call.kind {
            AggKind::Avg | AggKind::Count | AggKind::Sum | AggKind::ApproxCountDistinct => Ok(
                Self::InMemoryValueState(InMemoryValueState::new(agg_call, prev_output.cloned())?),
            ),
            // optimization: use single-value state for append-only min/max
            AggKind::Max | AggKind::Min if agg_call.append_only => Ok(Self::InMemoryValueState(
                InMemoryValueState::new(agg_call, prev_output.cloned())?,
            )),
            AggKind::Max | AggKind::Min => Ok(Self::MaterializedInputState(Box::new(
                GenericExtremeState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    agg_state_table
                        .expect("non-append-only min/max must have state table")
                        .mapping
                        .clone(),
                    row_count,
                    extreme_cache_size,
                    input_schema,
                ),
            ))),
            AggKind::StringAgg => Ok(Self::MaterializedInputState(Box::new(
                ManagedStringAggState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    agg_state_table
                        .expect("string_agg must have state table")
                        .mapping
                        .clone(),
                    row_count,
                ),
            ))),
            AggKind::ArrayAgg => Ok(Self::MaterializedInputState(Box::new(
                ManagedArrayAggState::new(
                    agg_call,
                    group_key,
                    pk_indices,
                    agg_state_table
                        .expect("array_agg must have state table")
                        .mapping
                        .clone(),
                    row_count,
                ),
            ))),
        }
    }
}
