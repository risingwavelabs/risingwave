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
use dyn_clone::{self, DynClone};
pub use foldable::*;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, ArrayRef, BoolArray, DecimalArray, F32Array,
    F64Array, I16Array, I32Array, I64Array, IntervalArray, ListArray, NaiveDateArray,
    NaiveDateTimeArray, NaiveTimeArray, Row, StructArray, Utf8Array,
};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema};
use risingwave_common::hash::HashCode;
use risingwave_common::types::{DataType, Datum};
use risingwave_expr::expr::AggKind;
use risingwave_expr::*;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::{Keyspace, StateStore};
pub use row_count::*;
use static_assertions::const_assert_eq;

use crate::executor::aggregation::single_value::StreamingSingleValueAgg;
use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::managed_state::aggregation::ManagedStateImpl;
use crate::executor::{Executor, PkDataTypes};

mod agg_call;
mod agg_state;
mod foldable;
mod row_count;
mod single_value;

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
                        Box::new(<$state_impl>::new_with_datum(datum)?)
                    }
                    (AggKind::$agg_type, $input_type! { type_match_pattern }, $return_type! { type_match_pattern }, None) => {
                        Box::new(<$state_impl>::new())
                    }
                )*
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
                    // Min
                    (Min, int16, int16, StreamingMinAgg::<I16Array>),
                    (Min, int32, int32, StreamingMinAgg::<I32Array>),
                    (Min, int64, int64, StreamingMinAgg::<I64Array>),
                    (Min, decimal, decimal, StreamingMinAgg::<DecimalArray>),
                    (Min, float32, float32, StreamingMinAgg::<F32Array>),
                    (Min, float64, float64, StreamingMinAgg::<F64Array>),
                    // Max
                    (Max, int16, int16, StreamingMaxAgg::<I16Array>),
                    (Max, int32, int32, StreamingMaxAgg::<I32Array>),
                    (Max, int64, int64, StreamingMaxAgg::<I64Array>),
                    (Max, decimal, decimal, StreamingMaxAgg::<DecimalArray>),
                    (Max, float32, float32, StreamingMaxAgg::<F32Array>),
                    (Max, float64, float64, StreamingMaxAgg::<F64Array>),
                    (
                        SingleValue,
                        int16,
                        int16,
                        StreamingSingleValueAgg::<I16Array>
                    ),
                    (
                        SingleValue,
                        int32,
                        int32,
                        StreamingSingleValueAgg::<I32Array>
                    ),
                    (
                        SingleValue,
                        int64,
                        int64,
                        StreamingSingleValueAgg::<I64Array>
                    ),
                    (
                        SingleValue,
                        float32,
                        float32,
                        StreamingSingleValueAgg::<F32Array>
                    ),
                    (
                        SingleValue,
                        float64,
                        float64,
                        StreamingSingleValueAgg::<F64Array>
                    ),
                    (
                        SingleValue,
                        boolean,
                        boolean,
                        StreamingSingleValueAgg::<BoolArray>
                    ),
                    (
                        SingleValue,
                        decimal,
                        decimal,
                        StreamingSingleValueAgg::<DecimalArray>
                    ),
                    (
                        SingleValue,
                        varchar,
                        varchar,
                        StreamingSingleValueAgg::<Utf8Array>
                    )
                ]
            )
        }
        [] => {
            match (agg_type, return_type, datum) {
                // `AggKind::Count` for partial/local Count(*) == RowCount while `AggKind::Sum` for
                // final/global Count(*)
                (AggKind::RowCount, DataType::Int64, Some(datum)) => {
                    Box::new(StreamingRowCountAgg::with_row_cnt(datum))
                }
                (AggKind::RowCount, DataType::Int64, None) => Box::new(StreamingRowCountAgg::new()),
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

/// Get clones of aggregation inputs by `agg_calls` and `columns`.
pub fn agg_input_arrays(agg_calls: &[AggCall], columns: &[Column]) -> Vec<Vec<ArrayRef>> {
    agg_calls
        .iter()
        .map(|agg| {
            agg.args
                .val_indices()
                .iter()
                .map(|val_idx| columns[*val_idx].array())
                .collect()
        })
        .collect()
}

/// Get references to aggregation inputs by `agg_calls` and `columns`.
pub fn agg_input_array_refs<'a>(
    agg_calls: &[AggCall],
    columns: &'a [Column],
) -> Vec<Vec<&'a ArrayImpl>> {
    agg_calls
        .iter()
        .map(|agg| {
            agg.args
                .val_indices()
                .iter()
                .map(|val_idx| columns[*val_idx].array_ref())
                .collect()
        })
        .collect()
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

/// Infer column desc for state table.
/// The column desc layout is
/// [ `group_key` (only for hash agg) / `sort_key` (only for simple agg) / `value`(the agg call
/// return type)].
/// This is the Row layout insert into state table.
/// For different agg call, different executor (hash agg or simple agg), the layout will be
/// different.
pub fn generate_column_descs(
    agg_call: &AggCall,
    group_keys: &[usize],
    pk_indices: &[usize],
    agg_schema: &Schema,
    input_ref: &dyn Executor,
) -> Vec<ColumnDesc> {
    let mut column_descs = Vec::with_capacity(group_keys.len() + 1);
    let mut next_column_id = 0;

    // Define a closure for DRY.
    let mut add_column_desc = |data_type: DataType| {
        column_descs.push(ColumnDesc::unnamed(
            ColumnId::new(next_column_id),
            data_type,
        ));
        next_column_id += 1;
    };

    for (idx, _) in group_keys.iter().enumerate() {
        add_column_desc(agg_schema.fields[idx].data_type.clone());
    }

    // For max, min, the table descs should include sort key.
    // The added columns should be (sort_key, pk from input data).
    if (agg_call.kind == AggKind::Max || agg_call.kind == AggKind::Min) && !agg_call.append_only {
        // Add value as part of sort key.
        add_column_desc(agg_call.return_type.clone());

        for pk_idx in pk_indices {
            add_column_desc(input_ref.schema().fields[*pk_idx].data_type.clone());
        }
    }

    // Agg value should also be part of state table.
    add_column_desc(agg_call.return_type.clone());

    column_descs
}
/// Generate initial [`AggState`] from `agg_calls`. For [`crate::executor::HashAggExecutor`], the
/// group key should be provided.
pub async fn generate_managed_agg_state<S: StateStore>(
    key: Option<&Row>,
    agg_calls: &[AggCall],
    keyspace: &[Keyspace<S>],
    pk_data_types: PkDataTypes,
    epoch: u64,
    key_hash_code: Option<HashCode>,
    state_tables: &[StateTable<S>],
) -> StreamExecutorResult<AggState<S>> {
    let mut managed_states = vec![];

    // Currently the loop here only works if `ROW_COUNT_COLUMN` is 0.
    const_assert_eq!(ROW_COUNT_COLUMN, 0);
    let mut row_count = None;

    for ((idx, agg_call), keyspace) in agg_calls.iter().enumerate().zip_eq(keyspace) {
        // TODO: in pure in-memory engine, we should not do this serialization.

        // The prefix of the state is `table_id/[group_key]`
        let keyspace = if let Some(key) = key {
            let bytes = key.serialize().unwrap();
            keyspace.append(bytes)
        } else {
            keyspace.clone()
        };

        let mut managed_state = ManagedStateImpl::create_managed_state(
            agg_call.clone(),
            keyspace,
            row_count,
            pk_data_types.clone(),
            idx == ROW_COUNT_COLUMN,
            key_hash_code.clone(),
            key,
            &state_tables[idx],
        )
        .await?;

        if idx == ROW_COUNT_COLUMN {
            // For the rowcount state, we should record the rowcount.
            let output = managed_state.get_output(epoch).await?;
            row_count = Some(output.as_ref().map(|x| *x.as_int64() as usize).unwrap_or(0));
        }

        managed_states.push(managed_state);
    }

    Ok(AggState {
        managed_states,
        prev_states: None,
    })
}

/// Get the pk keys len (Do not count group key).
/// For hash agg, add with group key to get internal table primary key len.
/// For simple agg,
pub fn get_key_len(agg_call: &AggCall) -> usize {
    match agg_call.kind {
        // If append_only, do not need order key.
        AggKind::Min | AggKind::Max => {
            if agg_call.append_only {
                0
            } else {
                1
            }
        }
        // These agg call do not have keys besides group key.
        AggKind::Sum | AggKind::Count | AggKind::SingleValue | AggKind::RowCount => 0,
        _ => unimplemented!("{:?} do not implemented!", agg_call.kind),
    }
}
