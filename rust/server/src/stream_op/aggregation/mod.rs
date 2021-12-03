mod agg_call;
use std::any::Any;

pub use agg_call::*;

mod foldable;
pub use foldable::*;

mod row_count;
use risingwave_common::array::stream_chunk::Ops;
pub use row_count::*;

mod keyed_state;
pub use keyed_state::*;

mod hash_kv;
pub use hash_kv::*;

use super::{
    StreamingCountAgg, StreamingFloatMaxAgg, StreamingFloatMinAgg, StreamingFloatSumAgg,
    StreamingMaxAgg, StreamingMinAgg, StreamingSumAgg,
};
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, DecimalArray, F32Array, F64Array, I16Array,
    I32Array, I64Array,
};
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::expr::AggKind;
use risingwave_common::types::{DataTypeKind, DataTypeRef, Datum};

use dyn_clone::{self, DynClone};

/// `StreamingAggState` records a state of streaming expression. For example,
/// there will be `StreamingAggCompare` and `StreamingAggSum`.
pub trait StreamingAggState<A: Array>: Send + Sync + 'static {
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &A,
    ) -> Result<()>;
}

/// `StreamingAggFunction` allows us to get output from a streaming state.
pub trait StreamingAggFunction<B: ArrayBuilder>: Send + Sync + 'static {
    fn get_output_concrete(&self) -> Result<Option<<B::ArrayType as Array>::OwnedItem>>;
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
    ) -> Result<()>;

    /// Get the output value
    fn get_output(&self) -> Result<Datum>;

    /// Get the builder of the state output
    fn new_builder(&self) -> ArrayBuilderImpl;
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
    input_types: &[DataTypeRef],
    agg_type: &AggKind,
    return_type: &DataTypeRef,
    datum: Option<Datum>,
) -> Result<Box<dyn StreamingAggStateImpl>> {
    let state: Box<dyn StreamingAggStateImpl> = match input_types {
        [input_type] => {
            match (
                input_type.data_type_kind(),
                agg_type,
                return_type.data_type_kind(),
                datum,
            ) {
                (_, AggKind::Count, DataTypeKind::Int64, Some(datum)) => {
                    Box::new(StreamingCountAgg::<I64Array>::try_from(datum)?)
                }
                (_, AggKind::Count, DataTypeKind::Int64, None) => {
                    Box::new(StreamingCountAgg::<I64Array>::new())
                }
                (DataTypeKind::Int64, AggKind::Sum, DataTypeKind::Int64, Some(datum)) => {
                    Box::new(StreamingSumAgg::<I64Array, I64Array>::try_from(datum)?)
                }
                (DataTypeKind::Int64, AggKind::Sum, DataTypeKind::Int64, None) => {
                    Box::new(StreamingSumAgg::<I64Array, I64Array>::new())
                }
                (DataTypeKind::Int32, AggKind::Sum, DataTypeKind::Int64, Some(datum)) => {
                    Box::new(StreamingSumAgg::<I64Array, I32Array>::try_from(datum)?)
                }
                (DataTypeKind::Int32, AggKind::Sum, DataTypeKind::Int64, None) => {
                    Box::new(StreamingSumAgg::<I64Array, I32Array>::new())
                }
                (DataTypeKind::Int16, AggKind::Sum, DataTypeKind::Int64, Some(datum)) => {
                    Box::new(StreamingSumAgg::<I64Array, I16Array>::try_from(datum)?)
                }
                (DataTypeKind::Int16, AggKind::Sum, DataTypeKind::Int64, None) => {
                    Box::new(StreamingSumAgg::<I64Array, I16Array>::new())
                }
                (DataTypeKind::Int64, AggKind::Sum, DataTypeKind::Decimal, Some(datum)) => {
                    Box::new(StreamingSumAgg::<DecimalArray, I64Array>::try_from(datum)?)
                }
                (DataTypeKind::Int64, AggKind::Sum, DataTypeKind::Decimal, None) => {
                    Box::new(StreamingSumAgg::<DecimalArray, I64Array>::new())
                }
                (DataTypeKind::Float32, AggKind::Sum, DataTypeKind::Float64, Some(datum)) => {
                    Box::new(StreamingFloatSumAgg::<F64Array, F32Array>::try_from(datum)?)
                }
                (DataTypeKind::Float32, AggKind::Sum, DataTypeKind::Float64, None) => {
                    Box::new(StreamingFloatSumAgg::<F64Array, F32Array>::new())
                }
                (DataTypeKind::Float64, AggKind::Sum, DataTypeKind::Float64, Some(datum)) => {
                    Box::new(StreamingFloatSumAgg::<F64Array, F64Array>::try_from(datum)?)
                }
                (DataTypeKind::Float64, AggKind::Sum, DataTypeKind::Float64, None) => {
                    Box::new(StreamingFloatSumAgg::<F64Array, F64Array>::new())
                }
                (DataTypeKind::Float32, AggKind::Sum, DataTypeKind::Float32, Some(datum)) => {
                    Box::new(StreamingFloatSumAgg::<F32Array, F32Array>::try_from(datum)?)
                }
                (DataTypeKind::Float32, AggKind::Sum, DataTypeKind::Float32, None) => {
                    Box::new(StreamingFloatSumAgg::<F32Array, F32Array>::new())
                }
                (DataTypeKind::Decimal, AggKind::Sum, DataTypeKind::Decimal, Some(datum)) => {
                    Box::new(StreamingSumAgg::<DecimalArray, DecimalArray>::try_from(
                        datum,
                    )?)
                }
                (DataTypeKind::Decimal, AggKind::Sum, DataTypeKind::Decimal, None) => {
                    Box::new(StreamingSumAgg::<DecimalArray, DecimalArray>::new())
                }
                (DataTypeKind::Int16, AggKind::Min, DataTypeKind::Int16, Some(datum)) => {
                    Box::new(StreamingMinAgg::<I16Array>::try_from(datum)?)
                }
                (DataTypeKind::Int16, AggKind::Min, DataTypeKind::Int16, None) => {
                    Box::new(StreamingMinAgg::<I16Array>::new())
                }
                (DataTypeKind::Int32, AggKind::Min, DataTypeKind::Int32, Some(datum)) => {
                    Box::new(StreamingMinAgg::<I32Array>::try_from(datum)?)
                }
                (DataTypeKind::Int32, AggKind::Min, DataTypeKind::Int32, None) => {
                    Box::new(StreamingMinAgg::<I32Array>::new())
                }
                (DataTypeKind::Int64, AggKind::Min, DataTypeKind::Int64, Some(datum)) => {
                    Box::new(StreamingMinAgg::<I64Array>::try_from(datum)?)
                }
                (DataTypeKind::Int64, AggKind::Min, DataTypeKind::Int64, None) => {
                    Box::new(StreamingMinAgg::<I64Array>::new())
                }
                (DataTypeKind::Float32, AggKind::Min, DataTypeKind::Float32, Some(datum)) => {
                    Box::new(StreamingFloatMinAgg::<F32Array>::try_from(datum)?)
                }
                (DataTypeKind::Float32, AggKind::Min, DataTypeKind::Float32, None) => {
                    Box::new(StreamingFloatMinAgg::<F32Array>::new())
                }
                (DataTypeKind::Float64, AggKind::Min, DataTypeKind::Float64, Some(datum)) => {
                    Box::new(StreamingFloatMinAgg::<F64Array>::try_from(datum)?)
                }
                (DataTypeKind::Float64, AggKind::Min, DataTypeKind::Float64, None) => {
                    Box::new(StreamingFloatMinAgg::<F64Array>::new())
                }
                (DataTypeKind::Int16, AggKind::Max, DataTypeKind::Int16, Some(datum)) => {
                    Box::new(StreamingMaxAgg::<I16Array>::try_from(datum)?)
                }
                (DataTypeKind::Int16, AggKind::Max, DataTypeKind::Int16, None) => {
                    Box::new(StreamingMaxAgg::<I16Array>::new())
                }
                (DataTypeKind::Int32, AggKind::Max, DataTypeKind::Int32, Some(datum)) => {
                    Box::new(StreamingMaxAgg::<I32Array>::try_from(datum)?)
                }
                (DataTypeKind::Int32, AggKind::Max, DataTypeKind::Int32, None) => {
                    Box::new(StreamingMaxAgg::<I32Array>::new())
                }
                (DataTypeKind::Int64, AggKind::Max, DataTypeKind::Int64, Some(datum)) => {
                    Box::new(StreamingMaxAgg::<I64Array>::try_from(datum)?)
                }
                (DataTypeKind::Int64, AggKind::Max, DataTypeKind::Int64, None) => {
                    Box::new(StreamingMaxAgg::<I64Array>::new())
                }
                (DataTypeKind::Float32, AggKind::Max, DataTypeKind::Float32, Some(datum)) => {
                    Box::new(StreamingFloatMaxAgg::<F32Array>::try_from(datum)?)
                }
                (DataTypeKind::Float32, AggKind::Max, DataTypeKind::Float32, None) => {
                    Box::new(StreamingFloatMaxAgg::<F32Array>::new())
                }
                (DataTypeKind::Float64, AggKind::Max, DataTypeKind::Float64, Some(datum)) => {
                    Box::new(StreamingFloatMaxAgg::<F64Array>::try_from(datum)?)
                }
                (DataTypeKind::Float64, AggKind::Max, DataTypeKind::Float64, None) => {
                    Box::new(StreamingFloatMaxAgg::<F64Array>::new())
                }
                (other_input, other_agg, other_return, _) => panic!(
                    "streaming state not implemented: {:?} {:?} {:?}",
                    other_input, other_agg, other_return
                ),
            }
        }
        [] => {
            match (agg_type, return_type.data_type_kind(), datum) {
                // `AggKind::Count` for partial/local Count(*) == RowCount while `AggKind::Sum` for
                // final/global Count(*)
                (AggKind::RowCount, DataTypeKind::Int64, Some(datum)) => {
                    Box::new(StreamingRowCountAgg::with_row_cnt(datum))
                }
                (AggKind::RowCount, DataTypeKind::Int64, None) => {
                    Box::new(StreamingRowCountAgg::new())
                }
                // According to the function header comments and the link, Count(*) == RowCount
                // `StreamingCountAgg` does not count `NULL`, so we use `StreamingRowCountAgg` here.
                (AggKind::Count, DataTypeKind::Int64, Some(datum)) => {
                    Box::new(StreamingRowCountAgg::with_row_cnt(datum))
                }
                (AggKind::Count, DataTypeKind::Int64, None) => {
                    Box::new(StreamingRowCountAgg::new())
                }
                _ => unimplemented!(),
            }
        }
        _ => todo!(),
    };
    Ok(state)
}
