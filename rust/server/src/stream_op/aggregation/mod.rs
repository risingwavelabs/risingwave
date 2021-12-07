mod agg_call;
use std::any::Any;

pub use agg_call::*;

mod foldable;
pub use foldable::*;

mod row_count;
use risingwave_common::array::stream_chunk::Ops;
pub use row_count::*;

use super::{
    StreamingCountAgg, StreamingFloatMaxAgg, StreamingFloatMinAgg, StreamingFloatSumAgg,
    StreamingMaxAgg, StreamingMinAgg, StreamingSumAgg,
};
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, BoolArray, DecimalArray, F32Array, F64Array,
    I16Array, I32Array, I64Array, Utf8Array,
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
    macro_rules! gen_unary_agg_state_match {
    ($agg_type_expr:expr, $input_type_expr:expr, $return_type_expr:expr, $datum: expr, [$(($agg_type:ident, $input_type_kind:ident, $return_type_kind:ident, $state_impl:ty)),*$(,)?]) => {
      match (
        $agg_type_expr,
        $input_type_expr,
        $return_type_expr,
        $datum,
      ) {
        $(
          (AggKind::$agg_type, DataTypeKind::$input_type_kind, DataTypeKind::$return_type_kind, Some(datum)) => {
            Box::new(<$state_impl>::try_from(datum)?)
          }
          (AggKind::$agg_type, DataTypeKind::$input_type_kind, DataTypeKind::$return_type_kind, None) => {
            Box::new(<$state_impl>::new())
          }
        )*
        (other_agg, other_input, other_return, _) => panic!(
          "streaming state not implemented: {:?} {:?} {:?}",
          other_agg, other_input, other_return
        )
      }
    }
  }

    let state: Box<dyn StreamingAggStateImpl> = match input_types {
        [input_type] => {
            gen_unary_agg_state_match!(
                agg_type,
                input_type.data_type_kind(),
                return_type.data_type_kind(),
                datum,
                [
                    // Count
                    (Count, Int64, Int64, StreamingCountAgg::<I64Array>),
                    (Count, Int32, Int64, StreamingCountAgg::<I32Array>),
                    (Count, Int16, Int64, StreamingCountAgg::<I32Array>),
                    (Count, Float64, Int64, StreamingCountAgg::<F32Array>),
                    (Count, Float32, Int64, StreamingCountAgg::<F32Array>),
                    (Count, Decimal, Int64, StreamingCountAgg::<DecimalArray>),
                    (Count, Boolean, Int64, StreamingCountAgg::<BoolArray>),
                    (Count, Char, Int64, StreamingCountAgg::<Utf8Array>),
                    (Count, Varchar, Int64, StreamingCountAgg::<Utf8Array>),
                    // Sum
                    (Sum, Int64, Int64, StreamingSumAgg::<I64Array, I64Array>),
                    (
                        Sum,
                        Int64,
                        Decimal,
                        StreamingSumAgg::<DecimalArray, I64Array>
                    ),
                    (Sum, Int32, Int64, StreamingSumAgg::<I64Array, I32Array>),
                    (Sum, Int16, Int64, StreamingSumAgg::<I64Array, I16Array>),
                    (Sum, Int32, Int32, StreamingSumAgg::<I32Array, I32Array>),
                    (Sum, Int16, Int16, StreamingSumAgg::<I16Array, I16Array>),
                    (
                        Sum,
                        Float32,
                        Float64,
                        StreamingFloatSumAgg::<F64Array, F32Array>
                    ),
                    (
                        Sum,
                        Float32,
                        Float32,
                        StreamingFloatSumAgg::<F32Array, F32Array>
                    ),
                    (
                        Sum,
                        Float64,
                        Float64,
                        StreamingFloatSumAgg::<F64Array, F64Array>
                    ),
                    (
                        Sum,
                        Decimal,
                        Decimal,
                        StreamingSumAgg::<DecimalArray, DecimalArray>
                    ),
                    // Min
                    (Min, Int16, Int16, StreamingMinAgg::<I16Array>),
                    (Min, Int32, Int32, StreamingMinAgg::<I32Array>),
                    (Min, Int64, Int64, StreamingMinAgg::<I64Array>),
                    (Min, Decimal, Decimal, StreamingMinAgg::<DecimalArray>),
                    (Min, Float32, Float32, StreamingFloatMinAgg::<F32Array>),
                    (Min, Float64, Float64, StreamingFloatMinAgg::<F64Array>),
                    // Max
                    (Max, Int16, Int16, StreamingMaxAgg::<I16Array>),
                    (Max, Int32, Int32, StreamingMaxAgg::<I32Array>),
                    (Max, Int64, Int64, StreamingMaxAgg::<I64Array>),
                    (Max, Decimal, Decimal, StreamingMaxAgg::<DecimalArray>),
                    (Max, Float32, Float32, StreamingFloatMaxAgg::<F32Array>),
                    (Max, Float64, Float64, StreamingFloatMaxAgg::<F64Array>),
                ]
            )
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
