mod agg_call;
pub use agg_call::*;

mod foldable;
pub use foldable::*;

mod row_count;
pub use row_count::*;

mod avg;
pub use avg::*;

use super::{
    Op, Ops, StreamingCountAgg, StreamingFloatMaxAgg, StreamingFloatMinAgg, StreamingFloatSumAgg,
    StreamingMaxAgg, StreamingMinAgg, StreamingSumAgg,
};
use risingwave_common::array::{
    Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, F32Array, F64Array, I16Array, I32Array,
    I64Array,
};
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::expr::AggKind;
use risingwave_common::types::{DataTypeKind, DataTypeRef};

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
    fn get_output_concrete(&self, builder: &mut B) -> Result<()>;
}

/// `StreamingAggStateImpl` erases the associated type information of
/// `StreamingAggState` and `StreamingAggFunction`. You should manually
/// implement this trait for necessary types.
pub trait StreamingAggStateImpl: Send + Sync + 'static {
    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()>;

    fn get_output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;

    fn new_builder(&self) -> ArrayBuilderImpl;
}

/// [postgresql specification of aggregate functions](https://www.postgresql.org/docs/13/functions-aggregate.html)
/// Most of the general-purpose aggregate functions have one input except for:
/// 1. `count(*) -> bigint`. The input type of count(*)
/// 2. `json_object_agg ( key "any", value "any" ) -> json`
/// 3. `jsonb_object_agg ( key "any", value "any" ) -> jsonb`
/// 4. `string_agg ( value text, delimiter text ) -> text`
/// 5. `string_agg ( value bytea, delimiter bytea ) -> bytea`
/// We remark that there is difference between `count(*)` and `count(any)`:
/// 1. `count(*)` computes the number of input rows. And the semantics of row count is equal to the semantics of `count(*)`
/// 2. `count("any")` computes the number of input rows in which the input value is not null.
pub fn create_streaming_agg_state(
    input_types: &[DataTypeRef],
    agg_type: &AggKind,
    return_type: &DataTypeRef,
) -> Result<Box<dyn StreamingAggStateImpl>> {
    let state: Box<dyn StreamingAggStateImpl> = match input_types {
        [input_type] => {
            match (
                input_type.data_type_kind(),
                agg_type,
                return_type.data_type_kind(),
            ) {
                (_, AggKind::Count, DataTypeKind::Int64) => {
                    Box::new(StreamingCountAgg::<I64Array>::new())
                }
                (_, AggKind::Sum, DataTypeKind::Int64) => {
                    Box::new(StreamingSumAgg::<I64Array>::new())
                }
                (_, AggKind::Sum, DataTypeKind::Int32) => {
                    Box::new(StreamingSumAgg::<I32Array>::new())
                }
                (_, AggKind::Sum, DataTypeKind::Int16) => {
                    Box::new(StreamingSumAgg::<I16Array>::new())
                }
                (_, AggKind::Sum, DataTypeKind::Float64) => {
                    Box::new(StreamingFloatSumAgg::<F64Array>::new())
                }
                (_, AggKind::Sum, DataTypeKind::Float32) => {
                    Box::new(StreamingFloatSumAgg::<F32Array>::new())
                }
                (_, AggKind::Min, DataTypeKind::Int16) => {
                    Box::new(StreamingMinAgg::<I16Array>::new())
                }
                (_, AggKind::Min, DataTypeKind::Int32) => {
                    Box::new(StreamingMinAgg::<I32Array>::new())
                }
                (_, AggKind::Min, DataTypeKind::Int64) => {
                    Box::new(StreamingMinAgg::<I64Array>::new())
                }
                (_, AggKind::Min, DataTypeKind::Float32) => {
                    Box::new(StreamingFloatMinAgg::<F32Array>::new())
                }
                (_, AggKind::Min, DataTypeKind::Float64) => {
                    Box::new(StreamingFloatMinAgg::<F64Array>::new())
                }
                (_, AggKind::Max, DataTypeKind::Int16) => {
                    Box::new(StreamingMaxAgg::<I16Array>::new())
                }
                (_, AggKind::Max, DataTypeKind::Int32) => {
                    Box::new(StreamingMaxAgg::<I32Array>::new())
                }
                (_, AggKind::Max, DataTypeKind::Int64) => {
                    Box::new(StreamingMaxAgg::<I64Array>::new())
                }
                (_, AggKind::Max, DataTypeKind::Float32) => {
                    Box::new(StreamingFloatMaxAgg::<F32Array>::new())
                }
                (_, AggKind::Max, DataTypeKind::Float64) => {
                    Box::new(StreamingFloatMaxAgg::<F64Array>::new())
                }
                (other_input, other_agg, other_return) => panic!(
                    "streaming state not implemented: {:?} {:?} {:?}",
                    other_input, other_agg, other_return
                ),
            }
        }
        [] => {
            match (agg_type, return_type.data_type_kind()) {
                // `AggKind::Count` for partial/local Count(*) == RowCount while `AggKind::Sum` for final/global Count(*)
                (AggKind::RowCount, DataTypeKind::Int64) => Box::new(StreamingRowCountAgg::new()),
                // According to the function header comments and the link, Count(*) == RowCount
                // `StreamingCountAgg` does not count `NULL`, so we use `StreamingRowCountAgg` here.
                (AggKind::Count, DataTypeKind::Int64) => Box::new(StreamingRowCountAgg::new()),
                _ => unimplemented!(),
            }
        }
        _ => todo!(),
    };
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    use risingwave_common::array::from_builder;

    pub fn get_output_from_state<O, S>(state: &mut S) -> O::ArrayType
    where
        O: ArrayBuilder,
        S: StreamingAggFunction<O>,
    {
        from_builder(|builder| state.get_output_concrete(builder)).unwrap()
    }

    pub fn get_output_from_impl_state(state: &mut impl StreamingAggStateImpl) -> ArrayImpl {
        let mut builder = state.new_builder();
        state.get_output(&mut builder).unwrap();
        builder.finish().unwrap()
    }
}
