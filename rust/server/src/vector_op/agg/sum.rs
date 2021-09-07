use std::marker::PhantomData;

use super::{AggFunctionConcrete, AggStateConcrete, Aggregator};
use crate::array2::*;
use crate::error::Result;
use crate::types::*;

pub trait Summable<R: Scalar, I: Scalar> {
    fn sum(result: &Option<R>, input: Option<I::ScalarRefType<'_>>) -> Option<R>;
}

/// `AggSum` sums all input data.
pub struct AggSum<Output, Input, S>
where
    Output: Array,
    Input: Array,
    S: Summable<Output::OwnedItem, Input::OwnedItem>,
{
    result: Option<Output::OwnedItem>,
    _phantom: PhantomData<(Input, S)>,
}

impl<Output, Input, S> AggStateConcrete<Input> for AggSum<Output, Input, S>
where
    Output: Array,
    Input: Array,
    S: Summable<Output::OwnedItem, Input::OwnedItem>,
{
    fn update_concrete(&mut self, input: &Input) -> Result<()> {
        for item in input.iter() {
            self.result = S::sum(&self.result, item);
        }
        Ok(())
    }
}

impl<Output, Input, S> AggFunctionConcrete<Output::Builder> for AggSum<Output, Input, S>
where
    Output: Array,
    Input: Array,
    S: Summable<Output::OwnedItem, Input::OwnedItem>,
{
    fn output_concrete(&self, builder: &mut Output::Builder) -> Result<()> {
        builder.append(self.result.as_ref().map(|x| x.as_scalar_ref()))?;
        Ok(())
    }
}

impl<Output, Input, S> AggSum<Output, Input, S>
where
    Output: Array,
    Input: Array,
    S: Summable<Output::OwnedItem, Input::OwnedItem>,
{
    pub fn new() -> Self {
        Self {
            result: None,
            _phantom: PhantomData,
        }
    }
}

use num_traits::AsPrimitive;

pub struct PrimitiveSum<R, I>
where
    R: std::ops::Add<Output = R> + NativeType + Scalar,
    I: AsPrimitive<R> + NativeType,
{
    _phantom: PhantomData<(R, I)>,
}

impl<R, I> Summable<R, I> for PrimitiveSum<R, I>
where
    R: std::ops::Add<Output = R> + NativeType + Scalar,
    I: AsPrimitive<R> + NativeType + Scalar,
{
    fn sum(result: &Option<R>, input: Option<I::ScalarRefType<'_>>) -> Option<R> {
        match (result, input) {
            (Some(a), Some(b)) => Some(a.add(b.to_owned_scalar().as_())),
            (Some(a), None) => Some(*a),
            (None, Some(b)) => Some(b.to_owned_scalar().as_()),
            (None, None) => None,
        }
    }
}

macro_rules! impl_sum {
    ($result:ty, $result_variant:ident, $input:ty, $input_variant:ident) => {
        impl Aggregator
            for AggSum<
                $result,
                $input,
                PrimitiveSum<<$result as Array>::OwnedItem, <$input as Array>::OwnedItem>,
            >
        {
            fn update(&mut self, input: &ArrayImpl) -> Result<()> {
                match input {
                    ArrayImpl::$input_variant(i) => self.update_concrete(i),
                    _ => panic!("type mismatch"),
                }
            }

            fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                match builder {
                    ArrayBuilderImpl::$result_variant(b) => self.output_concrete(b),
                    _ => panic!("type mismatch"),
                }
            }
        }
    };
}

impl_sum! { I64Array, Int64, I16Array, Int16 }
impl_sum! { I64Array, Int64, I32Array, Int32 }
impl_sum! { F32Array, Float32, F32Array, Float32 }
impl_sum! { F64Array, Float64, F64Array, Float64 }
