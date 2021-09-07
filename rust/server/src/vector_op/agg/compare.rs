use std::marker::PhantomData;

use super::{AggFunctionConcrete, AggStateConcrete, Aggregator};
use crate::array2::*;
use crate::error::Result;
use crate::types::*;

pub trait Comparer<T: Scalar> {
    fn replace_value<'a>(result: &'a Option<T>, input: Option<T::ScalarRefType<'a>>) -> bool;
}

pub struct SimpleCompareMin<T>
where
    T: Scalar + ScalarPartialOrd,
{
    _phantom: PhantomData<T>,
}

impl<T> Comparer<T> for SimpleCompareMin<T>
where
    T: Scalar + ScalarPartialOrd,
{
    fn replace_value<'a>(result: &'a Option<T>, input: Option<T::ScalarRefType<'a>>) -> bool {
        match (result, input) {
            (Some(_), None) => false,
            (None, Some(_)) => true,
            (Some(result), Some(input)) => {
                matches!(result.scalar_cmp(input), Some(std::cmp::Ordering::Greater))
            }
            _ => false,
        }
    }
}

/// `AggCompare` finds exterme in all input data.
pub struct AggCompare<A, C>
where
    A: Array,
    C: Comparer<A::OwnedItem>,
{
    result: Option<A::OwnedItem>,
    _phantom: PhantomData<C>,
}

impl<A, C> AggStateConcrete<A> for AggCompare<A, C>
where
    A: Array,
    C: Comparer<A::OwnedItem>,
{
    fn update_concrete(&mut self, input: &A) -> Result<()> {
        for idx in 0..input.len() {
            let item = input.value_at(idx);
            if C::replace_value(&self.result, item) {
                self.result = item.map(|x| x.to_owned_scalar());
            }
        }
        Ok(())
    }
}

impl<A, C> AggFunctionConcrete<A::Builder> for AggCompare<A, C>
where
    A: Array,
    C: Comparer<A::OwnedItem>,
{
    fn output_concrete(&self, builder: &mut A::Builder) -> Result<()> {
        builder.append(self.result.as_ref().map(|x| x.as_scalar_ref()))?;
        Ok(())
    }
}

impl<A, C> AggCompare<A, C>
where
    A: Array,
    C: Comparer<A::OwnedItem>,
{
    pub fn new() -> Self {
        Self {
            result: None,
            _phantom: PhantomData,
        }
    }
}

macro_rules! impl_compare_min {
    ($input:ty, $input_variant:ident) => {
        impl Aggregator for AggCompare<$input, SimpleCompareMin<<$input as Array>::OwnedItem>> {
            fn update(&mut self, input: &ArrayImpl) -> Result<()> {
                match input {
                    ArrayImpl::$input_variant(i) => self.update_concrete(i),
                    _ => panic!("type mismatch"),
                }
            }

            fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                match builder {
                    ArrayBuilderImpl::$input_variant(b) => self.output_concrete(b),
                    _ => panic!("type mismatch"),
                }
            }
        }
    };
}

impl_compare_min! { I16Array, Int16 }
impl_compare_min! { I32Array, Int32 }
impl_compare_min! { I64Array, Int64 }
impl_compare_min! { F32Array, Float32 }
impl_compare_min! { F64Array, Float64 }
impl_compare_min! { UTF8Array, UTF8 }
