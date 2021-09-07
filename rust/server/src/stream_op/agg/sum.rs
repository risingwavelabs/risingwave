//! Implementation of `StreamingSumAgg`

use std::marker::PhantomData;

use crate::array2::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, I64ArrayBuilder};
use crate::buffer::Bitmap;
use crate::error::Result;
use crate::types::Scalar;

use super::{StreamingAggFunction, StreamingAggState, StreamingAggStateImpl};

/// A trait over all sum functions.
///
/// `R`: Result (or output, stored) type.
/// `I`: Input type.
pub trait StreamingSummable<R: Scalar, I: Scalar> {
    fn accumulate<'a>(
        result: Option<R::ScalarRefType<'a>>,
        input: Option<I::ScalarRefType<'a>>,
    ) -> Option<R>;
    fn retract<'a>(
        result: Option<R::ScalarRefType<'a>>,
        input: Option<I::ScalarRefType<'a>>,
    ) -> Option<R>;
}

/// `StreamingSumAgg` wraps a streaming summing function `S` into an aggregator.
///
/// `R`: Result (or output, stored) type.
/// `I`: Input type.
/// `S`: Sum function.
pub struct StreamingSumAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingSummable<R::OwnedItem, I::OwnedItem>,
{
    result: R,
    _phantom: PhantomData<(I, S)>,
}

/// `PrimitiveSummable` sums two primitives by `accumulate` and `retract` functions.
/// It produces the same type of output as input `S`.
pub struct PrimitiveSummable<S>
where
    S: Scalar,
    for<'a> S::ScalarRefType<'a>: std::ops::Add<Output = S> + std::ops::Sub<Output = S>,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingSummable<S, S> for PrimitiveSummable<S>
where
    S: Scalar,
    for<'a> S::ScalarRefType<'a>: std::ops::Add<Output = S> + std::ops::Sub<Output = S>,
{
    fn accumulate<'a>(
        result: Option<S::ScalarRefType<'a>>,
        input: Option<S::ScalarRefType<'a>>,
    ) -> Option<S> {
        result.and_then(|x| input.map(|y| x + y))
    }

    fn retract<'a>(
        result: Option<S::ScalarRefType<'a>>,
        input: Option<S::ScalarRefType<'a>>,
    ) -> Option<S> {
        result.and_then(|x| input.map(|y| x - y))
    }
}

impl<R, I, S> StreamingAggState<I> for StreamingSumAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingSummable<R::OwnedItem, I::OwnedItem>,
{
    fn apply_batch_concrete(
        &mut self,
        _ops: super::Ops<'_>,
        _skip: Option<&Bitmap>,
        _data: &I,
    ) -> Result<()> {
        Ok(())
    }
}

impl<R, I, S> StreamingAggFunction<R::Builder> for StreamingSumAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingSummable<R::OwnedItem, I::OwnedItem>,
{
    fn get_output_concrete(&self, _builder: &mut R::Builder) -> Result<()> {
        Ok(())
    }
}

impl<R, I, S> StreamingAggStateImpl for StreamingSumAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingSummable<R::OwnedItem, I::OwnedItem>,
{
    fn apply_batch(
        &mut self,
        _ops: super::Ops<'_>,
        _skip: Option<&Bitmap>,
        _data: &ArrayImpl,
    ) -> Result<()> {
        Ok(())
    }

    fn get_output(&self, _builder: &mut ArrayBuilderImpl) -> Result<()> {
        Ok(())
    }

    fn new_builder(&self) -> ArrayBuilderImpl {
        ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0).unwrap())
    }
}
