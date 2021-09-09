//! Implementation of `StreamingSumAgg`

use std::marker::PhantomData;

use crate::array2::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl, I64Array};
use crate::buffer::Bitmap;
use crate::error::{ErrorCode::NumericValueOutOfRange, Result, RwError};
use crate::types::{option_as_scalar_ref, Scalar, ScalarRef};

use super::{Op, StreamingAggFunction, StreamingAggState, StreamingAggStateImpl};

/// A trait over all sum functions.
///
/// `R`: Result (or output, stored) type.
/// `I`: Input type.
pub trait StreamingSummable<R: Scalar, I: Scalar> {
    fn accumulate(
        result: Option<R::ScalarRefType<'_>>,
        input: Option<I::ScalarRefType<'_>>,
    ) -> Result<Option<R>>;
    fn retract(
        result: Option<R::ScalarRefType<'_>>,
        input: Option<I::ScalarRefType<'_>>,
    ) -> Result<Option<R>>;
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
    result: Option<R::OwnedItem>,
    _phantom: PhantomData<(I, S, R)>,
}

/// `PrimitiveSummable` sums two primitives by `accumulate` and `retract` functions.
/// It produces the same type of output as input `S`.
pub struct PrimitiveSummable<S>
where
    S: Scalar + std::ops::Add<Output = S> + std::ops::Sub<Output = S>,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingSummable<S, S> for PrimitiveSummable<S>
where
    S: Scalar + num_traits::CheckedAdd<Output = S> + num_traits::CheckedSub<Output = S>,
{
    fn accumulate(
        result: Option<S::ScalarRefType<'_>>,
        input: Option<S::ScalarRefType<'_>>,
    ) -> Result<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(
                x.to_owned_scalar()
                    .checked_add(&y.to_owned_scalar())
                    .ok_or_else(|| RwError::from(NumericValueOutOfRange))?,
            ),
            (Some(x), None) => Some(x.to_owned_scalar()),
            (None, Some(y)) => Some(y.to_owned_scalar()),
            (None, None) => None,
        })
    }

    fn retract(
        result: Option<S::ScalarRefType<'_>>,
        input: Option<S::ScalarRefType<'_>>,
    ) -> Result<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(
                x.to_owned_scalar()
                    .checked_sub(&y.to_owned_scalar())
                    .ok_or_else(|| RwError::from(NumericValueOutOfRange))?,
            ),
            (Some(x), None) => Some(x.to_owned_scalar()),
            (None, Some(y)) => Some(y.to_owned_scalar()),
            (None, None) => None,
        })
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
        ops: super::Ops<'_>,
        skip: Option<&Bitmap>,
        data: &I,
    ) -> Result<()> {
        match skip {
            Some(_) => {
                panic!("apply batch with visibility map is not supported yet")
            }
            None => {
                for (op, data) in ops.iter().zip(data.iter()) {
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            self.result = S::accumulate(option_as_scalar_ref(&self.result), data)?
                        }
                        Op::Delete | Op::UpdateDelete => {
                            self.result = S::retract(option_as_scalar_ref(&self.result), data)?
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

impl<R, I, S> StreamingAggFunction<R::Builder> for StreamingSumAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingSummable<R::OwnedItem, I::OwnedItem>,
{
    fn get_output_concrete(&self, builder: &mut R::Builder) -> Result<()> {
        builder.append(option_as_scalar_ref(&self.result))
    }
}

impl<R, I, S> StreamingSumAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingSummable<R::OwnedItem, I::OwnedItem>,
{
    pub fn new() -> Self {
        Self {
            result: None,
            _phantom: PhantomData,
        }
    }
}

macro_rules! impl_agg {
    ($aggregator:tt, $result:tt, $result_variant:tt, $input:tt) => {
        impl<S> StreamingAggStateImpl for $aggregator<$result, $input, S>
        where
            S: StreamingSummable<<$result as Array>::OwnedItem, <$input as Array>::OwnedItem>,
        {
            fn apply_batch(
                &mut self,
                ops: super::Ops<'_>,
                skip: Option<&Bitmap>,
                data: &ArrayImpl,
            ) -> Result<()> {
                self.apply_batch_concrete(ops, skip, data.into())
            }

            fn get_output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                match builder {
                    ArrayBuilderImpl::$result_variant(builder) => self.get_output_concrete(builder),
                    _ => unimplemented!(),
                }
            }

            fn new_builder(&self) -> ArrayBuilderImpl {
                ArrayBuilderImpl::$result_variant(<$result as Array>::Builder::new(0).unwrap())
            }
        }
    };
}

impl_agg! { StreamingSumAgg, I64Array, Int64, I64Array }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::I64Array;
    use crate::array_nonnull;

    #[test]
    fn test_primitive_sum() {
        let mut agg: Box<dyn StreamingAggStateImpl> =
            Box::new(StreamingSumAgg::<I64Array, I64Array, PrimitiveSummable<_>>::new());
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &array_nonnull!(I64Array, [1, 2, 3, 4]).into(),
        )
        .unwrap();
        let mut builder = agg.new_builder();
        agg.get_output(&mut builder).unwrap();
        let array = builder.finish().unwrap();
        assert_eq!(array.as_int64().value_at(0), Some(2));
    }
}
