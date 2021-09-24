//! Implementation of `StreamingFoldAgg`, which includes sum and count.

use std::marker::PhantomData;

use crate::array2::*;
use crate::buffer::Bitmap;
use crate::error::{ErrorCode::NumericValueOutOfRange, Result, RwError};
use crate::types::{option_as_scalar_ref, Scalar, ScalarRef};

use super::{Op, Ops, StreamingAggFunction, StreamingAggState, StreamingAggStateImpl};

/// A trait over all fold functions.
///
/// `R`: Result (or output, stored) type.
/// `I`: Input type.
pub trait StreamingFoldable<R: Scalar, I: Scalar>: Send + Sync + 'static {
    /// Called on `Insert` or `UpdateInsert`.
    fn accumulate(
        result: Option<R::ScalarRefType<'_>>,
        input: Option<I::ScalarRefType<'_>>,
    ) -> Result<Option<R>>;

    /// Called on `Delete` or `UpdateDelete`.
    fn retract(
        result: Option<R::ScalarRefType<'_>>,
        input: Option<I::ScalarRefType<'_>>,
    ) -> Result<Option<R>>;

    /// Get initial value of this foldable function.
    fn initial() -> Option<R> {
        None
    }
}

/// `StreamingSumAgg` wraps a streaming summing function `S` into an aggregator.
///
/// `R`: Result (or output, stored) type.
/// `I`: Input type.
/// `S`: Sum function.
pub struct StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    result: Option<R::OwnedItem>,
    _phantom: PhantomData<(I, S, R)>,
}

/// `PrimitiveSummable` sums two primitives by `accumulate` and `retract` functions.
/// It produces the same type of output as input `S`.
pub struct PrimitiveSummable<S>
where
    S: Scalar
        + num_traits::CheckedAdd<Output = S>
        + num_traits::CheckedSub<Output = S>
        + std::ops::Neg<Output = S>,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<S, S> for PrimitiveSummable<S>
where
    S: Scalar
        + num_traits::CheckedAdd<Output = S>
        + num_traits::CheckedSub<Output = S>
        + std::ops::Neg<Output = S>,
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
            (None, Some(y)) => Some(-y.to_owned_scalar()),
            (None, None) => None,
        })
    }
}

/// `FloatPrimitiveSummable` sums two primitives by `accumulate` and `retract` functions.
/// It produces the same type of output as input `S`.
pub struct FloatPrimitiveSummable<S>
where
    S: Scalar
        + std::ops::Add<Output = S>
        + std::ops::Sub<Output = S>
        + num_traits::Float
        + std::ops::Neg<Output = S>,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<S, S> for FloatPrimitiveSummable<S>
where
    S: Scalar
        + std::ops::Add<Output = S>
        + std::ops::Sub<Output = S>
        + num_traits::Float
        + std::ops::Neg<Output = S>,
{
    fn accumulate(
        result: Option<S::ScalarRefType<'_>>,
        input: Option<S::ScalarRefType<'_>>,
    ) -> Result<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => {
                let v = x.to_owned_scalar() + y.to_owned_scalar();
                if v.is_finite() && !v.is_nan() {
                    Some(v)
                } else {
                    return Err(RwError::from(NumericValueOutOfRange));
                }
            }
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
            (Some(x), Some(y)) => {
                let v = x.to_owned_scalar() - y.to_owned_scalar();
                if v.is_finite() && !v.is_nan() {
                    Some(v)
                } else {
                    return Err(RwError::from(NumericValueOutOfRange));
                }
            }
            (Some(x), None) => Some(x.to_owned_scalar()),
            (None, Some(y)) => Some(-y.to_owned_scalar()),
            (None, None) => None,
        })
    }
}

/// `Countable` do counts. The behavior of `Countable` is somehow counterintuitive.
/// In SQL logic, if there is no item in aggregation, count will return `null`.
/// However, this `Countable` will always return 0 if there is no item.
pub struct Countable<S>
where
    S: Scalar,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<i64, S> for Countable<S>
where
    S: Scalar,
{
    fn accumulate(result: Option<i64>, input: Option<S::ScalarRefType<'_>>) -> Result<Option<i64>> {
        Ok(match (result, input) {
            (Some(x), Some(_)) => Some(x + 1),
            (Some(x), None) => Some(x),
            // this is not possible, as initial value of countable is `0`.
            _ => unreachable!(),
        })
    }

    fn retract(result: Option<i64>, input: Option<S::ScalarRefType<'_>>) -> Result<Option<i64>> {
        Ok(match (result, input) {
            (Some(x), Some(_)) => Some(x - 1),
            (Some(x), None) => Some(x),
            // this is not possible, as initial value of countable is `0`.
            _ => unreachable!(),
        })
    }

    fn initial() -> Option<i64> {
        Some(0)
    }
}

impl<R, I, S> StreamingAggState<I> for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        skip: Option<&Bitmap>,
        data: &I,
    ) -> Result<()> {
        for (row_idx, (op, data)) in ops.iter().zip(data.iter()).enumerate() {
            if skip == None || skip.unwrap().is_set(row_idx).unwrap() {
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
        Ok(())
    }
}

impl<R, I, S> StreamingAggFunction<R::Builder> for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn get_output_concrete(&self, builder: &mut R::Builder) -> Result<()> {
        builder.append(option_as_scalar_ref(&self.result))
    }
}

impl<R, I, S> StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    pub fn new() -> Self {
        Self {
            result: S::initial(),
            _phantom: PhantomData,
        }
    }
}

macro_rules! impl_agg {
  ($result:tt, $result_variant:tt, $input:tt) => {
    impl<S> StreamingAggStateImpl for StreamingFoldAgg<$result, $input, S>
    where
      S: StreamingFoldable<<$result as Array>::OwnedItem, <$input as Array>::OwnedItem>,
    {
      fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        skip: Option<&Bitmap>,
        data: &ArrayImpl,
      ) -> Result<()> {
        self.apply_batch_concrete(ops, skip, data.into())
      }

      fn get_output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        match builder {
          ArrayBuilderImpl::$result_variant(builder) => self.get_output_concrete(builder),
          other_variant => panic!(
            "type mismatch in streaming aggregator StreamingFoldAgg output: expected {}, get {}",
            stringify!($result),
            other_variant.get_ident()
          ),
        }
      }

      fn new_builder(&self) -> ArrayBuilderImpl {
        ArrayBuilderImpl::$result_variant(<$result as Array>::Builder::new(0).unwrap())
      }
    }
  };
}

impl_agg! { I16Array, Int16, I16Array }
impl_agg! { I32Array, Int32, I32Array }
impl_agg! { I64Array, Int64, I64Array }
impl_agg! { F32Array, Float32, F32Array }
impl_agg! { F64Array, Float64, F64Array }
impl_agg! { I64Array, Int64, F64Array }
impl_agg! { I64Array, Int64, F32Array }
impl_agg! { I64Array, Int64, I32Array }
impl_agg! { I64Array, Int64, I16Array }
impl_agg! { I64Array, Int64, BoolArray }
impl_agg! { I64Array, Int64, UTF8Array }

#[cfg(test)]
mod tests {
    use super::super::tests::get_output_from_state;
    use super::*;
    use crate::array2::I64Array;
    use crate::{array, array_nonnull};

    type TestStreamingSumAgg<R> =
        StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem>>;

    type TestStreamingCountAgg<R> = StreamingFoldAgg<R, R, Countable<<R as Array>::OwnedItem>>;

    #[test]
    /// This test uses `Box<dyn StreamingAggStateImpl>` to test a state.
    fn test_primitive_sum_boxed() {
        let mut agg: Box<dyn StreamingAggStateImpl> =
            Box::new(TestStreamingSumAgg::<I64Array>::new());
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &array_nonnull!(I64Array, [1, 2, 3, 3]).into(),
        )
        .unwrap();
        let mut builder = agg.new_builder();
        agg.get_output(&mut builder).unwrap();
        let array = builder.finish().unwrap();
        assert_eq!(array.as_int64().value_at(0), Some(3));

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Delete, Op::Insert],
            Some(&Bitmap::from_vec(vec![true, true, false, false]).unwrap()),
            &array_nonnull!(I64Array, [3, 1, 3, 1]).into(),
        )
        .unwrap();
        let mut builder = agg.new_builder();
        agg.get_output(&mut builder).unwrap();
        let array = builder.finish().unwrap();
        assert_eq!(array.as_int64().value_at(0), Some(5));
    }

    #[test]
    fn test_primitive_sum() {
        let mut agg = TestStreamingSumAgg::<I64Array>::new();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &array_nonnull!(I64Array, [1, 2, 3, 3]).into(),
        )
        .unwrap();
        let array = get_output_from_state(&mut agg);
        assert_eq!(array.value_at(0), Some(3));

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Delete, Op::Insert],
            Some(&Bitmap::from_vec(vec![true, true, false, false]).unwrap()),
            &array_nonnull!(I64Array, [3, 1, 3, 1]).into(),
        )
        .unwrap();
        let mut builder = agg.new_builder();
        agg.get_output(&mut builder).unwrap();
        let array = builder.finish().unwrap();
        assert_eq!(array.as_int64().value_at(0), Some(5));
    }

    #[test]
    fn test_primitive_sum_first_deletion() {
        let mut agg = TestStreamingSumAgg::<I64Array>::new();
        agg.apply_batch(
            &[Op::Delete, Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &array_nonnull!(I64Array, [10, 1, 2, 3, 3]).into(),
        )
        .unwrap();
        let array = get_output_from_state(&mut agg);
        assert_eq!(array.value_at(0), Some(-7));

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&Bitmap::from_vec(vec![false, true, false, false]).unwrap()),
            &array_nonnull!(I64Array, [3, 1, 3, 1]).into(),
        )
        .unwrap();
        let mut builder = agg.new_builder();
        agg.get_output(&mut builder).unwrap();
        let array = builder.finish().unwrap();
        assert_eq!(array.as_int64().value_at(0), Some(-8));
    }

    #[test]
    /// Even if there is no element after some insertions and equal number of deletion opertaions,
    /// `PrimitiveSummable` should output `0` instead of `None`.
    fn test_primitive_sum_no_none() {
        let mut agg = TestStreamingSumAgg::<I64Array>::new();

        // When no operation has been applied, output should be `None`.
        let array = get_output_from_state(&mut agg);
        assert_eq!(array.value_at(0), None);

        agg.apply_batch(
            &[Op::Delete, Op::Insert, Op::Insert, Op::Delete],
            None,
            &array_nonnull!(I64Array, [1, 2, 1, 2]).into(),
        )
        .unwrap();
        let array = get_output_from_state(&mut agg);
        assert_eq!(array.value_at(0), Some(0));

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Insert],
            Some(&Bitmap::from_vec(vec![false, true, false, true]).unwrap()),
            &array_nonnull!(I64Array, [3, 1, 3, 1]).into(),
        )
        .unwrap();
        let mut builder = agg.new_builder();
        agg.get_output(&mut builder).unwrap();
        let array = builder.finish().unwrap();
        assert_eq!(array.as_int64().value_at(0), Some(0));
    }

    #[test]
    fn test_primitive_count() {
        let mut agg = TestStreamingCountAgg::<I64Array>::new();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &array!(I64Array, [Some(1), None, Some(3), Some(1)]).into(),
        )
        .unwrap();

        let array = get_output_from_state(&mut agg);
        assert_eq!(array.value_at(0), Some(1));

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&Bitmap::from_vec(vec![false, true, false, false]).unwrap()),
            &array!(I64Array, [Some(1), None, Some(3), Some(1)]).into(),
        )
        .unwrap();
        let mut builder = agg.new_builder();
        agg.get_output(&mut builder).unwrap();
        let array = builder.finish().unwrap();
        assert_eq!(array.as_int64().value_at(0), Some(1));
    }
}
