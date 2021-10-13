//! Implementation of `StreamingAvgAgg`. This aggregation state is only
//! used in `GlobalAggregationOperator`. For avg operation, the local
//! operator should always be `StreamingSumAgg`.

use std::marker::PhantomData;
use std::ops::Neg;

use crate::array::Array;
use crate::types::option_as_scalar_ref;

use num_traits::{CheckedAdd, CheckedSub};

use crate::array::*;
use crate::buffer::Bitmap;
use crate::stream_op::*;

use super::{StreamingAggFunction, StreamingAggState};

/// `StreamingAvgAgg` calculates average number of `A`. The result type is `O`.
///  We compose two foldable states, the `StreamingSumAgg` and the `StreamingCountAgg`.
///  The average can be derived from these two foldable states.
pub struct StreamingAvgAgg<A, O = A>
where
    A: Array,
    O: Array,
    A::OwnedItem: CheckedAdd<Output = A::OwnedItem>
        + CheckedSub<Output = A::OwnedItem>
        + Neg<Output = A::OwnedItem>
        + CastDiv<i64, O::OwnedItem>,
{
    sum_state: StreamingSumAgg<A>,
    count_state: StreamingCountAgg<A>,
    _phantom: PhantomData<O>,
}

impl<A, O> StreamingAvgAgg<A, O>
where
    A: Array,
    O: Array,
    A::OwnedItem: CheckedAdd<Output = A::OwnedItem>
        + CheckedSub<Output = A::OwnedItem>
        + Neg<Output = A::OwnedItem>
        + CastDiv<i64, O::OwnedItem>,
{
    fn new() -> Self {
        StreamingAvgAgg {
            sum_state: StreamingSumAgg::new(),
            count_state: StreamingCountAgg::new(),
            _phantom: PhantomData,
        }
    }
}

impl<A, O> StreamingAggState<A> for StreamingAvgAgg<A, O>
where
    A: Array,
    O: Array,
    A::OwnedItem: CheckedAdd<Output = A::OwnedItem>
        + CheckedSub<Output = A::OwnedItem>
        + Neg<Output = A::OwnedItem>
        + CastDiv<i64, O::OwnedItem>,
{
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &A,
    ) -> crate::error::Result<()> {
        self.sum_state.apply_batch_concrete(ops, visibility, data)?;
        self.count_state
            .apply_batch_concrete(ops, visibility, data)?;
        Ok(())
    }
}

impl<A, O> StreamingAggFunction<O::Builder> for StreamingAvgAgg<A, O>
where
    A: Array,
    O: Array,
    A::OwnedItem: CheckedAdd<Output = A::OwnedItem>
        + CheckedSub<Output = A::OwnedItem>
        + Neg<Output = A::OwnedItem>
        + CastDiv<i64, O::OwnedItem>,
{
    fn get_output_concrete(&self, builder: &mut O::Builder) -> Result<()> {
        let count = match self.count_state.get_state() {
            Some(x) => *x,
            None => {
                builder.append(None)?;
                return Ok(());
            }
        };
        let sum = match self.sum_state.get_state().as_ref() {
            Some(t) => t.clone(),
            None => {
                builder.append(None)?;
                return Ok(());
            }
        };

        builder.append(option_as_scalar_ref(&Some(sum.safe_div(count))))?;
        Ok(())
    }
}
// CastDiv trait enables division between primitive types.
// We need this abstract because explicit conversion between some primitive types does not exist. (such as f64::From<i64>)
// furthermore, extra check logic may be added into the division process in the feature.
pub trait CastDiv<Rhs, Output> {
    fn safe_div(self, r: Rhs) -> Output;
}

macro_rules! impl_cast_div {
    ($lhs:ty,$rhs:ty,$out:ty) => {
        impl CastDiv<$rhs, $out> for $lhs {
            fn safe_div(self, r: $rhs) -> $out {
                (self as $out) / (r as $out)
            }
        }
    };
}

impl_cast_div!(f64, i64, f64);
impl_cast_div!(i64, i64, f64);
impl_cast_div!(i32, i64, f64);
impl_cast_div!(i16, i64, f64);
impl_cast_div!(i64, i64, i64);

#[cfg(test)]
mod tests {

    use super::StreamingAvgAgg;
    use crate::array::ArrayBuilder;
    use crate::{
        array,
        array::{Array, F64Array, I64Array},
        array_nonnull,
        buffer::Bitmap,
        stream_op::{
            aggregation::{tests::get_output_from_state, StreamingAggState},
            Op,
        },
    };

    #[test]
    fn test_average() {
        let mut avg_agg = StreamingAvgAgg::<I64Array, F64Array>::new();
        avg_agg
            .apply_batch_concrete(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &array_nonnull!(I64Array, [10, 1, 2, 3, 3]),
            )
            .unwrap();
        let array = get_output_from_state(&mut avg_agg);
        assert_eq!(array.value_at(0), Some(3.8_f64));

        let mut avg_agg = StreamingAvgAgg::<I64Array, I64Array>::new();
        avg_agg
            .apply_batch_concrete(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &array_nonnull!(I64Array, [10, 1, 2, 3, 3]),
            )
            .unwrap();
        let array = get_output_from_state(&mut avg_agg);
        assert_eq!(array.value_at(0), Some(3));

        let mut avg_agg = StreamingAvgAgg::<I64Array, I64Array>::new();
        avg_agg
            .apply_batch_concrete(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert, Op::Delete],
                None,
                &array_nonnull!(I64Array, [10, 1, 2, 3, 3]),
            )
            .unwrap();
        let array = get_output_from_state(&mut avg_agg);
        assert_eq!(array.value_at(0), Some(4));
    }

    #[test]
    fn test_average_with_none() {
        let mut avg_agg = StreamingAvgAgg::<I64Array, F64Array>::new();
        avg_agg
            .apply_batch_concrete(
                &[Op::Insert, Op::Insert, Op::Insert, Op::Insert, Op::Insert],
                None,
                &array_nonnull!(I64Array, [10, 1, 2, 3, 3]),
            )
            .unwrap();

        avg_agg
            .apply_batch_concrete(
                &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
                Some(&Bitmap::from_vec(vec![false, true, false, true]).unwrap()),
                &array!(I64Array, [Some(1), None, Some(3), Some(1)]),
            )
            .unwrap();
        let array = get_output_from_state(&mut avg_agg);

        //sum=10+1+2+3+3-1=18 count=5-1=4
        assert_eq!(array.value_at(0), Some(4.5_f64));
    }

    #[test]
    fn test_average_with_empty() {
        let mut avg_agg = StreamingAvgAgg::<I64Array, F64Array>::new();

        let builder = <I64Array as Array>::Builder::new(0).unwrap();

        avg_agg
            .apply_batch_concrete(&[], None, &builder.finish().unwrap())
            .unwrap();

        let array = get_output_from_state(&mut avg_agg);

        assert_eq!(array.value_at(0), None);
    }
}
