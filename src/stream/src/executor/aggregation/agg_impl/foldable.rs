// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Implementation of `StreamingFoldAgg`, which includes sum and count.

use std::marker::PhantomData;

use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::*;
use risingwave_common::bail;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::{Datum, Scalar, ScalarRef};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::ExprError;

use super::{StreamingAggImpl, StreamingAggInput, StreamingAggOutput};
use crate::executor::error::StreamExecutorResult;

/// A trait over all fold functions.
///
/// `R`: Result (or output, stored) type.
/// `I`: Input type.
pub trait StreamingFoldable<R: Scalar, I: Scalar>: std::fmt::Debug + Send + Sync + 'static {
    /// Called on `Insert` or `UpdateInsert`.
    fn accumulate(
        result: Option<&R>,
        input: Option<I::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<R>>;

    /// Called on `Delete` or `UpdateDelete`.
    fn retract(
        result: Option<&R>,
        input: Option<I::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<R>>;

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
#[derive(Debug)]
pub struct StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    result: Option<R::OwnedItem>,
    _phantom: PhantomData<(I, S, R)>,
}

impl<R, I, S> Clone for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn clone(&self) -> Self {
        Self {
            result: self.result.clone(),
            _phantom: PhantomData,
        }
    }
}

/// `PrimitiveSummable` sums two primitives by `accumulate` and `retract` functions.
/// It produces the same type of output as input `S`.
#[derive(Debug)]
pub struct PrimitiveSummable<S, I>
where
    I: Scalar + Into<S> + std::ops::Neg<Output = I>,
    S: Scalar + num_traits::CheckedAdd<Output = S> + num_traits::CheckedSub<Output = S>,
{
    _phantom: PhantomData<(S, I)>,
}

impl<S, I> StreamingFoldable<S, I> for PrimitiveSummable<S, I>
where
    I: Scalar + Into<S> + std::ops::Neg<Output = I>,
    S: Scalar + num_traits::CheckedAdd<Output = S> + num_traits::CheckedSub<Output = S>,
{
    fn accumulate(
        result: Option<&S>,
        input: Option<I::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(
                x.checked_add(&(y.to_owned_scalar()).into())
                    .ok_or(ExprError::NumericOutOfRange)?,
            ),
            (Some(x), None) => Some(x.clone()),
            (None, Some(y)) => Some((y.to_owned_scalar()).into()),
            (None, None) => None,
        })
    }

    fn retract(
        result: Option<&S>,
        input: Option<I::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(
                x.checked_sub(&(y.to_owned_scalar()).into())
                    .ok_or(ExprError::NumericOutOfRange)?,
            ),
            (Some(x), None) => Some(x.clone()),
            (None, Some(y)) => Some((-y.to_owned_scalar()).into()),
            (None, None) => None,
        })
    }
}

/// `I64Sum0` sums two i64 by `accumulate` and `retract` functions.
/// It is initialized with 0.
#[derive(Debug)]
pub struct I64Sum0 {}

impl StreamingFoldable<i64, i64> for I64Sum0 {
    fn accumulate(
        result: Option<&i64>,
        input: Option<<i64 as Scalar>::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<i64>> {
        PrimitiveSummable::<i64, i64>::accumulate(result, input)
    }

    fn retract(
        result: Option<&i64>,
        input: Option<<i64 as Scalar>::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<i64>> {
        PrimitiveSummable::<i64, i64>::retract(result, input)
    }

    fn initial() -> Option<i64> {
        Some(0)
    }
}

/// `Countable` do counts. The behavior of `Countable` is somehow counterintuitive.
/// In SQL logic, if there is no item in aggregation, count will return `null`.
/// However, this `Countable` will always return 0 if there is no item.
#[derive(Debug)]
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
    fn accumulate(
        result: Option<&i64>,
        input: Option<S::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<i64>> {
        Ok(match (result, input) {
            (Some(x), Some(_)) => Some(x + 1),
            (Some(x), None) => Some(*x),
            _ => unreachable!("count initial value is 0"),
        })
    }

    fn retract(
        result: Option<&i64>,
        input: Option<S::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<i64>> {
        Ok(match (result, input) {
            (Some(x), Some(_)) => Some(x - 1),
            (Some(x), None) => Some(*x),
            _ => unreachable!("count initial value is 0"),
        })
    }

    fn initial() -> Option<i64> {
        Some(0)
    }
}

/// `Minimizable` return minimum value overall.
/// It produces the same type of output as input `S`.
#[derive(Debug)]
pub struct Minimizable<S>
where
    S: Scalar + Ord,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<S, S> for Minimizable<S>
where
    S: Scalar + Ord,
{
    fn accumulate(
        result: Option<&S>,
        input: Option<S::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(x.clone().min(y.to_owned_scalar())),
            (None, Some(y)) => Some(y.to_owned_scalar()),
            (Some(x), None) => Some(x.clone()),
            (None, None) => None,
        })
    }

    fn retract(
        _result: Option<&S>,
        _input: Option<S::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<S>> {
        bail!("insert only for minimum")
    }
}

/// `Maximizable` return maximum value overall.
/// It produces the same type of output as input `S`.
#[derive(Debug)]
pub struct Maximizable<S>
where
    S: Scalar + Ord,
{
    _phantom: PhantomData<S>,
}

impl<S> StreamingFoldable<S, S> for Maximizable<S>
where
    S: Scalar + Ord,
{
    fn accumulate(
        result: Option<&S>,
        input: Option<S::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<S>> {
        Ok(match (result, input) {
            (Some(x), Some(y)) => Some(x.clone().max(y.to_owned_scalar())),
            (None, Some(y)) => Some(y.to_owned_scalar()),
            (Some(x), None) => Some(x.clone()),
            (None, None) => None,
        })
    }

    fn retract(
        _result: Option<&S>,
        _input: Option<S::ScalarRefType<'_>>,
    ) -> StreamExecutorResult<Option<S>> {
        bail!("insert only for maximum")
    }
}

impl<R, I, S> StreamingAggInput<I> for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &I,
    ) -> StreamExecutorResult<()> {
        match visibility {
            None => {
                for (op, data) in ops.iter().zip_eq_fast(data.iter()) {
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            self.result = S::accumulate(self.result.as_ref(), data)?
                        }
                        Op::Delete | Op::UpdateDelete => {
                            self.result = S::retract(self.result.as_ref(), data)?
                        }
                    }
                }
            }
            Some(visibility) => {
                for idx in visibility.iter_ones() {
                    // SAFETY(value_at_unchecked): the idx is always in bound.
                    unsafe {
                        match ops[idx] {
                            Op::Insert | Op::UpdateInsert => {
                                self.result = S::accumulate(
                                    self.result.as_ref(),
                                    data.value_at_unchecked(idx),
                                )?
                            }
                            Op::Delete | Op::UpdateDelete => {
                                self.result =
                                    S::retract(self.result.as_ref(), data.value_at_unchecked(idx))?
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl<R, I, S> StreamingAggOutput<R::Builder> for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn get_output_concrete(&self) -> StreamExecutorResult<Option<R::OwnedItem>> {
        Ok(self.result.clone())
    }
}

impl<R, I, S> Default for StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    fn default() -> Self {
        Self {
            result: S::initial(),
            _phantom: PhantomData,
        }
    }
}

impl<R, I, S> StreamingFoldAgg<R, I, S>
where
    R: Array,
    I: Array,
    S: StreamingFoldable<R::OwnedItem, I::OwnedItem>,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_datum(x: Datum) -> StreamExecutorResult<Self> {
        let mut result = None;
        if let Some(scalar) = x {
            result = Some(R::OwnedItem::try_from(scalar)?);
        }

        Ok(Self {
            result,
            _phantom: PhantomData,
        })
    }

    /// Get current state without using an array builder
    pub fn get_state(&self) -> &Option<R::OwnedItem> {
        &self.result
    }
}

macro_rules! impl_fold_agg {
    ($result:ty, $result_variant:ident, $input:ty) => {
        impl<S> StreamingAggImpl for StreamingFoldAgg<$result, $input, S>
        where
            S: StreamingFoldable<<$result as Array>::OwnedItem, <$input as Array>::OwnedItem>,
        {
            fn apply_batch(
                &mut self,
                ops: Ops<'_>,
                visibility: Option<&Bitmap>,
                data: &[&ArrayImpl],
            ) -> StreamExecutorResult<()> {
                self.apply_batch_concrete(ops, visibility, data[0].into())
            }

            fn get_output(&self) -> StreamExecutorResult<Datum> {
                Ok(self.result.clone().map(Scalar::to_scalar_value))
            }

            fn new_builder(&self) -> ArrayBuilderImpl {
                ArrayBuilderImpl::$result_variant(<$result as Array>::Builder::new(0))
            }

            fn reset(&mut self) {
                self.result = S::initial();
            }
        }
    };
}

// Implement all supported combination of input and output for `StreamingFoldAgg`.
// count
impl_fold_agg! { I64Array, Int64, I64Array } // max/min
impl_fold_agg! { I64Array, Int64, F64Array }
impl_fold_agg! { I64Array, Int64, F32Array }
impl_fold_agg! { I64Array, Int64, I32Array } // sum
impl_fold_agg! { I64Array, Int64, I16Array } // sum
impl_fold_agg! { I64Array, Int64, BoolArray }
impl_fold_agg! { I64Array, Int64, Utf8Array }
impl_fold_agg! { I64Array, Int64, DecimalArray }
impl_fold_agg! { I64Array, Int64, StructArray }
impl_fold_agg! { I64Array, Int64, ListArray }
impl_fold_agg! { I64Array, Int64, IntervalArray }
impl_fold_agg! { I64Array, Int64, TimeArray }
impl_fold_agg! { I64Array, Int64, DateArray }
impl_fold_agg! { I64Array, Int64, TimestampArray }
// max/min
impl_fold_agg! { I16Array, Int16, I16Array }
impl_fold_agg! { I32Array, Int32, I32Array }
impl_fold_agg! { F32Array, Float32, F32Array }
impl_fold_agg! { F64Array, Float64, F64Array }
impl_fold_agg! { DecimalArray, Decimal, DecimalArray }
impl_fold_agg! { Utf8Array, Utf8, Utf8Array }
impl_fold_agg! { BytesArray, Bytea, BytesArray }
impl_fold_agg! { StructArray, Struct, StructArray }
impl_fold_agg! { IntervalArray, Interval, IntervalArray }
impl_fold_agg! { TimeArray, Time, TimeArray }
impl_fold_agg! { DateArray, Date, DateArray }
impl_fold_agg! { TimestampArray, Timestamp, TimestampArray }
// sum
impl_fold_agg! { DecimalArray, Decimal, I64Array }
// avg
impl_fold_agg! { F64Array, Float64, F32Array }

#[cfg(test)]
mod tests {
    extern crate test;

    use risingwave_common::array::stream_chunk::Op;
    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::test_utils::{rand_bitmap, rand_stream_chunk};
    use risingwave_common::types::F64;
    use risingwave_common::{array, array_nonnull};
    use test::Bencher;

    use super::*;

    type TestStreamingSumAgg<R> =
        StreamingFoldAgg<R, R, PrimitiveSummable<<R as Array>::OwnedItem, <R as Array>::OwnedItem>>;

    type TestStreamingCountAgg<R> = StreamingFoldAgg<R, R, Countable<<R as Array>::OwnedItem>>;

    type TestStreamingMinAgg<R> = StreamingFoldAgg<R, R, Minimizable<<R as Array>::OwnedItem>>;

    type TestStreamingMaxAgg<R> = StreamingFoldAgg<R, R, Maximizable<<R as Array>::OwnedItem>>;

    #[test]
    /// This test uses `Box<dyn StreamingAggImpl>` to test an aggregator.
    fn test_primitive_sum_boxed() {
        let mut agg: Box<dyn StreamingAggImpl> = Box::<TestStreamingSumAgg<I64Array>>::default();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 3, 3]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &3);

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Delete, Op::Insert],
            Some(&(vec![true, true, false, false]).into_iter().collect()),
            &[&array_nonnull!(I64Array, [3, 1, 3, 1]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &5);
    }

    #[test]
    fn test_primitive_sum_i64() {
        let mut agg = TestStreamingSumAgg::<I64Array>::default();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 3, 3]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &3);

        agg.apply_batch(
            &[Op::Insert, Op::Delete, Op::Delete, Op::Insert],
            Some(&(vec![true, true, false, false]).into_iter().collect()),
            &[&array_nonnull!(I64Array, [3, 1, 3, 1]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &5);
    }

    #[test]
    fn test_primitive_sum_f64() {
        let testcases = [
            (vec![('+', 1.0), ('+', 2.0), ('+', 3.0), ('-', 4.0)], 2.0),
            (
                vec![('+', 1.0), ('+', f64::INFINITY), ('+', 3.0), ('-', 3.0)],
                f64::INFINITY,
            ),
            (vec![('+', 0.0), ('-', f64::NEG_INFINITY)], f64::INFINITY),
            (vec![('+', 1.0), ('+', f64::NAN), ('+', 1926.0)], f64::NAN),
        ];

        for (input, expected) in testcases {
            let (ops, data): (Vec<_>, Vec<_>) = input
                .into_iter()
                .map(|(c, v)| {
                    (
                        if c == '+' { Op::Insert } else { Op::Delete },
                        Some(F64::from(v)),
                    )
                })
                .unzip();
            let mut agg = TestStreamingSumAgg::<F64Array>::default();
            agg.apply_batch(
                &ops,
                None,
                &[&ArrayImpl::Float64(F64Array::from_iter(&data))],
            )
            .unwrap();
            assert_eq!(
                agg.get_output().unwrap().unwrap().as_float64(),
                &F64::from(expected)
            );
        }
    }

    #[test]
    fn test_primitive_sum_first_deletion() {
        let mut agg = TestStreamingSumAgg::<I64Array>::default();
        agg.apply_batch(
            &[Op::Delete, Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array_nonnull!(I64Array, [10, 1, 2, 3, 3]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &-7);

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&(vec![false, true, false, false]).into_iter().collect()),
            &[&array_nonnull!(I64Array, [3, 1, 3, 1]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &-8);
    }

    #[test]
    /// Even if there is no element after some insertions and equal number of deletion operations,
    /// `PrimitiveSummable` should output `0` instead of `None`.
    fn test_primitive_sum_no_none() {
        let mut agg = TestStreamingSumAgg::<I64Array>::default();

        assert_eq!(agg.get_output().unwrap(), None);

        agg.apply_batch(
            &[Op::Delete, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array_nonnull!(I64Array, [1, 2, 1, 2]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &0);

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Insert],
            Some(&(vec![false, true, false, true]).into_iter().collect()),
            &[&array_nonnull!(I64Array, [3, 1, 3, 1]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &0);
    }

    #[test]
    fn test_primitive_count() {
        let mut agg = TestStreamingCountAgg::<I64Array>::default();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Delete],
            None,
            &[&array!(I64Array, [Some(1), None, Some(3), Some(1)]).into()],
        )
        .unwrap();

        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &1);

        agg.apply_batch(
            &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
            Some(&(vec![false, true, false, false]).into_iter().collect()),
            &[&array!(I64Array, [Some(1), None, Some(3), Some(1)]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &1);
    }

    #[test]
    fn test_minimum() {
        let mut agg = TestStreamingMinAgg::<I64Array>::default();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(I64Array, [Some(1), Some(10), None, Some(5)]).into()],
        )
        .unwrap();

        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &1);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(I64Array, [Some(1), Some(10), Some(-1), Some(5)]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &-1);
    }

    #[test]
    fn test_minimum_float() {
        let mut agg = TestStreamingMinAgg::<F64Array>::default();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(F64Array, [Some(1.0), Some(10.0), None, Some(5.0)]).into()],
        )
        .unwrap();

        assert_eq!(agg.get_output().unwrap().unwrap().as_float64(), &1.0);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(F64Array, [Some(1.0), Some(10.0), Some(-1.0), Some(5.0)]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_float64(), &-1.0);
    }

    #[test]
    fn test_maximum() {
        let mut agg = TestStreamingMaxAgg::<I64Array>::default();
        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(I64Array, [Some(10), Some(1), None, Some(5)]).into()],
        )
        .unwrap();

        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &10);

        agg.apply_batch(
            &[Op::Insert, Op::Insert, Op::Insert, Op::Insert],
            None,
            &[&array!(I64Array, [Some(1), Some(10), Some(100), Some(5)]).into()],
        )
        .unwrap();
        assert_eq!(agg.get_output().unwrap().unwrap().as_int64(), &100);
    }

    fn bench_i64(
        b: &mut Bencher,
        mut agg: Box<dyn StreamingAggImpl>,
        agg_desc: &str,
        chunk_size: usize,
        vis_rate: f64,
        iter_count: usize,
        append_only: bool,
    ) {
        println!(
            "benching {} agg, chunk_size {}, vis_rate {}, iter_count {}",
            agg_desc, chunk_size, vis_rate, iter_count
        );
        let bitmap = if vis_rate < 1.0 {
            Some(rand_bitmap::gen_rand_bitmap(
                chunk_size,
                (chunk_size as f64 * vis_rate) as usize,
                666,
            ))
        } else {
            None
        };
        let (ops, data) = rand_stream_chunk::gen_legal_stream_chunk(
            bitmap.as_ref(),
            chunk_size,
            append_only,
            666,
        );
        b.iter(|| {
            for _ in 0..iter_count {
                agg.apply_batch(&ops, bitmap.as_ref(), &[&data]).unwrap();
            }
        });
    }

    // TODO: refactor with macro
    #[bench]
    fn bench_foldable_sum_agg_without_vis(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingSumAgg<I64Array>>::default(),
            "sum",
            1024,
            1.0,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_sum_agg_vis_rate_0_75(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingSumAgg<I64Array>>::default(),
            "sum",
            1024,
            0.75,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_sum_agg_vis_rate_0_5(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingSumAgg<I64Array>>::default(),
            "sum",
            1024,
            0.5,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_sum_agg_vis_rate_0_25(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingSumAgg<I64Array>>::default(),
            "sum",
            1024,
            0.25,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_sum_agg_vis_rate_0_05(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingSumAgg<I64Array>>::default(),
            "sum",
            1024,
            0.05,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_count_agg_without_vis(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingCountAgg<I64Array>>::default(),
            "count",
            1024,
            1.0,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_count_agg_vis_rate_0_75(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingCountAgg<I64Array>>::default(),
            "count",
            1024,
            0.75,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_count_agg_vis_rate_0_5(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingCountAgg<I64Array>>::default(),
            "count",
            1024,
            0.5,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_count_agg_vis_rate_0_25(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingCountAgg<I64Array>>::default(),
            "count",
            1024,
            0.25,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_count_agg_vis_rate_0_05(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingCountAgg<I64Array>>::default(),
            "count",
            1024,
            0.05,
            100,
            false,
        );
    }

    #[bench]
    fn bench_foldable_min_agg_without_vis(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMinAgg<I64Array>>::default(),
            "min",
            1024,
            1.0,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_min_agg_vis_rate_0_75(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMinAgg<I64Array>>::default(),
            "min",
            1024,
            0.75,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_min_agg_vis_rate_0_5(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMinAgg<I64Array>>::default(),
            "min",
            1024,
            0.5,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_min_agg_vis_rate_0_25(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMinAgg<I64Array>>::default(),
            "min",
            1024,
            0.25,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_min_agg_vis_rate_0_05(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMinAgg<I64Array>>::default(),
            "min",
            1024,
            0.05,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_max_agg_without_vis(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMaxAgg<I64Array>>::default(),
            "max",
            1024,
            1.0,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_max_agg_vis_rate_0_75(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMaxAgg<I64Array>>::default(),
            "max",
            1024,
            0.75,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_max_agg_vis_rate_0_5(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMaxAgg<I64Array>>::default(),
            "max",
            1024,
            0.5,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_max_agg_vis_rate_0_25(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMaxAgg<I64Array>>::default(),
            "max",
            1024,
            0.25,
            100,
            true,
        );
    }

    #[bench]
    fn bench_foldable_max_agg_vis_rate_0_05(b: &mut Bencher) {
        bench_i64(
            b,
            Box::<TestStreamingMaxAgg<I64Array>>::default(),
            "max",
            1024,
            0.05,
            100,
            true,
        );
    }
}
