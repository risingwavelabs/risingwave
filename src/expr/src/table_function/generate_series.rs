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

use anyhow::anyhow;
use itertools::multizip;
use num_traits::Zero;
use risingwave_common::array::{
    Array, ArrayImpl, DataChunk, I32Array, IntervalArray, TimestampArray,
};
use risingwave_common::types::{CheckedAdd, IsNegative, Scalar, ScalarRef, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqDebug;

use super::*;
use crate::ExprError;

#[derive(Debug)]
pub struct GenerateSeries<T: Array, S: Array, const STOP_INCLUSIVE: bool> {
    start: BoxedExpression,
    stop: BoxedExpression,
    step: BoxedExpression,
    chunk_size: usize,
    _phantom: std::marker::PhantomData<(T, S)>,
}

impl<T: Array, S: Array, const STOP_INCLUSIVE: bool> GenerateSeries<T, S, STOP_INCLUSIVE>
where
    T::OwnedItem: for<'a> PartialOrd<T::RefItem<'a>>,
    T::OwnedItem: for<'a> CheckedAdd<S::RefItem<'a>, Output = T::OwnedItem>,
    for<'a> S::RefItem<'a>: IsNegative,
    for<'a> &'a T: From<&'a ArrayImpl>,
    for<'a> &'a S: From<&'a ArrayImpl>,
{
    fn new(
        start: BoxedExpression,
        stop: BoxedExpression,
        step: BoxedExpression,
        chunk_size: usize,
    ) -> Self {
        Self {
            start,
            stop,
            step,
            chunk_size,
            _phantom: Default::default(),
        }
    }

    #[try_stream(ok = T::OwnedItem, error = ExprError)]
    async fn eval_row<'a>(
        &'a self,
        start: T::RefItem<'a>,
        stop: T::RefItem<'a>,
        step: S::RefItem<'a>,
    ) {
        if step.is_zero() {
            return Err(ExprError::InvalidParam {
                name: "step",
                reason: "must be non-zero".to_string(),
            });
        }

        let mut cur: T::OwnedItem = start.to_owned_scalar();

        while if step.is_negative() {
            if STOP_INCLUSIVE {
                cur >= stop
            } else {
                cur > stop
            }
        } else if STOP_INCLUSIVE {
            cur <= stop
        } else {
            cur < stop
        } {
            yield cur.clone();
            cur = cur.checked_add(step).ok_or(ExprError::NumericOutOfRange)?;
        }
    }

    #[try_stream(boxed, ok = DataChunk, error = ExprError)]
    async fn eval_inner<'a>(&'a self, input: &'a DataChunk) {
        let ret_start = self.start.eval_checked(input).await?;
        let arr_start: &T = ret_start.as_ref().into();
        let ret_stop = self.stop.eval_checked(input).await?;
        let arr_stop: &T = ret_stop.as_ref().into();
        let ret_step = self.step.eval_checked(input).await?;
        let arr_step: &S = ret_step.as_ref().into();

        let mut builder =
            DataChunkBuilder::new(vec![DataType::Int64, self.return_type()], self.chunk_size);
        for (i, ((start, stop, step), visible)) in
            multizip((arr_start.iter(), arr_stop.iter(), arr_step.iter()))
                .zip_eq_debug(input.vis().iter())
                .enumerate()
        {
            if let (Some(start), Some(stop), Some(step)) = (start, stop, step) && visible {
                #[for_await]
                for res in self.eval_row(start, stop, step) {
                    let value = res?;
                    if let Some(chunk) = builder.append_one_row([Some(ScalarRefImpl::Int64(i as i64)), Some(value.as_scalar_ref().into())]) {
                        yield chunk;
                    }
                }
            }
        }
        if let Some(chunk) = builder.consume_all() {
            yield chunk;
        }
    }
}

#[async_trait::async_trait]
impl<T: Array, S: Array, const STOP_INCLUSIVE: bool> TableFunction
    for GenerateSeries<T, S, STOP_INCLUSIVE>
where
    T::OwnedItem: for<'a> PartialOrd<T::RefItem<'a>>,
    T::OwnedItem: for<'a> CheckedAdd<S::RefItem<'a>, Output = T::OwnedItem>,
    for<'a> S::RefItem<'a>: IsNegative,
    for<'a> &'a T: From<&'a ArrayImpl>,
    for<'a> &'a S: From<&'a ArrayImpl>,
{
    fn return_type(&self) -> DataType {
        self.start.return_type()
    }

    async fn eval<'a>(&'a self, input: &'a DataChunk) -> BoxStream<'a, Result<DataChunk>> {
        self.eval_inner(input)
    }
}

pub fn new_generate_series<const STOP_INCLUSIVE: bool>(
    prost: &PbTableFunction,
    chunk_size: usize,
) -> Result<BoxedTableFunction> {
    let return_type = DataType::from(prost.get_return_type().unwrap());
    let args: Vec<_> = prost.args.iter().map(expr_build_from_prost).try_collect()?;
    let [start, stop, step]: [_; 3] = args.try_into().unwrap();

    match return_type {
        DataType::Timestamp => Ok(
            GenerateSeries::<TimestampArray, IntervalArray, STOP_INCLUSIVE>::new(
                start, stop, step, chunk_size,
            )
            .boxed(),
        ),
        DataType::Int32 => Ok(GenerateSeries::<I32Array, I32Array, STOP_INCLUSIVE>::new(
            start, stop, step, chunk_size,
        )
        .boxed()),
        _ => Err(ExprError::Internal(anyhow!(
            "the return type of Generate Series Function is incorrect".to_string(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{DataType, Interval, ScalarImpl, Timestamp};

    use super::*;
    use crate::expr::{Expression, LiteralExpression};
    use crate::vector_op::cast::str_to_timestamp;

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_generate_i32_series() {
        generate_series_test_case(2, 4, 1).await;
        generate_series_test_case(4, 2, -1).await;
        generate_series_test_case(0, 9, 2).await;
        generate_series_test_case(0, (CHUNK_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn generate_series_test_case(start: i32, stop: i32, step: i32) {
        fn to_lit_expr(v: i32) -> BoxedExpression {
            LiteralExpression::new(DataType::Int32, Some(v.into())).boxed()
        }

        let function = GenerateSeries::<I32Array, I32Array, true>::new(
            to_lit_expr(start),
            to_lit_expr(stop),
            to_lit_expr(step),
            CHUNK_SIZE,
        )
        .boxed();
        let expect_cnt = ((stop - start) / step + 1) as usize;

        let dummy_chunk = DataChunk::new_dummy(1);
        let mut actual_cnt = 0;
        let mut output = function.eval(&dummy_chunk).await;
        while let Some(Ok(chunk)) = output.next().await {
            actual_cnt += chunk.cardinality();
        }
        assert_eq!(actual_cnt, expect_cnt);
    }

    #[tokio::test]
    async fn test_generate_time_series() {
        let start_time = str_to_timestamp("2008-03-01 00:00:00").unwrap();
        let stop_time = str_to_timestamp("2008-03-09 00:00:00").unwrap();
        let one_minute_step = Interval::from_minutes(1);
        let one_hour_step = Interval::from_minutes(60);
        let one_day_step = Interval::from_days(1);
        generate_time_series_test_case(start_time, stop_time, one_minute_step, 60 * 24 * 8 + 1)
            .await;
        generate_time_series_test_case(start_time, stop_time, one_hour_step, 24 * 8 + 1).await;
        generate_time_series_test_case(start_time, stop_time, one_day_step, 8 + 1).await;
        generate_time_series_test_case(stop_time, start_time, -one_day_step, 8 + 1).await;
    }

    async fn generate_time_series_test_case(
        start: Timestamp,
        stop: Timestamp,
        step: Interval,
        expect_cnt: usize,
    ) {
        fn to_lit_expr(ty: DataType, v: ScalarImpl) -> BoxedExpression {
            LiteralExpression::new(ty, Some(v)).boxed()
        }

        let function = GenerateSeries::<TimestampArray, IntervalArray, true>::new(
            to_lit_expr(DataType::Timestamp, start.into()),
            to_lit_expr(DataType::Timestamp, stop.into()),
            to_lit_expr(DataType::Interval, step.into()),
            CHUNK_SIZE,
        );

        let dummy_chunk = DataChunk::new_dummy(1);
        let mut actual_cnt = 0;
        let mut output = function.eval(&dummy_chunk).await;
        while let Some(Ok(chunk)) = output.next().await {
            actual_cnt += chunk.cardinality();
        }
        assert_eq!(actual_cnt, expect_cnt);
    }

    #[tokio::test]
    async fn test_i32_range() {
        range_test_case(2, 4, 1).await;
        range_test_case(4, 2, -1).await;
        range_test_case(0, 9, 2).await;
        range_test_case(0, (CHUNK_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn range_test_case(start: i32, stop: i32, step: i32) {
        fn to_lit_expr(v: i32) -> BoxedExpression {
            LiteralExpression::new(DataType::Int32, Some(v.into())).boxed()
        }

        let function = GenerateSeries::<I32Array, I32Array, false>::new(
            to_lit_expr(start),
            to_lit_expr(stop),
            to_lit_expr(step),
            CHUNK_SIZE,
        )
        .boxed();
        let expect_cnt = ((stop - start - step.signum()) / step + 1) as usize;

        let dummy_chunk = DataChunk::new_dummy(1);
        let mut actual_cnt = 0;
        let mut output = function.eval(&dummy_chunk).await;
        while let Some(Ok(chunk)) = output.next().await {
            actual_cnt += chunk.cardinality();
        }
        assert_eq!(actual_cnt, expect_cnt);
    }

    #[tokio::test]
    async fn test_time_range() {
        let start_time = str_to_timestamp("2008-03-01 00:00:00").unwrap();
        let stop_time = str_to_timestamp("2008-03-09 00:00:00").unwrap();
        let one_minute_step = Interval::from_minutes(1);
        let one_hour_step = Interval::from_minutes(60);
        let one_day_step = Interval::from_days(1);
        time_range_test_case(start_time, stop_time, one_minute_step, 60 * 24 * 8).await;
        time_range_test_case(start_time, stop_time, one_hour_step, 24 * 8).await;
        time_range_test_case(start_time, stop_time, one_day_step, 8).await;
        time_range_test_case(stop_time, start_time, -one_day_step, 8).await;
    }

    async fn time_range_test_case(
        start: Timestamp,
        stop: Timestamp,
        step: Interval,
        expect_cnt: usize,
    ) {
        fn to_lit_expr(ty: DataType, v: ScalarImpl) -> BoxedExpression {
            LiteralExpression::new(ty, Some(v)).boxed()
        }

        let function = GenerateSeries::<TimestampArray, IntervalArray, false>::new(
            to_lit_expr(DataType::Timestamp, start.into()),
            to_lit_expr(DataType::Timestamp, stop.into()),
            to_lit_expr(DataType::Interval, step.into()),
            CHUNK_SIZE,
        );

        let dummy_chunk = DataChunk::new_dummy(1);
        let mut actual_cnt = 0;
        let mut output = function.eval(&dummy_chunk).await;
        while let Some(Ok(chunk)) = output.next().await {
            actual_cnt += chunk.cardinality();
        }
        assert_eq!(actual_cnt, expect_cnt);
    }
}
