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

use num_traits::One;
use risingwave_common::types::{CheckedAdd, IsNegative};
use risingwave_expr_macro::function;

use super::*;

#[function("generate_series(int32, int32) -> setof int32")]
#[function("generate_series(int64, int64) -> setof int64")]
#[function("generate_series(decimal, decimal) -> setof decimal")]
fn generate_series<T>(start: T, stop: T) -> impl Iterator<Item = T>
where
    T: CheckedAdd<Output = T> + PartialOrd + Copy + One + IsNegative,
{
    range_generic::<_, _, true>(start, stop, T::one())
}

#[function("generate_series(int32, int32, int32) -> setof int32")]
#[function("generate_series(int64, int64, int64) -> setof int64")]
#[function("generate_series(decimal, decimal, decimal) -> setof decimal")]
#[function("generate_series(timestamp, timestamp, interval) -> setof timestamp")]
fn generate_series_step<T, S>(start: T, stop: T, step: S) -> impl Iterator<Item = T>
where
    T: CheckedAdd<S, Output = T> + PartialOrd + Copy,
    S: IsNegative + Copy,
{
    range_generic::<_, _, true>(start, stop, step)
}

#[function("range(int32, int32) -> setof int32")]
#[function("range(int64, int64) -> setof int64")]
#[function("range(decimal, decimal) -> setof decimal")]
fn range<T>(start: T, stop: T) -> impl Iterator<Item = T>
where
    T: CheckedAdd<Output = T> + PartialOrd + Copy + One + IsNegative,
{
    range_generic::<_, _, false>(start, stop, T::one())
}

#[function("range(int32, int32, int32) -> setof int32")]
#[function("range(int64, int64, int64) -> setof int64")]
#[function("range(decimal, decimal, decimal) -> setof decimal")]
#[function("range(timestamp, timestamp, interval) -> setof timestamp")]
fn range_step<T, S>(start: T, stop: T, step: S) -> impl Iterator<Item = T>
where
    T: CheckedAdd<S, Output = T> + PartialOrd + Copy,
    S: IsNegative + Copy,
{
    range_generic::<_, _, false>(start, stop, step)
}

#[inline]
fn range_generic<T, S, const INCLUSIVE: bool>(start: T, stop: T, step: S) -> impl Iterator<Item = T>
where
    T: CheckedAdd<S, Output = T> + PartialOrd + Copy,
    S: IsNegative + Copy,
{
    let mut cur = start;
    std::iter::from_fn(move || {
        match (INCLUSIVE, step.is_negative()) {
            (true, true) if cur < stop => return None,
            (true, false) if cur > stop => return None,
            (false, true) if cur <= stop => return None,
            (false, false) if cur >= stop => return None,
            _ => {}
        };
        let ret = cur;
        cur = cur.checked_add(step).unwrap();
        Some(ret)
    })
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
    async fn test_generate_series_i32() {
        generate_series_i32(2, 4, 1).await;
        generate_series_i32(4, 2, -1).await;
        generate_series_i32(0, 9, 2).await;
        generate_series_i32(0, (CHUNK_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn generate_series_i32(start: i32, stop: i32, step: i32) {
        fn literal(v: i32) -> BoxedExpression {
            LiteralExpression::new(DataType::Int32, Some(v.into())).boxed()
        }
        let function = build(
            PbType::GenerateSeries,
            DataType::Int32,
            CHUNK_SIZE,
            vec![literal(start), literal(stop), literal(step)],
        )
        .unwrap();
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
    async fn test_generate_series_timestamp() {
        let start_time = str_to_timestamp("2008-03-01 00:00:00").unwrap();
        let stop_time = str_to_timestamp("2008-03-09 00:00:00").unwrap();
        let one_minute_step = Interval::from_minutes(1);
        let one_hour_step = Interval::from_minutes(60);
        let one_day_step = Interval::from_days(1);
        generate_series_timestamp(start_time, stop_time, one_minute_step, 60 * 24 * 8 + 1).await;
        generate_series_timestamp(start_time, stop_time, one_hour_step, 24 * 8 + 1).await;
        generate_series_timestamp(start_time, stop_time, one_day_step, 8 + 1).await;
        generate_series_timestamp(stop_time, start_time, -one_day_step, 8 + 1).await;
    }

    async fn generate_series_timestamp(
        start: Timestamp,
        stop: Timestamp,
        step: Interval,
        expect_cnt: usize,
    ) {
        fn literal(ty: DataType, v: ScalarImpl) -> BoxedExpression {
            LiteralExpression::new(ty, Some(v)).boxed()
        }
        let function = build(
            PbType::GenerateSeries,
            DataType::Timestamp,
            CHUNK_SIZE,
            vec![
                literal(DataType::Timestamp, start.into()),
                literal(DataType::Timestamp, stop.into()),
                literal(DataType::Interval, step.into()),
            ],
        )
        .unwrap();

        let dummy_chunk = DataChunk::new_dummy(1);
        let mut actual_cnt = 0;
        let mut output = function.eval(&dummy_chunk).await;
        while let Some(Ok(chunk)) = output.next().await {
            actual_cnt += chunk.cardinality();
        }
        assert_eq!(actual_cnt, expect_cnt);
    }

    #[tokio::test]
    async fn test_range_i32() {
        range_i32(2, 4, 1).await;
        range_i32(4, 2, -1).await;
        range_i32(0, 9, 2).await;
        range_i32(0, (CHUNK_SIZE * 2 + 3) as i32, 1).await;
    }

    async fn range_i32(start: i32, stop: i32, step: i32) {
        fn literal(v: i32) -> BoxedExpression {
            LiteralExpression::new(DataType::Int32, Some(v.into())).boxed()
        }
        let function = build(
            PbType::Range,
            DataType::Int32,
            CHUNK_SIZE,
            vec![literal(start), literal(stop), literal(step)],
        )
        .unwrap();
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
    async fn test_range_timestamp() {
        let start_time = str_to_timestamp("2008-03-01 00:00:00").unwrap();
        let stop_time = str_to_timestamp("2008-03-09 00:00:00").unwrap();
        let one_minute_step = Interval::from_minutes(1);
        let one_hour_step = Interval::from_minutes(60);
        let one_day_step = Interval::from_days(1);
        range_timestamp(start_time, stop_time, one_minute_step, 60 * 24 * 8).await;
        range_timestamp(start_time, stop_time, one_hour_step, 24 * 8).await;
        range_timestamp(start_time, stop_time, one_day_step, 8).await;
        range_timestamp(stop_time, start_time, -one_day_step, 8).await;
    }

    async fn range_timestamp(start: Timestamp, stop: Timestamp, step: Interval, expect_cnt: usize) {
        fn literal(ty: DataType, v: ScalarImpl) -> BoxedExpression {
            LiteralExpression::new(ty, Some(v)).boxed()
        }
        let function = build(
            PbType::Range,
            DataType::Timestamp,
            CHUNK_SIZE,
            vec![
                literal(DataType::Timestamp, start.into()),
                literal(DataType::Timestamp, stop.into()),
                literal(DataType::Interval, step.into()),
            ],
        )
        .unwrap();

        let dummy_chunk = DataChunk::new_dummy(1);
        let mut actual_cnt = 0;
        let mut output = function.eval(&dummy_chunk).await;
        while let Some(Ok(chunk)) = output.next().await {
            actual_cnt += chunk.cardinality();
        }
        assert_eq!(actual_cnt, expect_cnt);
    }
}
