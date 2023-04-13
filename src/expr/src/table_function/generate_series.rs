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
use risingwave_common::types::CheckedAdd;
use risingwave_expr_macro::table_function;

use super::*;

#[table_function("generate_series(int32, int32) -> int32")]
#[table_function("generate_series(int64, int64) -> int64")]
#[table_function("generate_series(decimal, decimal) -> decimal")]
fn generate_series<T>(start: T, stop: T) -> impl Iterator<Item = T>
where
    T: CheckedAdd<Output = T> + PartialOrd + Copy + One,
{
    generate_series_step(start, stop, T::one())
}

#[table_function("generate_series(int32, int32, int32) -> int32")]
#[table_function("generate_series(int64, int64, int64) -> int64")]
#[table_function("generate_series(decimal, decimal, decimal) -> decimal")]
#[table_function("generate_series(timestamp, timestamp, interval) -> timestamp")]
fn generate_series_step<T, S>(start: T, stop: T, step: S) -> impl Iterator<Item = T>
where
    T: CheckedAdd<S, Output = T> + PartialOrd + Copy,
    S: Copy,
{
    let mut cur = start;
    std::iter::from_fn(move || {
        if cur > stop {
            None
        } else {
            let ret = cur;
            cur = cur.checked_add(step).unwrap();
            Some(ret)
        }
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
