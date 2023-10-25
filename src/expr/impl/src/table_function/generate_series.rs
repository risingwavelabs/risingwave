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
use risingwave_common::types::{CheckedAdd, Decimal, IsNegative};
use risingwave_expr::{function, ExprError, Result};

#[function("generate_series(int4, int4) -> setof int4")]
#[function("generate_series(int8, int8) -> setof int8")]
fn generate_series<T>(start: T, stop: T) -> Result<impl Iterator<Item = Result<T>>>
where
    T: CheckedAdd<Output = T> + PartialOrd + Copy + One + IsNegative,
{
    range_generic::<_, _, true>(start, stop, T::one())
}

#[function("generate_series(decimal, decimal) -> setof decimal")]
fn generate_series_decimal(
    start: Decimal,
    stop: Decimal,
) -> Result<impl Iterator<Item = Result<Decimal>>>
where
{
    validate_range_parameters(start, stop, Decimal::one())?;
    range_generic::<Decimal, Decimal, true>(start, stop, Decimal::one())
}

#[function("generate_series(int4, int4, int4) -> setof int4")]
#[function("generate_series(int8, int8, int8) -> setof int8")]
#[function("generate_series(timestamp, timestamp, interval) -> setof timestamp")]
fn generate_series_step<T, S>(start: T, stop: T, step: S) -> Result<impl Iterator<Item = Result<T>>>
where
    T: CheckedAdd<S, Output = T> + PartialOrd + Copy,
    S: IsNegative + Copy,
{
    range_generic::<_, _, true>(start, stop, step)
}

#[function("generate_series(decimal, decimal, decimal) -> setof decimal")]
fn generate_series_step_decimal(
    start: Decimal,
    stop: Decimal,
    step: Decimal,
) -> Result<impl Iterator<Item = Result<Decimal>>> {
    validate_range_parameters(start, stop, step)?;
    range_generic::<_, _, true>(start, stop, step)
}

#[function("range(int4, int4) -> setof int4")]
#[function("range(int8, int8) -> setof int8")]
fn range<T>(start: T, stop: T) -> Result<impl Iterator<Item = Result<T>>>
where
    T: CheckedAdd<Output = T> + PartialOrd + Copy + One + IsNegative,
{
    range_generic::<_, _, false>(start, stop, T::one())
}

#[function("range(decimal, decimal) -> setof decimal")]
fn range_decimal(start: Decimal, stop: Decimal) -> Result<impl Iterator<Item = Result<Decimal>>>
where
{
    validate_range_parameters(start, stop, Decimal::one())?;
    range_generic::<Decimal, Decimal, false>(start, stop, Decimal::one())
}

#[function("range(int4, int4, int4) -> setof int4")]
#[function("range(int8, int8, int8) -> setof int8")]
#[function("range(timestamp, timestamp, interval) -> setof timestamp")]
fn range_step<T, S>(start: T, stop: T, step: S) -> Result<impl Iterator<Item = Result<T>>>
where
    T: CheckedAdd<S, Output = T> + PartialOrd + Copy,
    S: IsNegative + Copy,
{
    range_generic::<_, _, false>(start, stop, step)
}

#[function("range(decimal, decimal, decimal) -> setof decimal")]
fn range_step_decimal(
    start: Decimal,
    stop: Decimal,
    step: Decimal,
) -> Result<impl Iterator<Item = Result<Decimal>>> {
    validate_range_parameters(start, stop, step)?;
    range_generic::<_, _, false>(start, stop, step)
}

#[inline]
fn range_generic<T, S, const INCLUSIVE: bool>(
    start: T,
    stop: T,
    step: S,
) -> Result<impl Iterator<Item = Result<T>>>
where
    T: CheckedAdd<S, Output = T> + PartialOrd + Copy,
    S: IsNegative + Copy,
{
    if step.is_zero() {
        return Err(ExprError::InvalidParam {
            name: "step",
            reason: "step size cannot equal zero".into(),
        });
    }
    let mut cur = start;
    let neg = step.is_negative();
    let mut next = move || {
        match (INCLUSIVE, neg) {
            (true, true) if cur < stop => return Ok(None),
            (true, false) if cur > stop => return Ok(None),
            (false, true) if cur <= stop => return Ok(None),
            (false, false) if cur >= stop => return Ok(None),
            _ => {}
        };
        let ret = cur;
        cur = cur.checked_add(step).ok_or(ExprError::NumericOutOfRange)?;
        Ok(Some(ret))
    };
    Ok(std::iter::from_fn(move || next().transpose()))
}

#[inline]
fn validate_range_parameters(start: Decimal, stop: Decimal, step: Decimal) -> Result<()> {
    validate_decimal(start, "start")?;
    validate_decimal(stop, "stop")?;
    validate_decimal(step, "step")?;
    Ok(())
}

#[inline]
fn validate_decimal(decimal: Decimal, name: &'static str) -> Result<()> {
    match decimal {
        Decimal::Normalized(_) => Ok(()),
        Decimal::PositiveInf | Decimal::NegativeInf => Err(ExprError::InvalidParam {
            name,
            reason: format!("{} value cannot be infinity", name).into(),
        }),
        Decimal::NaN => Err(ExprError::InvalidParam {
            name,
            reason: format!("{} value cannot be NaN", name).into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use futures_util::StreamExt;
    use risingwave_common::array::DataChunk;
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{DataType, Decimal, Interval, ScalarImpl, Timestamp};
    use risingwave_expr::expr::{BoxedExpression, ExpressionBoxExt, LiteralExpression};
    use risingwave_expr::table_function::build;
    use risingwave_expr::ExprError;
    use risingwave_pb::expr::table_function::PbType;

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
        let start_time = Timestamp::from_str("2008-03-01 00:00:00").unwrap();
        let stop_time = Timestamp::from_str("2008-03-09 00:00:00").unwrap();
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
        let start_time = Timestamp::from_str("2008-03-01 00:00:00").unwrap();
        let stop_time = Timestamp::from_str("2008-03-09 00:00:00").unwrap();
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

    #[tokio::test]
    async fn test_generate_series_decimal() {
        let start = Decimal::from_str("1").unwrap();
        let start_inf = Decimal::from_str("infinity").unwrap();
        let stop = Decimal::from_str("5").unwrap();
        let stop_inf = Decimal::from_str("-infinity").unwrap();

        let step = Decimal::from_str("1").unwrap();
        let step_nan = Decimal::from_str("nan").unwrap();
        let step_inf = Decimal::from_str("infinity").unwrap();
        generate_series_decimal(start, stop, step, true).await;
        generate_series_decimal(start_inf, stop, step, false).await;
        generate_series_decimal(start_inf, stop_inf, step, false).await;
        generate_series_decimal(start, stop_inf, step, false).await;
        generate_series_decimal(start, stop, step_nan, false).await;
        generate_series_decimal(start, stop, step_inf, false).await;
        generate_series_decimal(start, stop_inf, step_nan, false).await;
    }

    async fn generate_series_decimal(
        start: Decimal,
        stop: Decimal,
        step: Decimal,
        expect_ok: bool,
    ) {
        fn literal(ty: DataType, v: ScalarImpl) -> BoxedExpression {
            LiteralExpression::new(ty, Some(v)).boxed()
        }
        let function = build(
            PbType::GenerateSeries,
            DataType::Decimal,
            CHUNK_SIZE,
            vec![
                literal(DataType::Decimal, start.into()),
                literal(DataType::Decimal, stop.into()),
                literal(DataType::Decimal, step.into()),
            ],
        )
        .unwrap();

        let dummy_chunk = DataChunk::new_dummy(1);
        let mut output = function.eval(&dummy_chunk).await;
        while let Some(res) = output.next().await {
            match res {
                Ok(_) => {
                    assert!(expect_ok);
                }
                Err(ExprError::InvalidParam { .. }) => {
                    assert!(!expect_ok);
                }
                Err(_) => {
                    unreachable!();
                }
            }
        }
    }
}
