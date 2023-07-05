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

use std::convert::From;
use std::ops::{BitAnd, BitOr, BitXor};

use num_traits::CheckedAdd;
use risingwave_expr_macro::aggregate;

use crate::{ExprError, Result};

#[aggregate("sum(int16) -> int64")]
#[aggregate("sum(int32) -> int64")]
#[aggregate("sum(int64) -> int64")]
#[aggregate("sum(int64) -> decimal")]
#[aggregate("sum(float32) -> float32")]
#[aggregate("sum(float64) -> float64")]
#[aggregate("sum(decimal) -> decimal")]
#[aggregate("sum(interval) -> interval")]
#[aggregate("sum(int256) -> int256", state = "Int256")]
#[aggregate("sum0(int64) -> int64", init_state = "Some(0)")]
fn sum<S, T>(state: S, input: T) -> Result<S>
where
    S: From<T> + CheckedAdd<Output = S>,
{
    state
        .checked_add(&S::from(input))
        .ok_or(ExprError::NumericOutOfRange)
}

#[aggregate("min(*) -> auto")]
fn min<T: Ord>(state: T, input: T) -> T {
    state.min(input)
}

#[aggregate("max(*) -> auto")]
fn max<T: Ord>(state: T, input: T) -> T {
    state.max(input)
}

#[aggregate("bit_and(*int) -> auto")]
fn bit_and<T>(state: T, input: T) -> T
where
    T: BitAnd<Output = T>,
{
    state.bitand(input)
}

#[aggregate("bit_or(*int) -> auto")]
fn bit_or<T>(state: T, input: T) -> T
where
    T: BitOr<Output = T>,
{
    state.bitor(input)
}

#[aggregate("bit_xor(*int) -> auto")]
fn bit_xor<T>(state: T, input: T) -> T
where
    T: BitXor<Output = T>,
{
    state.bitxor(input)
}

#[aggregate("first_value(*) -> auto")]
fn first_value<T>(state: T, _: T) -> T {
    state
}

#[aggregate("last_value(*) -> auto")]
fn last_value<T>(_: T, input: T) -> T {
    input
}

/// Note the following corner cases:
///
/// ```slt
/// statement ok
/// create table t(v1 int);
///
/// statement ok
/// insert into t values (null);
///
/// query I
/// select count(*) from t;
/// ----
/// 1
///
/// query I
/// select count(v1) from t;
/// ----
/// 0
///
/// query I
/// select sum(v1) from t;
/// ----
/// NULL
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("count(*) -> int64", init_state = "Some(0)")]
fn count<T>(state: i64, _: T) -> i64 {
    state + 1
}

/// Returns true if all non-null input values are true, otherwise false.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (b1 boolean, b2 boolean, b3 boolean, b4 boolean);
///
/// query T
/// select bool_and(b1) from t;
/// ----
/// NULL
///
/// statement ok
/// insert into t values
///     (true,  null, false, null),
///     (false, true, null,  null),
///     (null,  true, false, null);
///
/// query TTTTTT
/// select
///     bool_and(b1),
///     bool_and(b2),
///     bool_and(b3),
///     bool_and(b4),
///     bool_and(NOT b2),
///     bool_and(NOT b3)
/// FROM t;
/// ----
/// f t f NULL f t
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("bool_and(boolean) -> boolean")]
fn bool_and(state: bool, input: bool) -> bool {
    state && input
}

/// Returns true if any non-null input value is true, otherwise false.
///
/// # Example
///
/// ```slt
/// statement ok
/// create table t (b1 boolean, b2 boolean, b3 boolean, b4 boolean);
///
/// query T
/// select bool_or(b1) from t;
/// ----
/// NULL
///
/// statement ok
/// insert into t values
///     (true,  null, false, null),
///     (false, true, null,  null),
///     (null,  true, false, null);
///
/// query TTTTTT
/// select
///     bool_or(b1),
///     bool_or(b2),
///     bool_or(b3),
///     bool_or(b4),
///     bool_or(NOT b2),
///     bool_or(NOT b3)
/// FROM t;
/// ----
/// t t f NULL f t
///
/// statement ok
/// drop table t;
/// ```
#[aggregate("bool_or(boolean) -> boolean")]
fn bool_or(state: bool, input: bool) -> bool {
    state || input
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::*;
    use risingwave_common::types::{DataType, Decimal};

    use crate::agg::AggCall;
    use crate::Result;

    async fn eval_agg(
        pretty: &str,
        input: ArrayRef,
        mut builder: ArrayBuilderImpl,
    ) -> Result<ArrayImpl> {
        let len = input.len();
        let input_chunk = DataChunk::new(vec![input], len);
        let mut agg_state = crate::agg::build(AggCall::from_pretty(pretty))?;
        agg_state
            .update_multi(&input_chunk, 0, input_chunk.cardinality())
            .await?;
        agg_state.output(&mut builder)?;
        Ok(builder.finish())
    }

    #[tokio::test]
    async fn vec_sum_int32() -> Result<()> {
        let input = I32Array::from_iter([1, 2, 3]);
        let actual = eval_agg(
            "(sum:int8 $0:int4)",
            Arc::new(input.into()),
            ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)),
        )
        .await?;
        let actual = actual.as_int64();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(6)]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_sum_int64() -> Result<()> {
        let input = I64Array::from_iter([1, 2, 3]);
        let actual = eval_agg(
            "(sum:decimal $0:int8)",
            Arc::new(input.into()),
            DecimalArrayBuilder::new(0).into(),
        )
        .await?;
        let actual: DecimalArray = actual.into();
        let actual = actual.iter().collect::<Vec<Option<Decimal>>>();
        assert_eq!(actual, vec![Some(Decimal::from(6))]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_min_float32() -> Result<()> {
        let input = F32Array::from_iter([1.0, 2.0, 3.0]);
        let actual = eval_agg(
            "(min:float4 $0:float4)",
            Arc::new(input.into()),
            ArrayBuilderImpl::Float32(F32ArrayBuilder::new(0)),
        )
        .await?;
        let actual = actual.as_float32();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(1.0.into())]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_min_char() -> Result<()> {
        let input = Utf8Array::from_iter(["b", "aa"]);
        let actual = eval_agg(
            "(min:varchar $0:varchar)",
            Arc::new(input.into()),
            ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0)),
        )
        .await?;
        let actual = actual.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("aa")]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_min_list() -> Result<()> {
        let input = ListArray::from_iter(
            [
                Some(I32Array::from_iter([Some(0)]).into()),
                Some(I32Array::from_iter([Some(1)]).into()),
                Some(I32Array::from_iter([Some(2)]).into()),
            ],
            DataType::Int32,
        );
        let actual = eval_agg(
            "(min:int4[] $0:int4[])",
            Arc::new(input.into()),
            ArrayBuilderImpl::List(ListArrayBuilder::with_type(
                0,
                DataType::List(Box::new(DataType::Int32)),
            )),
        )
        .await?;
        let actual = actual.as_list();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![Some(ListRef::ValueRef {
                val: &ListValue::new(vec![Some(0i32.into())])
            })]
        );
        Ok(())
    }

    #[tokio::test]
    async fn vec_max_char() -> Result<()> {
        let input = Utf8Array::from_iter(["b", "aa"]);
        let actual = eval_agg(
            "(max:varchar $0:varchar)",
            Arc::new(input.into()),
            ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0)),
        )
        .await?;
        let actual = actual.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("b")]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_count_int32() -> Result<()> {
        async fn test_case(input: ArrayImpl, expected: &[Option<i64>]) -> Result<()> {
            let actual = eval_agg(
                "(count:int8 $0:int4)",
                Arc::new(input),
                ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)),
            )
            .await?;
            let actual = actual.as_int64();
            let actual = actual.iter().collect::<Vec<_>>();
            assert_eq!(actual, expected);
            Ok(())
        }
        let input = I32Array::from_iter([1, 2, 3]);
        let expected = &[Some(3)];
        test_case(input.into(), expected).await?;
        #[allow(clippy::needless_borrow)]
        let input = I32Array::from_iter(&[]);
        let expected = &[Some(0)];
        test_case(input.into(), expected).await?;
        let input = I32Array::from_iter([None]);
        let expected = &[Some(0)];
        test_case(input.into(), expected).await
    }

    #[tokio::test]
    async fn vec_distinct_sum_int32() -> Result<()> {
        let input = I32Array::from_iter([1, 1, 3]);
        let actual = eval_agg(
            "(sum:int8 $0:int4 distinct)",
            Arc::new(input.into()),
            ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)),
        )
        .await?;
        let actual = actual.as_int64();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(4)]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_distinct_sum_int64() -> Result<()> {
        let input = I64Array::from_iter([1, 1, 3]);
        let actual = eval_agg(
            "(sum:decimal $0:int8 distinct)",
            Arc::new(input.into()),
            DecimalArrayBuilder::new(0).into(),
        )
        .await?;
        let actual: &DecimalArray = (&actual).into();
        let actual = actual.iter().collect::<Vec<Option<Decimal>>>();
        assert_eq!(actual, vec![Some(Decimal::from(4))]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_distinct_min_float32() -> Result<()> {
        let input = F32Array::from_iter([1.0, 2.0, 3.0]);
        let actual = eval_agg(
            "(min:float4 $0:float4 distinct)",
            Arc::new(input.into()),
            ArrayBuilderImpl::Float32(F32ArrayBuilder::new(0)),
        )
        .await?;
        let actual = actual.as_float32();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(1.0.into())]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_distinct_min_char() -> Result<()> {
        let input = Utf8Array::from_iter(["b", "aa"]);
        let actual = eval_agg(
            "(min:varchar $0:varchar distinct)",
            Arc::new(input.into()),
            ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0)),
        )
        .await?;
        let actual = actual.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("aa")]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_distinct_max_char() -> Result<()> {
        let input = Utf8Array::from_iter(["b", "aa"]);
        let actual = eval_agg(
            "(max:varchar $0:varchar distinct)",
            Arc::new(input.into()),
            ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0)),
        )
        .await?;
        let actual = actual.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("b")]);
        Ok(())
    }

    #[tokio::test]
    async fn vec_distinct_count_int32() -> Result<()> {
        async fn test_case(input: ArrayImpl, expected: &[Option<i64>]) -> Result<()> {
            let actual = eval_agg(
                "(count:int8 $0:int4 distinct)",
                Arc::new(input),
                ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)),
            )
            .await?;
            let actual = actual.as_int64();
            let actual = actual.iter().collect::<Vec<_>>();
            assert_eq!(actual, expected);
            Ok(())
        }
        let input = I32Array::from_iter([1, 1, 3]);
        let expected = &[Some(2)];
        test_case(input.into(), expected).await?;
        #[allow(clippy::needless_borrow)]
        let input = I32Array::from_iter(&[]);
        let expected = &[Some(0)];
        test_case(input.into(), expected).await?;
        let input = I32Array::from_iter([None]);
        let expected = &[Some(0)];
        test_case(input.into(), expected).await
    }
}
