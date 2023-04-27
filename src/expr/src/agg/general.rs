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

#[aggregate("bit_and(int16) -> int16")]
#[aggregate("bit_and(int32) -> int32")]
#[aggregate("bit_and(int64) -> int64")]
fn bit_and<T>(state: T, input: T) -> T
where
    T: BitAnd<Output = T>,
{
    state.bitand(input)
}

#[aggregate("bit_or(int16) -> int16")]
#[aggregate("bit_or(int32) -> int32")]
#[aggregate("bit_or(int64) -> int64")]
fn bit_or<T>(state: T, input: T) -> T
where
    T: BitOr<Output = T>,
{
    state.bitor(input)
}

#[aggregate("bit_xor(int16) -> int16")]
#[aggregate("bit_xor(int32) -> int32")]
#[aggregate("bit_xor(int64) -> int64")]
fn bit_xor<T>(state: T, input: T) -> T
where
    T: BitXor<Output = T>,
{
    state.bitxor(input)
}

#[aggregate("first_value(*) -> auto")]
fn first<T>(state: T, _: T) -> T {
    state
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::*;
    use risingwave_common::types::{DataType, Decimal};

    use crate::agg::{AggArgs, AggCall, AggKind};
    use crate::Result;

    async fn eval_agg(
        input_type: DataType,
        input: ArrayRef,
        agg_kind: AggKind,
        return_type: DataType,
        mut builder: ArrayBuilderImpl,
    ) -> Result<ArrayImpl> {
        let len = input.len();
        let input_chunk = DataChunk::new(vec![Column::new(input)], len);
        let mut agg_state = crate::agg::build(AggCall {
            kind: agg_kind,
            args: AggArgs::Unary(input_type, 0),
            return_type,
            column_orders: vec![],
            filter: None,
            distinct: false,
        })?;
        agg_state
            .update_multi(&input_chunk, 0, input_chunk.cardinality())
            .await?;
        agg_state.output(&mut builder)?;
        Ok(builder.finish())
    }

    #[tokio::test]
    async fn vec_sum_int32() -> Result<()> {
        let input = I32Array::from_iter([1, 2, 3]);
        let agg_kind = AggKind::Sum;
        let input_type = DataType::Int32;
        let return_type = DataType::Int64;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
        let agg_kind = AggKind::Sum;
        let input_type = DataType::Int64;
        let return_type = DataType::Decimal;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
        let input = F32Array::from_iter([Some(1.0.into()), Some(2.0.into()), Some(3.0.into())]);
        let agg_kind = AggKind::Min;
        let input_type = DataType::Float32;
        let return_type = DataType::Float32;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
        let agg_kind = AggKind::Min;
        let input_type = DataType::Varchar;
        let return_type = DataType::Varchar;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
        let agg_type = AggKind::Min;
        let input_type = DataType::List {
            datatype: Box::new(DataType::Int32),
        };
        let return_type = DataType::List {
            datatype: Box::new(DataType::Int32),
        };
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            agg_type,
            return_type,
            ArrayBuilderImpl::List(ListArrayBuilder::with_type(
                0,
                DataType::List {
                    datatype: Box::new(DataType::Int32),
                },
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
        let agg_kind = AggKind::Max;
        let input_type = DataType::Varchar;
        let return_type = DataType::Varchar;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
            let agg_kind = AggKind::Count;
            let input_type = DataType::Int32;
            let return_type = DataType::Int64;
            let actual = eval_agg(
                input_type,
                Arc::new(input),
                agg_kind,
                return_type,
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

    async fn eval_agg_distinct(
        input_type: DataType,
        input: ArrayRef,
        agg_kind: AggKind,
        return_type: DataType,
        mut builder: ArrayBuilderImpl,
    ) -> Result<ArrayImpl> {
        let len = input.len();
        let input_chunk = DataChunk::new(vec![Column::new(input)], len);
        let mut agg_state = crate::agg::build(AggCall {
            kind: agg_kind,
            args: AggArgs::Unary(input_type, 0),
            return_type,
            column_orders: vec![],
            filter: None,
            distinct: true,
        })?;
        agg_state
            .update_multi(&input_chunk, 0, input_chunk.cardinality())
            .await?;
        agg_state.output(&mut builder)?;
        Ok(builder.finish())
    }

    #[tokio::test]
    async fn vec_distinct_sum_int32() -> Result<()> {
        let input = I32Array::from_iter([1, 1, 3]);
        let agg_kind = AggKind::Sum;
        let input_type = DataType::Int32;
        let return_type = DataType::Int64;
        let actual = eval_agg_distinct(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
        let agg_kind = AggKind::Sum;
        let input_type = DataType::Int64;
        let return_type = DataType::Decimal;
        let actual = eval_agg_distinct(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
        let input = F32Array::from_iter([Some(1.0.into()), Some(2.0.into()), Some(3.0.into())]);
        let agg_kind = AggKind::Min;
        let input_type = DataType::Float32;
        let return_type = DataType::Float32;
        let actual = eval_agg_distinct(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
        let agg_kind = AggKind::Min;
        let input_type = DataType::Varchar;
        let return_type = DataType::Varchar;
        let actual = eval_agg_distinct(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
        let agg_kind = AggKind::Max;
        let input_type = DataType::Varchar;
        let return_type = DataType::Varchar;
        let actual = eval_agg_distinct(
            input_type,
            Arc::new(input.into()),
            agg_kind,
            return_type,
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
            let agg_kind = AggKind::Count;
            let input_type = DataType::Int32;
            let return_type = DataType::Int64;
            let actual = eval_agg_distinct(
                input_type,
                Arc::new(input),
                agg_kind,
                return_type,
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
