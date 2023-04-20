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

use num_traits::CheckedAdd;

use crate::{ExprError, Result};

#[aggregate("sum(int16) -> int64")]
#[aggregate("sum(int32) -> int64")]
#[aggregate("sum(int64) -> int64")]
#[aggregate("sum(int64) -> decimal")]
#[aggregate("sum(float32) -> float32")]
#[aggregate("sum(float64) -> float64")]
#[aggregate("sum(decimal) -> decimal")]
#[aggregate("sum(interval) -> interval")]
fn sum<R, T>(result: Option<R>, input: Option<T>) -> Result<Option<R>>
where
    R: From<T> + CheckedAdd<Output = R> + Copy,
{
    Ok(match (result, input) {
        (_, None) => result,
        (None, Some(i)) => Some(R::from(i)),
        (Some(r), Some(i)) => r
            .checked_add(&R::from(i))
            .map_or(Err(ExprError::NumericOutOfRange), |x| Ok(Some(x)))?,
    })
}

#[aggregate("min(*) -> auto")]
fn min<T: Ord>(result: Option<T>, input: Option<T>) -> Option<T> {
    match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(r.min(i)),
    }
}

#[aggregate("max(*) -> auto")]
fn max<T: Ord>(result: Option<T>, input: Option<T>) -> Option<T> {
    match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(r.max(i)),
    }
}

#[aggregate("first_value(*) -> auto")]
fn first<T>(result: Option<T>, input: Option<T>) -> Option<T> {
    result.or(input)
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
#[aggregate("count(*) -> int64")]
fn count<T>(result: Option<i64>, input: Option<T>) -> Option<i64> {
    match (result, input) {
        (None, None) => Some(0),
        (Some(r), None) => Some(r),
        (None, Some(_)) => Some(1),
        (Some(r), Some(_)) => Some(r + 1),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::types::Decimal;

    use super::*;
    use crate::function::aggregate::AggKind;
    use crate::vector_op::agg::aggregator::create_agg_state_unary;

    async fn eval_agg(
        input_type: DataType,
        input: ArrayRef,
        agg_kind: AggKind,
        return_type: DataType,
        mut builder: ArrayBuilderImpl,
    ) -> Result<ArrayImpl> {
        let len = input.len();
        let input_chunk = DataChunk::new(vec![Column::new(input)], len);
        let mut agg_state = create_agg_state_unary(input_type, 0, agg_kind, return_type, false)?;
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
        use risingwave_common::array;
        let input = ListArray::from_iter(
            [
                Some(array! { I32Array, [Some(0)] }.into()),
                Some(array! { I32Array, [Some(1)] }.into()),
                Some(array! { I32Array, [Some(2)] }.into()),
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
            ArrayBuilderImpl::List(ListArrayBuilder::with_meta(
                0,
                ArrayMeta::List {
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
                val: &ListValue::new(vec![Some(ScalarImpl::Int32(0))])
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
        let mut agg_state = create_agg_state_unary(input_type, 0, agg_kind, return_type, true)?;
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
        let expected = &[None];
        test_case(input.into(), expected).await?;
        let input = I32Array::from_iter([None]);
        let expected = &[Some(0)];
        test_case(input.into(), expected).await
    }
}
