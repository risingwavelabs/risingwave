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

use num_traits::{CheckedAdd, CheckedSub};
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
fn sum<S, T>(state: S, input: T, retract: bool) -> Result<S>
where
    S: From<T> + CheckedAdd<Output = S> + CheckedSub<Output = S>,
{
    if retract {
        state
            .checked_sub(&S::from(input))
            .ok_or(ExprError::NumericOutOfRange)
    } else {
        state
            .checked_add(&S::from(input))
            .ok_or(ExprError::NumericOutOfRange)
    }
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
fn bit_xor<T>(state: T, input: T, _retract: bool) -> T
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
fn count<T>(state: i64, _: T, retract: bool) -> i64 {
    if retract {
        state - 1
    } else {
        state + 1
    }
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

    use futures_util::FutureExt;
    use risingwave_common::array::*;
    use risingwave_common::types::{DataType, Datum, Decimal};

    use crate::agg::AggCall;

    fn eval_agg(pretty: &str, input: impl Array) -> Datum {
        let input = Arc::new(input.into());
        let len = input.len();
        let input_chunk = DataChunk::new(vec![input], len).into();
        let mut agg_state = crate::agg::build(AggCall::from_pretty(pretty)).unwrap();
        agg_state
            .update_multi(&input_chunk, 0, input_chunk.cardinality())
            .now_or_never()
            .unwrap()
            .unwrap();
        agg_state.output().unwrap()
    }

    #[test]
    fn sum_int32() {
        let input = I32Array::from_iter([1, 2, 3]);
        let actual = eval_agg("(sum:int8 $0:int4)", input);
        assert_eq!(actual, Some(6i64.into()));
    }

    #[test]
    fn sum_int64() {
        let input = I64Array::from_iter([1, 2, 3]);
        let actual = eval_agg("(sum:decimal $0:int8)", input);
        assert_eq!(actual, Some(Decimal::from(6).into()));
    }

    #[test]
    fn min_float32() {
        let input = F32Array::from_iter([1.0, 2.0, 3.0]);
        let actual = eval_agg("(min:float4 $0:float4)", input);
        assert_eq!(actual, Some(1.0f32.into()));
    }

    #[test]
    fn min_char() {
        let input = Utf8Array::from_iter(["b", "aa"]);
        let actual = eval_agg("(min:varchar $0:varchar)", input);
        assert_eq!(actual, Some("aa".into()));
    }

    #[test]
    fn min_list() {
        let input = ListArray::from_iter(
            [
                Some(I32Array::from_iter([Some(0)]).into()),
                Some(I32Array::from_iter([Some(1)]).into()),
                Some(I32Array::from_iter([Some(2)]).into()),
            ],
            DataType::Int32,
        );
        let actual = eval_agg("(min:int4[] $0:int4[])", input);
        assert_eq!(actual, Some(ListValue::new(vec![Some(0i32.into())]).into()));
    }

    #[test]
    fn max_char() {
        let input = Utf8Array::from_iter(["b", "aa"]);
        let actual = eval_agg("(max:varchar $0:varchar)", input);
        assert_eq!(actual, Some("b".into()));
    }

    #[test]
    fn count_int32() {
        fn test_case(input: impl Array, expected: i64) {
            let actual = eval_agg("(count:int8 $0:int4)", input);
            assert_eq!(actual, Some(expected.into()));
        }
        test_case(I32Array::from_iter([1, 2, 3]), 3);
        test_case(I32Array::from_iter([0; 0]), 0);
        test_case(I32Array::from_iter([None]), 0);
    }

    #[test]
    fn distinct_sum_int32() {
        let input = I32Array::from_iter([1, 1, 3]);
        let actual = eval_agg("(sum:int8 $0:int4 distinct)", input);
        assert_eq!(actual, Some(4i64.into()));
    }

    #[test]
    fn distinct_sum_int64() {
        let input = I64Array::from_iter([1, 1, 3]);
        let actual = eval_agg("(sum:decimal $0:int8 distinct)", input);
        assert_eq!(actual, Some(Decimal::from(4).into()));
    }

    #[test]
    fn distinct_min_float32() {
        let input = F32Array::from_iter([1.0, 2.0, 3.0]);
        let actual = eval_agg("(min:float4 $0:float4 distinct)", input);
        assert_eq!(actual, Some(1.0f32.into()));
    }

    #[test]
    fn distinct_min_char() {
        let input = Utf8Array::from_iter(["b", "aa"]);
        let actual = eval_agg("(min:varchar $0:varchar distinct)", input);
        assert_eq!(actual, Some("aa".into()));
    }

    #[test]
    fn distinct_max_char() {
        let input = Utf8Array::from_iter(["b", "aa"]);
        let actual = eval_agg("(max:varchar $0:varchar distinct)", input);
        assert_eq!(actual, Some("b".into()));
    }

    #[test]
    fn distinct_count_int32() {
        fn test_case(input: impl Array, expected: i64) {
            let actual = eval_agg("(count:int8 $0:int4 distinct)", input);
            assert_eq!(actual, Some(expected.into()));
        }
        test_case(I32Array::from_iter([1, 1, 3]), 2);
        test_case(I32Array::from_iter([0; 0]), 0);
        test_case(I32Array::from_iter([None]), 0);
    }
}
