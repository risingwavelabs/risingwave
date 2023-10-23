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

use num_traits::{CheckedAdd, CheckedSub};
use risingwave_expr::{aggregate, ExprError, Result};

#[aggregate("sum(int2) -> int8")]
#[aggregate("sum(int4) -> int8")]
#[aggregate("sum(int8) -> int8")]
#[aggregate("sum(int8) -> decimal")]
#[aggregate("sum(float4) -> float4")]
#[aggregate("sum(float8) -> float8")]
#[aggregate("sum(decimal) -> decimal")]
#[aggregate("sum(interval) -> interval")]
#[aggregate("sum(int256) -> int256")]
#[aggregate("sum0(int8) -> int8", init_state = "0i64")]
fn sum<S, T>(state: S, input: T, retract: bool) -> Result<S>
where
    S: Default + From<T> + CheckedAdd<Output = S> + CheckedSub<Output = S>,
{
    if retract {
        state
            .checked_sub(&S::from(input))
            .ok_or_else(|| ExprError::NumericOutOfRange)
    } else {
        state
            .checked_add(&S::from(input))
            .ok_or_else(|| ExprError::NumericOutOfRange)
    }
}

#[aggregate("min(*) -> auto", state = "ref")]
fn min<T: Ord>(state: T, input: T) -> T {
    state.min(input)
}

#[aggregate("max(*) -> auto", state = "ref")]
fn max<T: Ord>(state: T, input: T) -> T {
    state.max(input)
}

#[aggregate("first_value(*) -> auto", state = "ref")]
fn first_value<T>(state: T, _: T) -> T {
    state
}

#[aggregate("last_value(*) -> auto", state = "ref")]
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
#[aggregate("count(*) -> int8", init_state = "0i64")]
fn count<T>(state: i64, _: T, retract: bool) -> i64 {
    if retract {
        state - 1
    } else {
        state + 1
    }
}

#[aggregate("count() -> int8", init_state = "0i64")]
fn count_star(state: i64, retract: bool) -> i64 {
    if retract {
        state - 1
    } else {
        state + 1
    }
}

#[cfg(test)]
mod tests {
    extern crate test;

    use std::sync::Arc;

    use futures_util::FutureExt;
    use risingwave_common::array::*;
    use risingwave_common::test_utils::{rand_bitmap, rand_stream_chunk};
    use risingwave_common::types::{Datum, Decimal};
    use risingwave_expr::aggregate::{build_append_only, AggCall};
    use test::Bencher;

    fn test_agg(pretty: &str, input: StreamChunk, expected: Datum) {
        let agg = build_append_only(&AggCall::from_pretty(pretty)).unwrap();
        let mut state = agg.create_state();
        agg.update(&mut state, &input)
            .now_or_never()
            .unwrap()
            .unwrap();
        let actual = agg.get_result(&state).now_or_never().unwrap().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn sum_int4() {
        let input = StreamChunk::from_pretty(
            " i
            + 3
            - 1
            - 3 D
            + 1 D",
        );
        test_agg("(sum:int8 $0:int4)", input, Some(2i64.into()));
    }

    #[test]
    fn sum_int8() {
        let input = StreamChunk::from_pretty(
            " I
            + 3
            - 1
            - 3 D
            + 1 D",
        );
        test_agg(
            "(sum:decimal $0:int8)",
            input,
            Some(Decimal::from(2).into()),
        );
    }

    #[test]
    fn sum_float8() {
        let input = StreamChunk::from_pretty(
            " F
            + 1.0
            + 2.0
            + 3.0
            - 4.0",
        );
        test_agg("(sum:float8 $0:float8)", input, Some(2.0f64.into()));

        let input = StreamChunk::from_pretty(
            " F
            + 1.0
            + inf
            + 3.0
            - 3.0",
        );
        test_agg("(sum:float8 $0:float8)", input, Some(f64::INFINITY.into()));

        let input = StreamChunk::from_pretty(
            " F
            + 0.0
            - -inf",
        );
        test_agg("(sum:float8 $0:float8)", input, Some(f64::INFINITY.into()));

        let input = StreamChunk::from_pretty(
            " F
            + 1.0
            + nan
            + 1926.0",
        );
        test_agg("(sum:float8 $0:float8)", input, Some(f64::NAN.into()));
    }

    /// Even if there is no element after some insertions and equal number of deletion operations,
    /// sum `AggregateFunction` should output `0` instead of `None`.
    #[test]
    fn sum_no_none() {
        test_agg("(sum:int8 $0:int8)", StreamChunk::from_pretty("I"), None);

        let input = StreamChunk::from_pretty(
            " I
            + 2
            - 1
            + 1
            - 2",
        );
        test_agg("(sum:int8 $0:int8)", input, Some(0i64.into()));

        let input = StreamChunk::from_pretty(
            " I
            - 3 D
            + 1
            - 3 D
            - 1",
        );
        test_agg("(sum:int8 $0:int8)", input, Some(0i64.into()));
    }

    #[test]
    fn min_int8() {
        let input = StreamChunk::from_pretty(
            " I
            + 1  D
            + 10
            + .
            + 5",
        );
        test_agg("(min:int8 $0:int8)", input, Some(5i64.into()));
    }

    #[test]
    fn min_float4() {
        let input = StreamChunk::from_pretty(
            " f
            + 1.0  D
            + 10.0
            + .
            + 5.0",
        );
        test_agg("(min:float4 $0:float4)", input, Some(5.0f32.into()));
    }

    #[test]
    fn min_char() {
        let input = StreamChunk::from_pretty(
            " T
            + b
            + aa",
        );
        test_agg("(min:varchar $0:varchar)", input, Some("aa".into()));
    }

    #[test]
    fn min_list() {
        let input = StreamChunk::from_pretty(
            " i[]
            + {0}
            + {1}
            + {2}",
        );
        test_agg(
            "(min:int4[] $0:int4[])",
            input,
            Some(ListValue::new(vec![Some(0i32.into())]).into()),
        );
    }

    #[test]
    fn max_int8() {
        let input = StreamChunk::from_pretty(
            " I
            + 1
            + 10 D
            + .
            + 5",
        );
        test_agg("(max:int8 $0:int8)", input, Some(5i64.into()));
    }

    #[test]
    fn max_char() {
        let input = StreamChunk::from_pretty(
            " T
            + b
            + aa",
        );
        test_agg("(max:varchar $0:varchar)", input, Some("b".into()));
    }

    #[test]
    fn count_int4() {
        let input = StreamChunk::from_pretty(
            " i
            + 1
            + 2
            + 3",
        );
        test_agg("(count:int8 $0:int4)", input, Some(3i64.into()));

        let input = StreamChunk::from_pretty(
            " i
            + 1
            + .
            + 3
            - 1",
        );
        test_agg("(count:int8 $0:int4)", input, Some(1i64.into()));

        let input = StreamChunk::from_pretty(
            " i
            - 1 D
            - .
            - 3 D
            - 1 D",
        );
        test_agg("(count:int8 $0:int4)", input, Some(0i64.into()));

        let input = StreamChunk::from_pretty("i");
        test_agg("(count:int8 $0:int4)", input, Some(0i64.into()));

        let input = StreamChunk::from_pretty(
            " i
            + .",
        );
        test_agg("(count:int8 $0:int4)", input, Some(0i64.into()));
    }

    #[test]
    fn count_star() {
        // when there is no element, output should be `0`.
        let input = StreamChunk::from_pretty("i");
        test_agg("(count:int8)", input, Some(0i64.into()));

        // insert one element to state
        let input = StreamChunk::from_pretty(
            " i
            + 0",
        );
        test_agg("(count:int8)", input, Some(1i64.into()));

        // delete one element from state
        let input = StreamChunk::from_pretty(
            " i
            + 0
            - 0",
        );
        test_agg("(count:int8)", input, Some(0i64.into()));

        let input = StreamChunk::from_pretty(
            " i
            - 0
            - 0 D
            + 1
            - 1",
        );
        test_agg("(count:int8)", input, Some((-1i64).into()));
    }

    #[test]
    fn bitxor_int8() {
        let input = StreamChunk::from_pretty(
            " I
            + 1
            - 10 D
            + .
            - 5",
        );
        test_agg("(bit_xor:int8 $0:int8)", input, Some(4i64.into()));
    }

    fn bench_i64(
        b: &mut Bencher,
        agg_desc: &str,
        chunk_size: usize,
        vis_rate: f64,
        append_only: bool,
    ) {
        println!(
            "benching {} agg, chunk_size={}, vis_rate={}",
            agg_desc, chunk_size, vis_rate
        );
        let vis =
            rand_bitmap::gen_rand_bitmap(chunk_size, (chunk_size as f64 * vis_rate) as usize, 666);
        let (ops, data) =
            rand_stream_chunk::gen_legal_stream_chunk(&vis, chunk_size, append_only, 666);
        let chunk = StreamChunk::from_parts(ops, DataChunk::new(vec![Arc::new(data)], vis));
        let pretty = format!("({agg_desc}:int8 $0:int8)");
        let agg = build_append_only(&AggCall::from_pretty(pretty)).unwrap();
        let mut state = agg.create_state();
        b.iter(|| {
            agg.update(&mut state, &chunk)
                .now_or_never()
                .unwrap()
                .unwrap();
        });
    }

    #[bench]
    fn sum_agg_without_vis(b: &mut Bencher) {
        bench_i64(b, "sum", 1024, 1.0, false);
    }

    #[bench]
    fn sum_agg_vis_rate_0_75(b: &mut Bencher) {
        bench_i64(b, "sum", 1024, 0.75, false);
    }

    #[bench]
    fn sum_agg_vis_rate_0_5(b: &mut Bencher) {
        bench_i64(b, "sum", 1024, 0.5, false);
    }

    #[bench]
    fn sum_agg_vis_rate_0_25(b: &mut Bencher) {
        bench_i64(b, "sum", 1024, 0.25, false);
    }

    #[bench]
    fn sum_agg_vis_rate_0_05(b: &mut Bencher) {
        bench_i64(b, "sum", 1024, 0.05, false);
    }

    #[bench]
    fn count_agg_without_vis(b: &mut Bencher) {
        bench_i64(b, "count", 1024, 1.0, false);
    }

    #[bench]
    fn count_agg_vis_rate_0_75(b: &mut Bencher) {
        bench_i64(b, "count", 1024, 0.75, false);
    }

    #[bench]
    fn count_agg_vis_rate_0_5(b: &mut Bencher) {
        bench_i64(b, "count", 1024, 0.5, false);
    }

    #[bench]
    fn count_agg_vis_rate_0_25(b: &mut Bencher) {
        bench_i64(b, "count", 1024, 0.25, false);
    }

    #[bench]
    fn count_agg_vis_rate_0_05(b: &mut Bencher) {
        bench_i64(b, "count", 1024, 0.05, false);
    }

    #[bench]
    fn min_agg_without_vis(b: &mut Bencher) {
        bench_i64(b, "min", 1024, 1.0, true);
    }

    #[bench]
    fn min_agg_vis_rate_0_75(b: &mut Bencher) {
        bench_i64(b, "min", 1024, 0.75, true);
    }

    #[bench]
    fn min_agg_vis_rate_0_5(b: &mut Bencher) {
        bench_i64(b, "min", 1024, 0.5, true);
    }

    #[bench]
    fn min_agg_vis_rate_0_25(b: &mut Bencher) {
        bench_i64(b, "min", 1024, 0.25, true);
    }

    #[bench]
    fn min_agg_vis_rate_0_05(b: &mut Bencher) {
        bench_i64(b, "min", 1024, 0.05, true);
    }

    #[bench]
    fn max_agg_without_vis(b: &mut Bencher) {
        bench_i64(b, "max", 1024, 1.0, true);
    }

    #[bench]
    fn max_agg_vis_rate_0_75(b: &mut Bencher) {
        bench_i64(b, "max", 1024, 0.75, true);
    }

    #[bench]
    fn max_agg_vis_rate_0_5(b: &mut Bencher) {
        bench_i64(b, "max", 1024, 0.5, true);
    }

    #[bench]
    fn max_agg_vis_rate_0_25(b: &mut Bencher) {
        bench_i64(b, "max", 1024, 0.25, true);
    }

    #[bench]
    fn max_agg_vis_rate_0_05(b: &mut Bencher) {
        bench_i64(b, "max", 1024, 0.05, true);
    }
}
