// Copyright 2026 RisingWave Labs
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

//! Randomized differential tests for [`HashJoinExecutor`].
//!
//! Each case joins two small random inputs on `l.k = r.k`, optionally with the non-equi condition
//! `l.v < r.v`, and compares the executor's output with a nested-loop reference. The inputs are
//! split into chunks of 1 to 4 rows and may contain invisible rows, and the executor runs with
//! several output chunk sizes, so a probe row's candidates often cross chunk boundaries. Keys and
//! values are drawn from tiny domains (with NULLs) to produce duplicate keys and partial matches.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::StreamExt;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::hash::Key32;
use risingwave_common::memory::MemoryContext;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::row::Row;
use risingwave_common::test_prelude::DataChunkTestExt;
use risingwave_common::types::DataType;
use risingwave_expr::expr::{BoxedExpression, build_from_pretty};

use super::{AsOfDesc, AsOfInequalityType, HashJoinExecutor, JoinType};
use crate::executor::test_utils::MockExecutor;
use crate::executor::{BoxedExecutor, Executor};
use crate::monitor::BatchSpillMetrics;
use crate::spill::spill_op::SpillBackend;
use crate::task::ShutdownToken;

const JOIN_TYPES: [JoinType; 8] = [
    JoinType::Inner,
    JoinType::LeftOuter,
    JoinType::RightOuter,
    JoinType::FullOuter,
    JoinType::LeftSemi,
    JoinType::LeftAnti,
    JoinType::RightSemi,
    JoinType::RightAnti,
];

const ASOF_INEQUALITIES: [AsOfInequalityType; 4] = [
    AsOfInequalityType::Lt,
    AsOfInequalityType::Le,
    AsOfInequalityType::Gt,
    AsOfInequalityType::Ge,
];

/// One input row: the join key, the value used by the non-equi condition, and its visibility.
#[derive(Clone, Copy)]
struct TestRow {
    key: Option<i32>,
    value: Option<i32>,
    visible: bool,
}

type Chunks = Vec<Vec<TestRow>>;

/// A small xorshift generator, so that every failure is reproducible from its seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }

    fn datum(&mut self, domain: u64) -> Option<i32> {
        (self.below(5) != 0).then(|| self.below(domain) as i32)
    }
}

fn gen_input(rng: &mut Rng, with_invisible_rows: bool) -> Chunks {
    let rows: Vec<_> = (0..rng.below(9))
        .map(|_| TestRow {
            key: rng.datum(3),
            value: rng.datum(4),
            visible: !(with_invisible_rows && rng.below(4) == 0),
        })
        .collect();
    let mut chunks = vec![];
    let mut rest = &rows[..];
    while !rest.is_empty() {
        let len = (1 + rng.below(4) as usize).min(rest.len());
        chunks.push(rest[..len].to_vec());
        rest = &rest[len..];
    }
    chunks
}

fn fmt_datum(datum: Option<i32>) -> String {
    datum.map_or_else(|| ".".to_owned(), |v| v.to_string())
}

fn fmt_row(row: &[Option<i32>]) -> String {
    row.iter()
        .map(|d| fmt_datum(*d))
        .collect::<Vec<_>>()
        .join(",")
}

fn fmt_input(chunks: &Chunks) -> String {
    chunks
        .iter()
        .map(|chunk| {
            chunk
                .iter()
                .map(|r| {
                    let invisible = if r.visible { "" } else { " invisible" };
                    format!("({},{}{invisible})", fmt_datum(r.key), fmt_datum(r.value))
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join(" | ")
}

fn mock_executor(chunks: &Chunks) -> BoxedExecutor {
    let schema = Schema::new(vec![
        Field::unnamed(DataType::Int32),
        Field::unnamed(DataType::Int32),
    ]);
    let mut executor = MockExecutor::new(schema);
    for chunk in chunks {
        let mut pretty = "i i".to_owned();
        for r in chunk {
            let invisible = if r.visible { "" } else { " D" };
            pretty += &format!("\n{} {}{invisible}", fmt_datum(r.key), fmt_datum(r.value));
        }
        executor.add(DataChunk::from_pretty(&pretty));
    }
    Box::new(executor)
}

fn visible_rows(chunks: &Chunks) -> Vec<TestRow> {
    chunks
        .iter()
        .flatten()
        .filter(|r| r.visible)
        .copied()
        .collect()
}

fn keys_match(l: Option<i32>, r: Option<i32>, null_safe: bool) -> bool {
    match (l, r) {
        (Some(l), Some(r)) => l == r,
        (None, None) => null_safe,
        _ => false,
    }
}

/// Nested-loop reference for every join type in [`JOIN_TYPES`], as sorted formatted rows.
fn reference_join(
    join_type: JoinType,
    left: &Chunks,
    right: &Chunks,
    with_cond: bool,
    null_safe: bool,
) -> Vec<String> {
    let left = visible_rows(left);
    let right = visible_rows(right);
    let matches = |l: &TestRow, r: &TestRow| {
        keys_match(l.key, r.key, null_safe)
            && (!with_cond || matches!((l.value, r.value), (Some(lv), Some(rv)) if lv < rv))
    };
    let left_matched = |l: &TestRow| right.iter().any(|r| matches(l, r));
    let right_matched = |r: &TestRow| left.iter().any(|l| matches(l, r));

    let mut out = vec![];
    match join_type {
        JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
            for l in &left {
                for r in right.iter().filter(|r| matches(l, r)) {
                    out.push(fmt_row(&[l.key, l.value, r.key, r.value]));
                }
            }
            if matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter) {
                for l in left.iter().filter(|l| !left_matched(l)) {
                    out.push(fmt_row(&[l.key, l.value, None, None]));
                }
            }
            if matches!(join_type, JoinType::RightOuter | JoinType::FullOuter) {
                for r in right.iter().filter(|r| !right_matched(r)) {
                    out.push(fmt_row(&[None, None, r.key, r.value]));
                }
            }
        }
        JoinType::LeftSemi | JoinType::LeftAnti => {
            let anti = join_type == JoinType::LeftAnti;
            for l in left.iter().filter(|l| left_matched(l) != anti) {
                out.push(fmt_row(&[l.key, l.value]));
            }
        }
        JoinType::RightSemi | JoinType::RightAnti => {
            let anti = join_type == JoinType::RightAnti;
            for r in right.iter().filter(|r| right_matched(r) != anti) {
                out.push(fmt_row(&[r.key, r.value]));
            }
        }
        JoinType::AsOfInner | JoinType::AsOfLeftOuter => unreachable!(),
    }
    out.sort();
    out
}

/// Reference for AS OF joins: each left row joins the closest right row with the same key that
/// satisfies `l.v <op> r.v`.
fn reference_asof_join(
    join_type: JoinType,
    inequality: &AsOfInequalityType,
    left: &Chunks,
    right: &Chunks,
    null_safe: bool,
) -> Vec<String> {
    let right = visible_rows(right);
    let mut out = vec![];
    for l in visible_rows(left) {
        let candidates = right.iter().filter_map(|r| {
            let (Some(lv), Some(rv)) = (l.value, r.value) else {
                return None;
            };
            let satisfied = match inequality {
                AsOfInequalityType::Lt => lv < rv,
                AsOfInequalityType::Le => lv <= rv,
                AsOfInequalityType::Gt => lv > rv,
                AsOfInequalityType::Ge => lv >= rv,
            };
            (keys_match(l.key, r.key, null_safe) && satisfied).then_some((rv, r))
        });
        let closest = match inequality {
            AsOfInequalityType::Lt | AsOfInequalityType::Le => candidates.min_by_key(|(rv, _)| *rv),
            AsOfInequalityType::Gt | AsOfInequalityType::Ge => candidates.max_by_key(|(rv, _)| *rv),
        };
        match closest {
            Some((_, r)) => out.push(fmt_row(&[l.key, l.value, r.key, r.value])),
            None if join_type == JoinType::AsOfLeftOuter => {
                out.push(fmt_row(&[l.key, l.value, None, None]))
            }
            None => {}
        }
    }
    out.sort();
    out
}

struct ExecutorConfig {
    join_type: JoinType,
    cond: Option<Arc<BoxedExpression>>,
    asof_desc: Option<AsOfDesc>,
    null_safe: bool,
    chunk_size: usize,
    spill: bool,
}

async fn run_executor(config: ExecutorConfig, left: &Chunks, right: &Chunks) -> Vec<String> {
    let output_len = match config.join_type {
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => 2,
        _ => 4,
    };
    let parent = MemoryContext::root(LabelGuardedIntGauge::test_int_gauge::<4>(), u64::MAX);
    let mem_ctx = if config.spill {
        // A zero memory limit makes the executor spill as soon as it reads the build side.
        MemoryContext::new_with_mem_limit(
            Some(parent),
            LabelGuardedIntGauge::test_int_gauge::<4>(),
            0,
        )
    } else {
        MemoryContext::new(Some(parent), LabelGuardedIntGauge::test_int_gauge::<4>())
    };
    let executor = Box::new(HashJoinExecutor::<Key32>::new(
        config.join_type,
        (0..output_len).collect(),
        mock_executor(left),
        mock_executor(right),
        vec![0],
        vec![0],
        vec![config.null_safe],
        config.cond,
        "HashJoinExecutor".to_owned(),
        config.chunk_size,
        config.asof_desc,
        config.spill.then_some(SpillBackend::Memory),
        BatchSpillMetrics::for_test(),
        ShutdownToken::empty(),
        mem_ctx,
    ));

    let mut out = vec![];
    let mut stream = executor.execute();
    while let Some(chunk) = stream.next().await {
        for row in chunk.unwrap().rows() {
            let row: Vec<_> = row.iter().map(|d| d.map(|s| s.into_int32())).collect();
            out.push(fmt_row(&row));
        }
    }
    out.sort();
    out
}

/// Collects mismatches, keeping the smallest failing case for each category.
#[derive(Default)]
struct Failures(BTreeMap<String, (usize, String)>);

impl Failures {
    fn check(
        &mut self,
        category: String,
        case: impl FnOnce() -> String,
        expected: &[String],
        actual: &[String],
    ) {
        if expected == actual {
            return;
        }
        let report = format!(
            "{}\n  expected: {expected:?}\n  actual:   {actual:?}",
            case()
        );
        let entry = self.0.entry(category).or_default();
        entry.0 += 1;
        if entry.1.is_empty() || report.len() < entry.1.len() {
            entry.1 = report;
        }
    }

    fn assert_empty(self) {
        if self.0.is_empty() {
            return;
        }
        let report: Vec<_> = self
            .0
            .into_iter()
            .map(|(category, (count, case))| {
                format!("[{category}] {count} failing cases, smallest:\n{case}")
            })
            .collect();
        panic!(
            "hash join differs from the reference:\n\n{}",
            report.join("\n\n")
        );
    }
}

async fn check_equi_joins(seeds: u64, with_invisible_rows: bool, spill: bool) {
    let mut failures = Failures::default();
    for seed in 1..=seeds {
        let mut rng = Rng::new(seed);
        let left = gen_input(&mut rng, with_invisible_rows);
        let right = gen_input(&mut rng, with_invisible_rows);
        for join_type in JOIN_TYPES {
            for with_cond in [false, true] {
                for null_safe in [false, true] {
                    let expected = reference_join(join_type, &left, &right, with_cond, null_safe);
                    for chunk_size in [1, 2, 3, 1024] {
                        let config = ExecutorConfig {
                            join_type,
                            cond: with_cond.then(|| {
                                build_from_pretty("(less_than:boolean $1:int4 $3:int4)").into()
                            }),
                            asof_desc: None,
                            null_safe,
                            chunk_size,
                            spill,
                        };
                        let actual = run_executor(config, &left, &right).await;
                        failures.check(
                            format!("{join_type:?} cond={with_cond}"),
                            || {
                                format!(
                                    "seed={seed} null_safe={null_safe} chunk_size={chunk_size} spill={spill}\n  left:  {}\n  right: {}",
                                    fmt_input(&left),
                                    fmt_input(&right)
                                )
                            },
                            &expected,
                            &actual,
                        );
                    }
                }
            }
        }
    }
    failures.assert_empty();
}

#[tokio::test]
async fn test_hash_join_matches_nested_loop() {
    check_equi_joins(300, false, false).await;
}

#[tokio::test]
async fn test_hash_join_matches_nested_loop_with_invisible_rows() {
    check_equi_joins(300, true, false).await;
}

#[tokio::test]
async fn test_hash_join_matches_nested_loop_with_spill() {
    check_equi_joins(60, true, true).await;
}

#[tokio::test]
async fn test_asof_join_matches_reference() {
    let mut failures = Failures::default();
    for seed in 1..=200 {
        let mut rng = Rng::new(seed);
        let left = gen_input(&mut rng, true);
        let right = gen_input(&mut rng, true);
        for join_type in [JoinType::AsOfInner, JoinType::AsOfLeftOuter] {
            for inequality in ASOF_INEQUALITIES {
                for null_safe in [false, true] {
                    let expected =
                        reference_asof_join(join_type, &inequality, &left, &right, null_safe);
                    for (chunk_size, spill) in [(1, false), (3, false), (1024, false), (3, true)] {
                        let op = match inequality {
                            AsOfInequalityType::Lt => "less_than",
                            AsOfInequalityType::Le => "less_than_or_equal",
                            AsOfInequalityType::Gt => "greater_than",
                            AsOfInequalityType::Ge => "greater_than_or_equal",
                        };
                        let config = ExecutorConfig {
                            join_type,
                            cond: Some(
                                build_from_pretty(format!("({op}:boolean $1:int4 $3:int4)")).into(),
                            ),
                            asof_desc: Some(AsOfDesc {
                                left_idx: 1,
                                right_idx: 1,
                                inequality_type: inequality.clone(),
                            }),
                            null_safe,
                            chunk_size,
                            spill,
                        };
                        let actual = run_executor(config, &left, &right).await;
                        failures.check(
                            format!("{join_type:?} {inequality:?}"),
                            || {
                                format!(
                                    "seed={seed} null_safe={null_safe} chunk_size={chunk_size} spill={spill}\n  left:  {}\n  right: {}",
                                    fmt_input(&left),
                                    fmt_input(&right)
                                )
                            },
                            &expected,
                            &actual,
                        );
                    }
                }
            }
        }
    }
    failures.assert_empty();
}
