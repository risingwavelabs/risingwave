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

//! Per-arrival cost of `MatchRecognizeExecutor` with `DEFINE` predicates, in the two shapes a
//! long partition buffer takes.
//!
//! `PATTERN (a+ b) DEFINE a AS v > T, b AS v < 0` over `R` rows with `v = 1`:
//!
//! - **pending run** (`T = 0`): every row is an `a` and no `b` ever arrives, so every start stays
//!   alive and each arrival rescans all `R` pending starts. A start walks the run to the boundary
//!   and back, asking `a` and then `b` at every row it passes — `Θ(R²)` predicate evaluations per
//!   arrival, each an expression-tree evaluation over a synthetic row. The shape the design doc
//!   calls inherently per-visit, and the shape of "failed attempts then a success" over a user
//!   with many attempts. `arrival/1` is the fixed per-arrival cost (chunk, barrier, drain) to
//!   subtract; `walk_only/R` drives the automaton directly over a trivial matcher, so
//!   `arrival/R − arrival/1 − walk_only/R` is what the executor's predicate path adds per arrival.
//! - **matchless run** (`T = 100`): every row fails `a` at once, so every start is matchless
//!   forever and an arrival's rescan is `O(1)` — but the dead rows stay in the buffer until a
//!   watermark's prune, so the buffer is long while the work is small. Anything the executor does
//!   per arrival in proportion to the buffer shows here and nowhere else.
//!
//! The executor is reloaded before every timed arrival so the buffer is exactly `R` rows;
//! reported per arrival. The pending run is settled with one watermark outside the timer (the
//! arrival's structural prune visit, and a re-derivation under a fresh budget after the load's
//! budget-truncated chunk); the matchless run is not settled, since the prune would empty it.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::test_epoch;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_expr::expr::test_utils::{make_func_call, make_input_ref};
use risingwave_pb::data::PbDataType;
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::expr::ExprNode;
use risingwave_pb::expr::expr_node::{RexNode, Type as PbExprType};
use risingwave_pb::stream_plan::match_recognize_define_slot::Kind as SlotKind;
use risingwave_pb::stream_plan::{
    MatchRecognizeDefine as PbDefine, MatchRecognizeDefineSlot as PbSlot,
};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::common::table::test_utils::gen_pbtable_with_dist_key;
use risingwave_stream::executor::match_recognize::executor::{
    CompiledDefine, MatchRecognizeExecutor, MatchRecognizeExecutorArgs,
};
use risingwave_stream::executor::match_recognize::nfa::{
    CandidateMatcher, MatchScan, Nfa, Pattern, Quantifier, ScanBudget, SkipMode,
};
use risingwave_stream::executor::test_utils::{MessageSender, MockSource};
use risingwave_stream::executor::{
    ActorContext, ActorContextRef, BoxedMessageStream, Execute, Message, StreamExecutorResult,
};
use risingwave_stream::task::ActorEvalErrorReport;
use tokio::runtime::Runtime;

risingwave_expr_impl::enable!();

/// The epoch `load_run` leaves the executor at; the timed arrival is the next one.
const LOADED_EPOCH: u64 = 3;
/// `a AS v > T` over rows with `v = 1`: every row is an `a`.
const PENDING: i64 = 0;
/// `a AS v > T` over rows with `v = 1`: no row is an `a`.
const MATCHLESS: i64 = 100;

/// Input: `(partition int8, ts int8, v int8)`; `PARTITION BY partition ORDER BY ts`.
fn input_types() -> Vec<DataType> {
    vec![DataType::Int64, DataType::Int64, DataType::Int64]
}

fn int8_literal(v: i64) -> ExprNode {
    ExprNode {
        function_type: PbExprType::Unspecified as i32,
        return_type: Some(PbDataType {
            type_name: TypeName::Int64 as i32,
            ..Default::default()
        }),
        rex_node: Some(RexNode::Constant(Some(ScalarImpl::Int64(v)).to_protobuf())),
    }
}

/// `DEFINE <symbol> AS <symbol>.v <op> <literal>`, compiled through the real proto lowering so
/// the slot kind is the planner's: one `SelfCol` slot over the input's `v` column.
fn define(
    symbol: &str,
    op: PbExprType,
    literal: i64,
    report: ActorEvalErrorReport,
) -> CompiledDefine {
    let pb = PbDefine {
        symbol: symbol.to_owned(),
        condition: Some(make_func_call(
            op,
            TypeName::Boolean,
            vec![make_input_ref(0, TypeName::Int64), int8_literal(literal)],
        )),
        slots: vec![PbSlot {
            kind: SlotKind::SelfCol as i32,
            vars: vec![],
            col_idx: 2,
            offset: 0,
        }],
    };
    CompiledDefine::from_protobuf(&pb, report).unwrap()
}

/// `(a+ b)`.
fn pattern() -> Nfa {
    Nfa::compile(&Pattern::Concat(vec![
        Pattern::Quantified(
            Box::new(Pattern::Var("a".to_owned())),
            Quantifier::Plus,
            false,
        ),
        Pattern::Var("b".to_owned()),
    ]))
}

/// Build the executor over an in-memory state store and drive it through its first barrier.
/// Returns the actor context too, for reading the executor's metrics.
async fn build(
    store: MemoryStateStore,
    a_threshold: i64,
) -> (MessageSender, BoxedMessageStream, ActorContextRef) {
    let input_schema = Schema::new(input_types().into_iter().map(Field::unnamed).collect());
    // Output: the partition column, then the hidden per-match id (no MEASURES).
    let output_schema = Schema::new(vec![
        Field::with_name(DataType::Int64, "partition_0"),
        Field::with_name(DataType::Int64, "_match_id"),
    ]);
    // State table: `seq` then the raw input columns (offset 1); key = partition, ts, seq — the
    // layout `StreamMatchRecognize::infer_state_table` produces.
    let table_columns = (0..4)
        .map(|i| ColumnDesc::unnamed(ColumnId::new(i), DataType::Int64))
        .collect();
    let state_table = StateTable::from_table_catalog(
        &gen_pbtable_with_dist_key(
            TableId::new(1),
            table_columns,
            vec![OrderType::ascending(); 3],
            vec![1, 2, 0],
            0,
            vec![1],
        ),
        store,
        Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into()),
    )
    .await;

    let ctx = ActorContext::for_test(1);
    let report = ActorEvalErrorReport {
        actor_context: ctx.clone(),
        identity: Arc::from("bench MatchRecognize"),
    };
    let defines = vec![
        define("a", PbExprType::GreaterThan, a_threshold, report.clone()),
        define("b", PbExprType::LessThan, 0, report.clone()),
    ];

    let (mut tx, source) = MockSource::channel();
    let source = source.into_executor(input_schema, vec![0, 1]);
    let executor = MatchRecognizeExecutor::new(MatchRecognizeExecutorArgs {
        ctx: ctx.clone(),
        input: source,
        schema: output_schema,
        chunk_size: 1024,
        partition_key_indices: vec![0],
        order_key_indices: vec![1],
        measures: vec![],
        defines,
        within: None,
        within_deadline: None,
        nfa: pattern(),
        skip: SkipMode::PastLastRow,
        eval_error_report: report,
        state_table,
    });
    let mut stream = executor.boxed().execute();

    tx.push_barrier(test_epoch(1), false);
    drain_until_barrier(&mut stream, test_epoch(1)).await;
    (tx, stream, ctx)
}

/// Rows `[from, to)` of partition 0, one per order key, all `v = 1`.
fn run_chunk(from: i64, to: i64) -> StreamChunk {
    let rows: Vec<(Op, OwnedRow)> = (from..to)
        .map(|ts| {
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(0)),
                    Some(ScalarImpl::Int64(ts)),
                    Some(ScalarImpl::Int64(1)),
                ]),
            )
        })
        .collect();
    StreamChunk::from_rows(&rows, &input_types())
}

async fn drain_until_barrier(stream: &mut BoxedMessageStream, epoch: u64) {
    while let Some(msg) = stream.next().await {
        match msg.unwrap() {
            Message::Barrier(b) if b.epoch.curr == epoch => return,
            other => {
                black_box(other);
            }
        }
    }
    panic!("stream ended before barrier {epoch}");
}

/// Load `r` rows into partition 0 and, if `settle`, visit it once with a watermark outside the
/// timer. Leaves the executor at `LOADED_EPOCH`.
async fn load_run(
    r: i64,
    a_threshold: i64,
    settle: bool,
) -> (MessageSender, BoxedMessageStream, ActorContextRef) {
    let (mut tx, mut stream, ctx) = build(MemoryStateStore::new(), a_threshold).await;
    tx.push_chunk(run_chunk(0, r));
    tx.push_barrier_with_prev_epoch_for_test(test_epoch(2), test_epoch(1), false);
    drain_until_barrier(&mut stream, test_epoch(2)).await;
    if settle {
        tx.push_int64_watermark(1, 0);
    }
    tx.push_barrier_with_prev_epoch_for_test(test_epoch(LOADED_EPOCH), test_epoch(2), false);
    drain_until_barrier(&mut stream, test_epoch(LOADED_EPOCH)).await;
    (tx, stream, ctx)
}

/// One more row at the end of the run, fenced by a barrier. Returns the elapsed time of exactly
/// that, and asserts the visit stayed inside its scan budget: a truncated rescan would time the
/// budget, not the work. (The counter is cumulative and shared by every executor built with the
/// same labels, and the load's single R-row chunk does exhaust a budget at larger R — that is
/// outside the timer and re-derived by the settle — so only the delta over the arrival counts.)
async fn arrival(
    mut tx: MessageSender,
    mut stream: BoxedMessageStream,
    ctx: ActorContextRef,
    r: i64,
) -> Duration {
    let exhausted_count = || {
        ctx.streaming_metrics
            .new_match_recognize_metrics(TableId::new(1), ctx.id, ctx.fragment_id)
            .match_recognize_scan_budget_exhausted_count
            .get()
    };
    let before = exhausted_count();
    let start = Instant::now();
    tx.push_chunk(run_chunk(r, r + 1));
    tx.push_barrier_with_prev_epoch_for_test(
        test_epoch(LOADED_EPOCH + 1),
        test_epoch(LOADED_EPOCH),
        false,
    );
    drain_until_barrier(&mut stream, test_epoch(LOADED_EPOCH + 1)).await;
    let elapsed = start.elapsed();
    assert_eq!(
        exhausted_count(),
        before,
        "the timed arrival must stay inside the scan budget (R too large)"
    );
    drop(tx);
    drop(stream);
    elapsed
}

/// The walk alone: `a` at every row, `b` nowhere, no expression evaluation and no executor.
struct AllA;

impl CandidateMatcher for AllA {
    async fn matches(
        &self,
        var: &str,
        _pos: usize,
        _labels: &[String],
    ) -> StreamExecutorResult<bool> {
        Ok(var == "a")
    }
}

/// One finder pass over a run of `r` all-`a` rows from every start — the automaton's share of
/// `arrival/R` on the pending run, with a matcher that costs one byte compare per question.
async fn walk_only(nfa: &Nfa, r: usize) {
    let mut scan = MatchScan::starting_at(0);
    let mut budget = ScanBudget::unlimited();
    let found = nfa
        .next_match(
            &mut scan,
            r,
            &AllA,
            &SkipMode::PastLastRow,
            &mut budget,
            true,
        )
        .await
        .unwrap();
    assert!(found.is_none(), "no `b`, no match");
}

fn bench_pending_run(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("match_recognize_pending_run");
    // The reload before every timed arrival dominates wall-clock; criterion sizes the iteration
    // count from the timed part alone, so keep the windows short.
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    // A start pays about six budget units per row it passes (two predicate charges, four ε
    // steps), so an arrival costs about 3R² units: 786k at R = 512 against the 2^20 budget.
    // `arrival` asserts the budget was not exhausted.
    for &r in &[1i64, 64, 256, 512] {
        group.bench_with_input(BenchmarkId::new("arrival", r), &r, |b, &r| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (tx, stream, ctx) = load_run(r, PENDING, true).await;
                    total += arrival(tx, stream, ctx, r).await;
                }
                total
            })
        });
    }
    let nfa = pattern();
    for &r in &[64usize, 256, 512] {
        group.bench_with_input(BenchmarkId::new("walk_only", r), &r, |b, &r| {
            b.to_async(&rt).iter(|| walk_only(&nfa, r))
        });
    }
    group.finish();
}

fn bench_matchless_run(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("match_recognize_matchless_run");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    for &r in &[1_000i64, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::new("arrival", r), &r, |b, &r| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (tx, stream, ctx) = load_run(r, MATCHLESS, false).await;
                    total += arrival(tx, stream, ctx, r).await;
                }
                total
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_pending_run, bench_matchless_run);
criterion_main!(benches);
