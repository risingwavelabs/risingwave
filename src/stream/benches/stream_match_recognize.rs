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

//! Per-watermark cost of `MatchRecognizeExecutor` as a function of the number of retained
//! partitions.
//!
//! Every partition holds ONE open partial (`PATTERN (a b) WITHIN bound`, no `DEFINE`, a single
//! row per partition), so a watermark that closes nothing has nothing to emit and nothing to
//! evict — the visit is pure overhead. Two shapes:
//!
//! - `idle_watermarks`: `IDLE_WATERMARKS` watermarks that stay below every deadline, reported per
//!   watermark. Before the wakeup frontier (#27205) each one visits all `N` partitions: `O(N)` per
//!   watermark. With it, `O(1)`.
//! - `barrier_only`: the same round with zero watermarks — the fence alone, reported per round.
//! - `within_cliff`: one watermark past every deadline, so all `N` partitions expire at once and
//!   every row is evicted, reported per expired partition. The frontier must not regress this: it
//!   is the case where every partition genuinely needs the visit.
//!
//! The executor forwards no watermark downstream (its output carries no watermark column), so the
//! only observable signal that a round's watermarks were processed is the barrier that follows
//! them — and `MemoryStateStore` syncs its whole key space on a barrier, an `O(stored rows)` cost
//! that has nothing to do with the executor. Hence the control: `idle_watermarks × IDLE_WATERMARKS
//! − barrier_only` is the executor's cost for the round, and with 200 watermarks per round the
//! fence is a small share of the timed interval to begin with.

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures::StreamExt;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::test_epoch;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::{NonStrictExpression, build_from_pretty};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_stream::common::table::state_table::StateTable;
use risingwave_stream::common::table::test_utils::gen_pbtable_with_dist_key;
use risingwave_stream::executor::match_recognize::executor::{
    DeadlineErrorReport, MatchRecognizeExecutor, MatchRecognizeExecutorArgs,
};
use risingwave_stream::executor::match_recognize::nfa::{Nfa, Pattern, SkipMode};
use risingwave_stream::executor::test_utils::{MessageSender, MockSource};
use risingwave_stream::executor::{ActorContext, BoxedMessageStream, Execute, Message};
use risingwave_stream::task::ActorEvalErrorReport;
use tokio::runtime::Runtime;

risingwave_expr_impl::enable!();

/// `WITHIN` bound in order-key units. Rows sit at order key 0, so every deadline is `BOUND`. Far
/// above any watermark the idle rounds can reach: with the wakeup index a round costs microseconds,
/// so criterion runs hundreds of thousands of them per sample.
const BOUND: i64 = 1 << 40;
/// Idle watermarks per timed round: enough that the fencing barrier (see the module doc) is a
/// small share of the interval.
const IDLE_WATERMARKS: i64 = 200;

/// Input: `(partition int8, ts int8, v int8)`; `PARTITION BY partition ORDER BY ts`.
fn input_types() -> Vec<DataType> {
    vec![DataType::Int64, DataType::Int64, DataType::Int64]
}

/// Build the executor over an in-memory state store and drive it through its first barrier.
async fn build(store: MemoryStateStore) -> (MessageSender, BoxedMessageStream) {
    let input_schema = Schema::new(input_types().into_iter().map(Field::unnamed).collect());
    // Output: the partition column, then the hidden per-match id (no MEASURES).
    let output_schema = Schema::new(vec![
        Field::with_name(DataType::Int64, "partition_0"),
        Field::with_name(DataType::Int64, "_match_id"),
    ]);

    // State table: `seq` then the raw input columns (offset 1); key = partition, ts, seq — the
    // layout `StreamMatchRecognize::infer_state_table` produces.
    let table_columns = vec![
        ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64),
        ColumnDesc::unnamed(ColumnId::new(3), DataType::Int64),
    ];
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
    // Span predicate over the synthetic `[last, first]` row, and the deadline `first + bound` —
    // built the way `from_proto` builds them.
    let within = NonStrictExpression::new_topmost(
        build_from_pretty(format!(
            "(less_than_or_equal:boolean (subtract:int8 $0:int8 $1:int8) {BOUND}:int8)"
        )),
        report.clone(),
    );
    let within_deadline = NonStrictExpression::new_topmost(
        build_from_pretty(format!("(add:int8 $0:int8 {BOUND}:int8)")),
        DeadlineErrorReport::new(report.clone()),
    );
    let nfa = Nfa::compile(&Pattern::Concat(vec![
        Pattern::Var("a".to_owned()),
        Pattern::Var("b".to_owned()),
    ]));

    let (mut tx, source) = MockSource::channel();
    let source = source.into_executor(input_schema, vec![0, 1]);
    let executor = MatchRecognizeExecutor::new(MatchRecognizeExecutorArgs {
        ctx,
        input: source,
        schema: output_schema,
        chunk_size: 1024,
        partition_key_indices: vec![0],
        order_key_indices: vec![1],
        measures: vec![],
        defines: vec![],
        within: Some(within),
        within_deadline: Some(within_deadline),
        nfa,
        skip: SkipMode::PastLastRow,
        eval_error_report: report,
        state_table,
    });
    let mut stream = executor.boxed().execute();

    tx.push_barrier(test_epoch(1), false);
    drain_until_barrier(&mut stream, test_epoch(1)).await;
    (tx, stream)
}

/// One row per partition, all at order key 0: `N` open partials, none completable.
fn partitions_chunk(n: usize) -> StreamChunk {
    let rows: Vec<(Op, OwnedRow)> = (0..n as i64)
        .map(|p| {
            (
                Op::Insert,
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int64(p)),
                    Some(ScalarImpl::Int64(0)),
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

/// Load `n` partitions and return the executor ready for the timed section.
async fn load(n: usize) -> (MessageSender, BoxedMessageStream) {
    let (mut tx, mut stream) = build(MemoryStateStore::new()).await;
    tx.push_chunk(partitions_chunk(n));
    tx.push_barrier_with_prev_epoch_for_test(test_epoch(2), test_epoch(1), false);
    drain_until_barrier(&mut stream, test_epoch(2)).await;
    (tx, stream)
}

/// `iters` rounds of `watermarks` idle watermarks on ONE loaded executor. An idle round mutates
/// nothing (no window closes), so the same partitions serve every round; the watermark keeps
/// climbing and each round is fenced by a barrier so the whole pass is observed before the clock
/// stops. `watermarks == 0` is the barrier-only control. Returns the summed time of the rounds.
async fn idle_rounds(
    mut tx: MessageSender,
    mut stream: BoxedMessageStream,
    iters: u64,
    watermarks: i64,
) -> Duration {
    let mut total = Duration::ZERO;
    let mut w = 1i64;
    for epoch in (3u64..).take(iters as usize) {
        assert!(
            w + watermarks < BOUND,
            "too many idle rounds: the watermark would reach the deadline"
        );
        let start = Instant::now();
        for _ in 0..watermarks {
            tx.push_int64_watermark(1, w);
            w += 1;
        }
        tx.push_barrier_with_prev_epoch_for_test(test_epoch(epoch), test_epoch(epoch - 1), false);
        drain_until_barrier(&mut stream, test_epoch(epoch)).await;
        total += start.elapsed();
    }
    // Dropping the sender and the stream tears the executor down; an executor stream does not
    // end on a stop barrier (that is actor-level shutdown), so there is nothing to drain to.
    drop(tx);
    drop(stream);
    total
}

/// One watermark past every deadline: all `n` partitions expire and every row is evicted. This
/// consumes the loaded state, so the caller reloads before every call.
async fn within_cliff(mut tx: MessageSender, mut stream: BoxedMessageStream) {
    tx.push_int64_watermark(1, BOUND + 1);
    tx.push_barrier_with_prev_epoch_for_test(test_epoch(3), test_epoch(2), false);
    drain_until_barrier(&mut stream, test_epoch(3)).await;
    drop(tx);
    drop(stream);
}

fn bench_watermark_pass(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("match_recognize_watermark_pass");
    // Loading tens of thousands of partitions dominates wall-clock, so keep the windows short:
    // criterion sizes the iteration count from the TIMED part alone.
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));

    for &n in &[1_000usize, 10_000, 50_000] {
        // Reported per watermark: the timed round is `IDLE_WATERMARKS` watermarks plus one fence.
        group.throughput(Throughput::Elements(IDLE_WATERMARKS as u64));
        group.bench_with_input(BenchmarkId::new("idle_watermarks", n), &n, |b, &n| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let (tx, stream) = load(n).await;
                idle_rounds(tx, stream, iters, IDLE_WATERMARKS).await
            })
        });
        // The fence alone, per round: subtract from `idle_watermarks × IDLE_WATERMARKS`.
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("barrier_only", n), &n, |b, &n| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let (tx, stream) = load(n).await;
                idle_rounds(tx, stream, iters, 0).await
            })
        });
        // Reported per expired partition, which is what the cliff scales with.
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("within_cliff", n), &n, |b, &n| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (tx, stream) = load(n).await;
                    let start = Instant::now();
                    within_cliff(tx, stream).await;
                    total += start.elapsed();
                }
                total
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_watermark_pass);
criterion_main!(benches);
