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

//! On-demand compaction diagnostics, independent of await-tree and Prometheus.
//! No periodic logging/sampling, no per-key instrumentation. Each split has its own statistics
//! lock; background uploads share only their parent split's lock. The task registry is touched
//! only on task registration/completion and report collection. Reports are best-effort snapshots.
//! Client call durations include SDK waits/retries and are not server processing times.

use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write;
use std::future::Future;
use std::sync::{Arc, LazyLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use await_tree::InstrumentAwait;
use parking_lot::Mutex;
use risingwave_hummock_sdk::compact_task::CompactTask;
use risingwave_hummock_sdk::{CompactionGroupId, HummockSstableObjectId};
use risingwave_pb::hummock::compact_task::TaskStatus;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::task_progress::{TaskProgress, TaskProgressGuard, TaskProgressManagerRef};
use super::{CompactorContext, await_tree_key};

const SLOW_TASK: Duration = Duration::from_secs(120);
const RECENT_TTL: Duration = Duration::from_secs(3600);
const RECENT_LIMIT: usize = 32;
const RUNNING_LIMIT: usize = 64;
const TASK_DETAIL_LIMIT: usize = 8;
const DETAIL_LIMIT: usize = 3;
const REPORT_BYTES_LIMIT: usize = 128 * 1024;

tokio::task_local! { static CURRENT: Context; }

#[derive(Clone)]
struct Context {
    task: Arc<Diagnostics>,
    // Root/catalog/split-generation work uses root statistics; each split owns another shard.
    stats: Arc<Mutex<Shard>>,
    split: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Operation {
    AcquireCatalog,
    GenerateSplits,
    FetchSstMeta,
    OpenReadStream,
    ReadStream,
    AllocateSstId,
    OpenUploader,
    WriteUpload,
    FinishUpload,
    WaitUploads,
}

#[derive(Clone, Debug, Default)]
struct IoSummary {
    completed: u64,
    failed: u64,
    cancelled: u64,
    bytes: u64,
    // Calls may overlap or nest; summing these is not task wall time or CPU time.
    total_ms: u128,
    max_ms: u128,
}

impl IoSummary {
    fn merge(&mut self, other: &Self) {
        self.completed += other.completed;
        self.failed += other.failed;
        self.cancelled += other.cancelled;
        self.bytes += other.bytes;
        self.total_ms += other.total_ms;
        self.max_ms = self.max_ms.max(other.max_ms);
    }
}

struct PendingIo {
    operation: Operation,
    object_id: Option<HummockSstableObjectId>,
    started: Instant,
}

struct Shard {
    started: Instant,
    finished: Option<Duration>,
    cancelled: bool,
    next_id: u64,
    active: BTreeMap<u64, PendingIo>,
    io: BTreeMap<Operation, IoSummary>,
    blocks: u64,
    retries: u64,
    last_progress: Option<Instant>,
}

impl Shard {
    fn new(started: Instant) -> Self {
        Self {
            started,
            finished: None,
            cancelled: false,
            next_id: 0,
            active: BTreeMap::new(),
            io: BTreeMap::new(),
            blocks: 0,
            retries: 0,
            last_progress: None,
        }
    }
}

struct TaskState {
    phase: &'static str,
    result: Option<TaskStatus>,
    splits: BTreeMap<usize, Arc<Mutex<Shard>>>,
}

struct Diagnostics {
    info: TaskInfo,
    started: Instant,
    registry: Option<await_tree::Registry>,
    progress: Arc<TaskProgress>,
    root: Arc<Mutex<Shard>>,
    state: Mutex<TaskState>,
}

#[derive(Clone, Debug)]
struct WaitSnapshot {
    operation: Operation,
    object_id: Option<HummockSstableObjectId>,
    split: Option<usize>,
    age_ms: u128,
}

#[derive(Clone, Debug)]
struct SplitSnapshot {
    index: usize,
    age_ms: u128,
    finished: bool,
    cancelled: bool,
}

#[derive(Clone)]
struct Snapshot {
    info: TaskInfo,
    phase: &'static str,
    result: Option<TaskStatus>,
    age_ms: u128,
    last_progress_age_ms: u128,
    progress: risingwave_pb::hummock::CompactTaskProgress,
    blocks: u64,
    retries: u64,
    io: BTreeMap<Operation, IoSummary>,
    active_io: usize,
    oldest_waits: Vec<WaitSnapshot>,
    splits_total: usize,
    splits_done: usize,
    splits_cancelled: usize,
    slowest_splits: Vec<SplitSnapshot>,
}

impl Snapshot {
    fn bytes(&self, operation: Operation) -> u64 {
        self.io.get(&operation).map_or(0, |io| io.bytes)
    }

    fn status(&self) -> &'static str {
        self.result
            .map_or("CANCELLED_OR_ABORTED", |result| result.as_str_name())
    }

    fn path(&self) -> &'static str {
        match self.phase {
            "execute_fast" => "fast",
            "execute_normal" => "normal",
            _ => "initializing",
        }
    }
}

impl Diagnostics {
    fn snapshot(&self, now: Instant) -> Snapshot {
        // Copy shard references, then release the task lock before reading any split's statistics.
        let (phase, result, splits) = {
            let state = self.state.lock();
            (
                state.phase,
                state.result,
                state
                    .splits
                    .iter()
                    .map(|(&id, shard)| (id, shard.clone()))
                    .collect::<Vec<_>>(),
            )
        };
        let mut snapshot = Snapshot {
            info: self.info.clone(),
            phase,
            result,
            age_ms: now.saturating_duration_since(self.started).as_millis(),
            last_progress_age_ms: 0,
            progress: self.progress.snapshot(self.info.task_id),
            blocks: 0,
            retries: 0,
            io: BTreeMap::new(),
            active_io: 0,
            oldest_waits: Vec::new(),
            splits_total: splits.len(),
            splits_done: 0,
            splits_cancelled: 0,
            slowest_splits: Vec::new(),
        };
        let mut last_progress = self.started;
        for (split, shard) in std::iter::once((None, self.root.clone()))
            .chain(splits.into_iter().map(|(id, shard)| (Some(id), shard)))
        {
            let shard = shard.lock();
            snapshot.blocks += shard.blocks;
            snapshot.retries += shard.retries;
            if let Some(progress) = shard.last_progress {
                last_progress = last_progress.max(progress);
            }
            for (&operation, io) in &shard.io {
                snapshot.io.entry(operation).or_default().merge(io);
            }
            snapshot.active_io += shard.active.len();
            snapshot
                .oldest_waits
                .extend(shard.active.values().map(|wait| WaitSnapshot {
                    operation: wait.operation,
                    object_id: wait.object_id,
                    split,
                    // A call can start after the report began taking its snapshot.
                    age_ms: now.saturating_duration_since(wait.started).as_millis(),
                }));
            if let Some(index) = split {
                snapshot.splits_done += usize::from(shard.finished.is_some());
                snapshot.splits_cancelled += usize::from(shard.cancelled);
                snapshot.slowest_splits.push(SplitSnapshot {
                    index,
                    age_ms: shard
                        .finished
                        .unwrap_or_else(|| now.saturating_duration_since(shard.started))
                        .as_millis(),
                    finished: shard.finished.is_some(),
                    cancelled: shard.cancelled,
                });
            }
        }
        snapshot.last_progress_age_ms = now.saturating_duration_since(last_progress).as_millis();
        snapshot
            .oldest_waits
            .sort_unstable_by_key(|wait| std::cmp::Reverse(wait.age_ms));
        snapshot.oldest_waits.truncate(DETAIL_LIMIT);
        snapshot
            .slowest_splits
            .sort_unstable_by_key(|split| (split.finished, std::cmp::Reverse(split.age_ms)));
        snapshot.slowest_splits.truncate(DETAIL_LIMIT);
        snapshot
    }
}

#[derive(Clone)]
struct CompletedTask {
    finished_at: Instant,
    snapshot: Snapshot,
}

#[derive(Default)]
struct ReportRegistry {
    running: BTreeMap<u64, Weak<Diagnostics>>,
    recent: VecDeque<CompletedTask>,
}

impl ReportRegistry {
    fn expire(&mut self, now: Instant) {
        self.recent
            .retain(|task| now.saturating_duration_since(task.finished_at) < RECENT_TTL);
    }

    fn remember(&mut self, task: CompletedTask) {
        self.expire(task.finished_at);
        self.recent.push_back(task);
        while self.recent.len() > RECENT_LIMIT {
            self.recent.pop_front();
        }
    }
}

type Reports = Arc<Mutex<ReportRegistry>>;
static REPORTS: LazyLock<Reports> =
    LazyLock::new(|| Arc::new(Mutex::new(ReportRegistry::default())));

struct TaskRegistration {
    task: Arc<Diagnostics>,
    reports: Reports,
}

impl TaskRegistration {
    fn new(task: Arc<Diagnostics>, reports: Reports) -> Self {
        reports
            .lock()
            .running
            .insert(task.info.task_id, Arc::downgrade(&task));
        Self { task, reports }
    }
}

impl Drop for TaskRegistration {
    fn drop(&mut self) {
        let now = Instant::now();
        let result = self.task.state.lock().result;
        let completed = (result != Some(TaskStatus::Success)
            || now.duration_since(self.task.started) >= SLOW_TASK)
            .then(|| CompletedTask {
                finished_at: now,
                snapshot: self.task.snapshot(now),
            });
        let mut reports = self.reports.lock();
        // Do not remove a newer registration if a task ID has been reused.
        if reports
            .running
            .get(&self.task.info.task_id)
            .is_some_and(|task| task.ptr_eq(&Arc::downgrade(&self.task)))
        {
            reports.running.remove(&self.task.info.task_id);
        }
        if let Some(completed) = completed {
            reports.remember(completed);
        }
    }
}

/// Plain text piggybacked on the existing `StackTrace` response, without a protocol change.
/// Its registry exists even when async stack tracing is disabled.
pub fn report() -> String {
    render_report(&REPORTS, Instant::now())
}

fn render_report(reports: &Reports, now: Instant) -> String {
    let (mut running, recent) = {
        let mut reports = reports.lock();
        reports.expire(now);
        let running = reports
            .running
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        let recent = reports.recent.iter().rev().cloned().collect::<Vec<_>>();
        (running, recent)
    };
    // No registry/task lock is held while formatting the report.
    running.sort_unstable_by_key(|task| (task.started, task.info.task_id));
    let total_running = running.len();
    running.truncate(RUNNING_LIMIT);
    let snapshots = running
        .iter()
        .map(|task| task.snapshot(now))
        .collect::<Vec<_>>();
    let mut out = String::new();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let _ = writeln!(
        out,
        "Compactor diagnostics v1; snapshot_unix_ms={timestamp}"
    );
    let _ = writeln!(
        out,
        "Cumulative values since task start; best-effort snapshot. I/O/block progress excludes key-only CPU work."
    );
    let _ = writeln!(
        out,
        "Client call times include SDK/cache waits and retries. Calls overlap/nest; total_ms is not task wall time."
    );
    let _ = writeln!(
        out,
        "Submitted writes may be buffered; finished uploads confirm completion. ReadStream calls are chunks, not HTTP GETs."
    );
    let _ = writeln!(
        out,
        "\nRunning tasks: {total_running} (showing at most {RUNNING_LIMIT}, oldest first)"
    );
    let _ = writeln!(
        out,
        "Task Group Type Path Phase Age_s Input_SSTs Input_MiB Read_MiB Submitted_MiB Splits_done/total Cancelled_splits Active_calls"
    );
    for task in &snapshots {
        let _ = writeln!(
            out,
            "{} {} {} {} {} {:.3} {} {:.3} {:.3} {:.3} {}/{} {} {}",
            task.info.task_id,
            task.info.group_id,
            task.info.task_type,
            task.path(),
            task.phase,
            task.age_ms as f64 / 1000.0,
            task.info.input_ssts,
            mib(task.info.input_bytes),
            mib(task.bytes(Operation::ReadStream)),
            mib(task.bytes(Operation::WriteUpload)),
            task.splits_done,
            task.splits_total,
            task.splits_cancelled,
            task.active_io
        );
    }
    if snapshots.is_empty() {
        let _ = writeln!(out, "(none)");
    }
    if total_running > RUNNING_LIMIT {
        let _ = writeln!(
            out,
            "Omitted running tasks: {}",
            total_running - RUNNING_LIMIT
        );
    }
    let _ = writeln!(out, "\nRunning task details (at most {TASK_DETAIL_LIMIT}):");
    for snapshot in snapshots.iter().take(TASK_DETAIL_LIMIT) {
        write_details(&mut out, snapshot);
    }
    let _ = writeln!(
        out,
        "\nRecent slow/failed tasks (at most {RECENT_LIMIT}, retained up to 1h; newest first):"
    );
    let _ = writeln!(
        out,
        "Task Group Result Duration_s Finished_ago_s Read_MiB Submitted_MiB"
    );
    for task in &recent {
        let snapshot = &task.snapshot;
        let _ = writeln!(
            out,
            "{} {} {} {:.3} {:.3} {:.3} {:.3}",
            snapshot.info.task_id,
            snapshot.info.group_id,
            snapshot.status(),
            snapshot.age_ms as f64 / 1000.0,
            now.saturating_duration_since(task.finished_at)
                .as_secs_f64(),
            mib(snapshot.bytes(Operation::ReadStream)),
            mib(snapshot.bytes(Operation::WriteUpload))
        );
    }
    if recent.is_empty() {
        let _ = writeln!(out, "(none)");
    }
    let _ = writeln!(
        out,
        "\nRecent task details (at most {TASK_DETAIL_LIMIT}; frozen at completion):"
    );
    for task in recent.iter().take(TASK_DETAIL_LIMIT) {
        write_details(&mut out, &task.snapshot);
    }
    if out.len() > REPORT_BYTES_LIMIT {
        let marker = "\n[diagnostic report truncated at 128 KiB]\n";
        let mut end = REPORT_BYTES_LIMIT - marker.len();
        while !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
        out.push_str(marker);
    }
    out
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn write_details(out: &mut String, task: &Snapshot) {
    let _ = writeln!(
        out,
        "\nTask {} group={} phase={} age_ms={} last_io_or_block_progress_age_ms={}",
        task.info.task_id, task.info.group_id, task.phase, task.age_ms, task.last_progress_age_ms
    );
    let _ = writeln!(
        out,
        "  keys={} blocks={} iterator_retries={} ssts_sealed={} ssts_uploaded={} active_sst_readers={} unfinished_output_ssts={}",
        task.progress.num_progress_key,
        task.blocks,
        task.retries,
        task.progress.num_ssts_sealed,
        task.progress.num_ssts_uploaded,
        task.progress.num_pending_read_io,
        task.progress.num_pending_write_io
    );
    let _ = writeln!(
        out,
        "  Completed calls (bytes are operation-specific; FinishUpload bytes=0):"
    );
    let _ = writeln!(
        out,
        "  Operation Completed Failed Cancelled Bytes Total_ms Max_ms"
    );
    for (operation, io) in &task.io {
        let _ = writeln!(
            out,
            "  {operation:?} {} {} {} {} {} {}",
            io.completed, io.failed, io.cancelled, io.bytes, io.total_ms, io.max_ms
        );
    }
    let _ = writeln!(
        out,
        "  Oldest active calls (at most {DETAIL_LIMIT}; not included in completed durations):"
    );
    for wait in &task.oldest_waits {
        let _ = writeln!(
            out,
            "  operation={:?} object={:?} split={:?} age_ms={}",
            wait.operation, wait.object_id, wait.split, wait.age_ms
        );
    }
    let _ = writeln!(
        out,
        "  Slowest splits (running first, at most {DETAIL_LIMIT}):"
    );
    for split in &task.slowest_splits {
        let _ = writeln!(
            out,
            "  split={} age_ms={} finished={} cancelled={}",
            split.index, split.age_ms, split.finished, split.cancelled
        );
    }
}

pub(crate) fn is_tracing() -> bool {
    CURRENT
        .try_with(|context| context.task.registry.is_some())
        .unwrap_or(false)
}

pub(crate) fn set_phase(phase: &'static str) {
    let _ = CURRENT.try_with(|context| context.task.state.lock().phase = phase);
}

pub(crate) fn set_result(result: TaskStatus) {
    let _ = CURRENT.try_with(|context| context.task.state.lock().result = Some(result));
}

pub(crate) fn record_block() {
    let _ = CURRENT.try_with(|context| {
        let mut stats = context.stats.lock();
        stats.blocks += 1;
        stats.last_progress = Some(Instant::now());
    });
}

pub(crate) fn record_retry() {
    let _ = CURRENT.try_with(|context| context.stats.lock().retries += 1);
}

struct IoGuard {
    stats: Arc<Mutex<Shard>>,
    id: u64,
    result: Option<(bool, u64)>,
}

impl Drop for IoGuard {
    fn drop(&mut self) {
        let now = Instant::now();
        let mut stats = self.stats.lock();
        let pending = stats
            .active
            .remove(&self.id)
            .expect("registered diagnostic I/O");
        let elapsed = now.duration_since(pending.started).as_millis();
        let summary = stats.io.entry(pending.operation).or_default();
        summary.total_ms += elapsed;
        summary.max_ms = summary.max_ms.max(elapsed);
        match self.result {
            Some((true, bytes)) => {
                summary.completed += 1;
                summary.bytes += bytes;
                stats.last_progress = Some(now);
            }
            Some((false, _)) => summary.failed += 1,
            None => summary.cancelled += 1,
        }
    }
}

pub(crate) async fn observe<T, E>(
    operation: Operation,
    object_id: Option<HummockSstableObjectId>,
    future: impl Future<Output = Result<T, E>>,
    bytes: impl FnOnce(&T) -> u64,
) -> Result<T, E> {
    let Ok((stats, trace)) =
        CURRENT.try_with(|context| (context.stats.clone(), context.task.registry.is_some()))
    else {
        return future.await;
    };
    let id = {
        let mut stats = stats.lock();
        let id = stats.next_id;
        stats.next_id += 1;
        stats.active.insert(
            id,
            PendingIo {
                operation,
                object_id,
                started: Instant::now(),
            },
        );
        id
    };
    let mut guard = IoGuard {
        stats,
        id,
        result: None,
    };
    let result = if trace {
        future
            .instrument_await(format!("{operation:?} object={object_id:?}"))
            .await
    } else {
        future.await
    };
    guard.result = Some((result.is_ok(), result.as_ref().map(bytes).unwrap_or(0)));
    result
}

/// Tokio does not inherit task locals or await-tree parents across a spawn boundary.
pub(crate) fn spawn_upload<F>(object_id: HummockSstableObjectId, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let context = CURRENT.try_with(Clone::clone).ok();
    tokio::spawn(async move {
        let Some(context) = context else {
            return future.await;
        };
        let registry = context.task.registry.clone();
        let task_id = context.task.info.task_id;
        let split = context.split;
        let future = CURRENT.scope(context, future);
        match registry {
            None => future.await,
            Some(registry) => {
                registry
                    .register(
                        await_tree_key::CompactUpload { task_id, object_id },
                        format!(
                            "Compaction task={task_id} split={split:?} upload object={object_id}"
                        ),
                    )
                    .instrument(future)
                    .await
            }
        }
    })
}

pub(crate) fn instrument_split<F: Future>(
    split: usize,
    future: F,
) -> impl Future<Output = F::Output> {
    // Heap-pin the large compaction future to avoid inflating its parent's stack frame.
    let future = Box::pin(future);
    let context = CURRENT
        .try_with(|parent| {
            let stats = Arc::new(Mutex::new(Shard::new(Instant::now())));
            parent.task.state.lock().splits.insert(split, stats.clone());
            Context {
                task: parent.task.clone(),
                stats,
                split: Some(split),
            }
        })
        .ok();
    // Captured before spawn, so dropping an unpolled split also records cancellation.
    let guard = context.as_ref().map(|context| SplitGuard {
        stats: context.stats.clone(),
        completed: false,
    });
    async move {
        let Some(context) = context else {
            return future.await;
        };
        let mut guard = guard.expect("registered split");
        let output = CURRENT.scope(context, future).await;
        guard.completed = true;
        output
    }
}

struct SplitGuard {
    stats: Arc<Mutex<Shard>>,
    completed: bool,
}
impl Drop for SplitGuard {
    fn drop(&mut self) {
        let mut stats = self.stats.lock();
        stats.finished = Some(stats.started.elapsed());
        stats.cancelled = !self.completed;
    }
}

#[derive(Clone)]
pub(crate) struct TaskInfo {
    task_id: u64,
    group_id: CompactionGroupId,
    input_ssts: usize,
    input_bytes: u64,
    task_type: &'static str,
}

impl From<&CompactTask> for TaskInfo {
    fn from(task: &CompactTask) -> Self {
        Self {
            task_id: task.task_id,
            group_id: task.compaction_group_id,
            input_ssts: task.read_input_ssts().count(),
            input_bytes: task.read_input_ssts().map(|sst| sst.sst_size).sum(),
            task_type: task.task_type.as_str_name(),
        }
    }
}

pub(crate) fn instrument_task<F: Future>(
    info: TaskInfo,
    context: &CompactorContext,
    future: F,
) -> impl Future<Output = F::Output> + use<F> {
    instrument_task_inner(
        info,
        context.task_progress_manager.clone(),
        context.await_tree_reg.clone(),
        REPORTS.clone(),
        Box::pin(future),
    )
}

async fn instrument_task_inner<F: Future>(
    info: TaskInfo,
    progress_manager: TaskProgressManagerRef,
    registry: Option<await_tree::Registry>,
    reports: Reports,
    future: F,
) -> F::Output {
    let _progress_guard = TaskProgressGuard::new(info.task_id, progress_manager);
    let started = Instant::now();
    let task = Arc::new(Diagnostics {
        info,
        started,
        registry,
        progress: _progress_guard.progress.clone(),
        root: Arc::new(Mutex::new(Shard::new(started))),
        state: Mutex::new(TaskState {
            phase: "starting",
            result: None,
            splits: BTreeMap::new(),
        }),
    });
    let _registration = TaskRegistration::new(task.clone(), reports);
    let scoped = CURRENT.scope(
        Context {
            task: task.clone(),
            stats: task.root.clone(),
            split: None,
        },
        future,
    );
    match &task.registry {
        None => scoped.await,
        Some(registry) => {
            registry
                .register(
                    await_tree_key::CompactTask {
                        task_id: task.info.task_id,
                    },
                    format!(
                        "Compaction task={} group={}",
                        task.info.task_id, task.info.group_id
                    ),
                )
                .instrument(scoped)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::{future, poll};

    use super::*;

    fn info(id: u64) -> TaskInfo {
        TaskInfo {
            task_id: id,
            group_id: CompactionGroupId::from(23),
            input_ssts: 1,
            input_bytes: 10,
            task_type: "DYNAMIC",
        }
    }

    fn context_at(id: u64, registry: Option<await_tree::Registry>, started: Instant) -> Context {
        let root = Arc::new(Mutex::new(Shard::new(started)));
        Context {
            task: Arc::new(Diagnostics {
                info: info(id),
                started,
                registry,
                progress: Arc::new(TaskProgress::default()),
                root: root.clone(),
                state: Mutex::new(TaskState {
                    phase: "starting",
                    result: None,
                    splits: BTreeMap::new(),
                }),
            }),
            stats: root,
            split: None,
        }
    }

    fn context(registry: Option<await_tree::Registry>) -> Context {
        context_at(17, registry, Instant::now())
    }

    fn reports() -> Reports {
        Arc::new(Mutex::new(ReportRegistry::default()))
    }

    #[tokio::test]
    async fn test_io_errors_and_cancellation_without_await_tree() {
        let context = context(None);
        let task = context.task.clone();
        CURRENT
            .scope(context, async {
                assert_eq!(
                    observe(
                        Operation::ReadStream,
                        None,
                        async { Ok::<_, ()>(vec![1, 2, 3]) },
                        |bytes| bytes.len() as u64
                    )
                    .await,
                    Ok(vec![1, 2, 3])
                );
                assert!(
                    observe(
                        Operation::ReadStream,
                        None,
                        async { Err::<(), _>("read failed") },
                        |_| 0
                    )
                    .await
                    .is_err()
                );
                let mut blocked = Box::pin(observe(
                    Operation::ReadStream,
                    None,
                    future::pending::<Result<(), ()>>(),
                    |_| 0,
                ));
                assert!(poll!(blocked.as_mut()).is_pending());
                assert_eq!(task.snapshot(Instant::now()).active_io, 1);
                drop(blocked);
            })
            .await;
        let snapshot = task.snapshot(Instant::now());
        assert_eq!(snapshot.active_io, 0);
        let io = &snapshot.io[&Operation::ReadStream];
        assert_eq!(
            (io.completed, io.failed, io.cancelled, io.bytes),
            (1, 1, 1, 3)
        );
    }

    #[tokio::test]
    async fn test_splits_use_independent_statistics_and_aggregate_on_demand() {
        let context = context(None);
        let task = context.task.clone();
        CURRENT
            .scope(context, async {
                let (first, second) = futures::join!(
                    instrument_split(
                        0,
                        observe(
                            Operation::ReadStream,
                            None,
                            async { Ok::<_, ()>(2_u64) },
                            |bytes| *bytes
                        )
                    ),
                    instrument_split(
                        1,
                        observe(
                            Operation::ReadStream,
                            None,
                            async { Ok::<_, ()>(3_u64) },
                            |bytes| *bytes
                        )
                    ),
                );
                first.unwrap();
                second.unwrap();
            })
            .await;
        let shards = task
            .state
            .lock()
            .splits
            .values()
            .cloned()
            .collect::<Vec<_>>();
        assert!(!Arc::ptr_eq(&shards[0], &shards[1]));
        assert!(!Arc::ptr_eq(&shards[0], &task.root));
        assert!(task.root.lock().io.is_empty());
        assert_eq!(shards[0].lock().io[&Operation::ReadStream].bytes, 2);
        assert_eq!(shards[1].lock().io[&Operation::ReadStream].bytes, 3);
        let snapshot = task.snapshot(Instant::now());
        assert_eq!(snapshot.bytes(Operation::ReadStream), 5);
        assert_eq!((snapshot.splits_total, snapshot.splits_done), (2, 2));
    }

    #[tokio::test]
    async fn test_queued_split_is_not_io_progress_and_cancels_cleanly() {
        let context = context_at(17, None, Instant::now() - Duration::from_secs(300));
        let task = context.task.clone();
        CURRENT
            .scope(context, async {
                let split = instrument_split(4, future::pending::<()>());
                assert!(task.snapshot(Instant::now()).last_progress_age_ms >= 300_000);
                drop(split);
            })
            .await;
        let snapshot = task.snapshot(Instant::now());
        assert_eq!((snapshot.splits_done, snapshot.splits_cancelled), (1, 1));
    }

    #[tokio::test]
    async fn test_background_upload_inherits_split_and_has_own_tree() {
        let registry = await_tree::Registry::new(await_tree::Config::default());
        let context = context(Some(registry.clone()));
        let task = context.task.clone();
        let object_id = HummockSstableObjectId::from(99);
        CURRENT
            .scope(context, async {
                instrument_split(3, async {
                    let (entered, ready) = tokio::sync::oneshot::channel();
                    let (release, wait) = tokio::sync::oneshot::channel();
                    let upload = spawn_upload(
                        object_id,
                        observe(
                            Operation::FinishUpload,
                            Some(object_id),
                            async move {
                                entered.send(()).unwrap();
                                wait.await.unwrap();
                                Ok::<_, ()>(())
                            },
                            |_| 0,
                        ),
                    );
                    ready.await.unwrap();
                    let snapshot = task.snapshot(Instant::now());
                    assert_eq!(snapshot.oldest_waits[0].split, Some(3));
                    let trees = registry.collect::<await_tree_key::Compaction>();
                    assert_eq!(trees.len(), 1);
                    assert!(trees[0].1.to_string().contains("FinishUpload"));
                    release.send(()).unwrap();
                    upload.await.unwrap().unwrap();
                })
                .await;
            })
            .await;
        let snapshot = task.snapshot(Instant::now());
        assert_eq!(snapshot.active_io, 0);
        assert_eq!(snapshot.io[&Operation::FinishUpload].completed, 1);
        assert_eq!((snapshot.splits_total, snapshot.splits_done), (1, 1));
    }

    #[tokio::test]
    async fn test_live_report_and_abort_cleanup_with_and_without_await_tree() {
        for tracing in [false, true] {
            let manager = TaskProgressManagerRef::default();
            let trees = await_tree::Registry::new(await_tree::Config::default());
            let reports = reports();
            let (entered, ready) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(instrument_task_inner(
                info(17),
                manager.clone(),
                tracing.then(|| trees.clone()),
                reports.clone(),
                observe(
                    Operation::AcquireCatalog,
                    None,
                    async move {
                        entered.send(()).unwrap();
                        future::pending::<Result<(), ()>>().await
                    },
                    |_| 0,
                ),
            ));
            ready.await.unwrap();
            assert_eq!(manager.lock().len(), 1);
            let report = render_report(&reports, Instant::now());
            assert!(report.contains("Running tasks: 1"));
            assert!(report.contains("operation=AcquireCatalog"));
            assert_eq!(
                trees.collect::<await_tree_key::Compaction>().len(),
                usize::from(tracing)
            );
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(manager.lock().is_empty());
            assert!(trees.collect::<await_tree_key::Compaction>().is_empty());
            assert!(reports.lock().running.is_empty());
            let report = render_report(&reports, Instant::now());
            assert!(report.contains("Running tasks: 0"));
            assert!(report.contains("CANCELLED_OR_ABORTED"));
        }
    }

    #[tokio::test]
    async fn test_quick_success_not_retained_but_failure_retained() {
        let reports = reports();
        let manager = TaskProgressManagerRef::default();
        instrument_task_inner(info(17), manager.clone(), None, reports.clone(), async {
            set_result(TaskStatus::Success);
        })
        .await;
        assert!(reports.lock().recent.is_empty());
        instrument_task_inner(info(18), manager.clone(), None, reports.clone(), async {
            set_result(TaskStatus::ExecuteFailed);
        })
        .await;
        assert!(manager.lock().is_empty());
        let recent = reports.lock();
        assert_eq!(recent.recent.len(), 1);
        assert_eq!(recent.recent[0].snapshot.status(), "EXECUTE_FAILED");
    }

    #[test]
    fn test_slow_success_is_frozen_and_recent_history_is_bounded_and_expires() {
        let now = Instant::now();
        let context = context_at(17, None, now - SLOW_TASK);
        context.task.state.lock().result = Some(TaskStatus::Success);
        let reports = reports();
        let registration = TaskRegistration::new(context.task, reports.clone());
        drop(registration);
        assert_eq!(reports.lock().recent.len(), 1);
        let frozen = reports.lock().recent[0].snapshot.clone();
        assert_eq!(frozen.result, Some(TaskStatus::Success));
        assert!(frozen.age_ms >= SLOW_TASK.as_millis());
        for id in 0..RECENT_LIMIT + 10 {
            let mut snapshot = frozen.clone();
            snapshot.info.task_id = id as u64;
            reports.lock().remember(CompletedTask {
                finished_at: now,
                snapshot,
            });
        }
        assert_eq!(reports.lock().recent.len(), RECENT_LIMIT);
        assert_eq!(
            reports.lock().recent.front().unwrap().snapshot.info.task_id,
            10
        );
        reports.lock().expire(now + RECENT_TTL);
        assert!(reports.lock().recent.is_empty());
    }

    #[test]
    fn test_report_limits_running_rows_and_expanded_details() {
        let reports = reports();
        let now = Instant::now();
        let contexts = (0..RUNNING_LIMIT + 10)
            .map(|id| context_at(id as u64, None, now))
            .collect::<Vec<_>>();
        for context in &contexts {
            reports
                .lock()
                .running
                .insert(context.task.info.task_id, Arc::downgrade(&context.task));
        }
        let report = render_report(&reports, now);
        assert!(report.contains("Omitted running tasks: 10"));
        assert_eq!(
            report.matches(" group=23 phase=").count(),
            TASK_DETAIL_LIMIT
        );
        assert!(report.len() <= REPORT_BYTES_LIMIT);
    }

    #[test]
    fn test_report_formats_client_waits_and_cumulative_units() {
        let now = Instant::now();
        let mut context = context_at(42, None, now - Duration::from_secs(420));
        let task = Arc::get_mut(&mut context.task).unwrap();
        task.info.input_ssts = 128;
        task.info.input_bytes = 1024 * 1024 * 1024;
        task.progress.inc_progress_key(4_000_000);
        for _ in 0..7 {
            task.progress.inc_ssts_sealed();
        }
        for _ in 0..6 {
            task.progress.inc_ssts_uploaded();
        }
        for _ in 0..16 {
            task.progress.inc_num_pending_read_io();
        }
        for _ in 0..2 {
            task.progress.inc_num_pending_write_io();
        }
        let mut state = task.state.lock();
        state.phase = "execute_normal";
        for id in 0..4 {
            let mut shard = Shard::new(now - Duration::from_secs(410));
            if id < 3 {
                shard.finished = Some(Duration::from_secs(200));
            } else {
                shard.blocks = 4096;
                shard.retries = 2;
                shard.last_progress = Some(now - Duration::from_secs(60));
                shard.io.insert(
                    Operation::ReadStream,
                    IoSummary {
                        completed: 65536,
                        failed: 2,
                        bytes: 256 * 1024 * 1024,
                        total_ms: 720_000,
                        max_ms: 60_000,
                        ..Default::default()
                    },
                );
                shard.io.insert(
                    Operation::WriteUpload,
                    IoSummary {
                        completed: 3072,
                        bytes: 192 * 1024 * 1024,
                        total_ms: 15_000,
                        max_ms: 200,
                        ..Default::default()
                    },
                );
                shard.io.insert(
                    Operation::FinishUpload,
                    IoSummary {
                        completed: 6,
                        total_ms: 90_000,
                        max_ms: 30_000,
                        ..Default::default()
                    },
                );
                shard.active.insert(
                    0,
                    PendingIo {
                        operation: Operation::FinishUpload,
                        object_id: Some(HummockSstableObjectId::from(9008)),
                        started: now - Duration::from_secs(120),
                    },
                );
                shard.active.insert(
                    1,
                    PendingIo {
                        operation: Operation::ReadStream,
                        object_id: Some(HummockSstableObjectId::from(7001)),
                        started: now - Duration::from_secs(60),
                    },
                );
            }
            state.splits.insert(id, Arc::new(Mutex::new(shard)));
        }
        drop(state);
        let reports = reports();
        reports
            .lock()
            .running
            .insert(42, Arc::downgrade(&context.task));
        let report = render_report(&reports, now);
        assert!(report.contains("420.000 128 1024.000 256.000 192.000 3/4"));
        assert!(report.contains("last_io_or_block_progress_age_ms=60000"));
        assert!(report.contains("ReadStream 65536 2 0 268435456 720000 60000"));
        assert!(
            report.contains("operation=FinishUpload object=Some(9008) split=Some(3) age_ms=120000")
        );
        // An opt-in --nocapture run supplies an example rendered by the actual formatter.
        println!("BEGIN_COMPACTOR_REPORT_SAMPLE");
        println!("{report}");
        println!("END_COMPACTOR_REPORT_SAMPLE");
    }

    #[tokio::test]
    async fn test_other_storage_work_is_not_instrumented() {
        assert_eq!(
            observe(Operation::ReadStream, None, async { Ok::<_, ()>(7) }, |_| 0).await,
            Ok(7)
        );
        spawn_upload(HummockSstableObjectId::from(99), async {
            assert!(CURRENT.try_with(|_| ()).is_err());
        })
        .await
        .unwrap();
    }
}
