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

//! Object read admission and pending-range coalescing.
//!
//! `batch` briefly plugs actual reads from one iterator window before admission. This module owns G credits,
//! pending groups, and admission waiters; `execution` owns P reservations, backend
//! reads, and completion. Keep compatible waiters visible to pending merges so that
//! backpressure and task handoff do not unnecessarily split a physical GET.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker, ready};
use std::time::Instant;

use bytes::Bytes;
use futures::future::{BoxFuture, join_all};
use risingwave_common::config::storage::ReadSchedulerConfig;
use tokio::sync::{AcquireError, Notify, OwnedSemaphorePermit, Semaphore, oneshot};

use super::{ObjectError, ObjectResult, ObjectStoreMetrics};

#[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
pub mod replay;
#[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
use replay::PhysicalPermit;
#[cfg(not(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay")))]
type PhysicalPermit = OwnedSemaphorePermit;

mod batch;
mod execution;

pub use batch::{ReadPlug, ReadPlugToken};
use execution::run;

pub(super) type ReadResult = ObjectResult<Bytes>;

/// Process-owned runtime, initialized only by the first scheduler that opts in.
#[cfg(not(madsim))]
fn dedicated_io_runtime(workers: usize) -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .thread_name("rw-read-io")
            .enable_all()
            .build()
            .expect("failed to build the object read scheduler runtime")
    });
    assert_eq!(
        runtime.metrics().num_workers(),
        workers,
        "conflicting dedicated_io_runtime_workers for the shared process runtime (existing, requested)",
    );
    runtime
}

/// G bounds admitted groups through completion; P bounds scheduled backend reads.
/// Staged window members and callers awaiting G are outside the admitted-group budget.
/// Compatible arrivals may join an existing group without consuming another G credit.
pub(super) struct ReadScheduler {
    _shutdown: oneshot::Sender<()>,
    groups: Arc<GroupQueue>,
    config: ReadSchedulerConfig,
    pending: prometheus::IntGauge,
    waiting: prometheus::IntGauge,
    queued: prometheus::IntGauge,
    admission_duration: prometheus::Histogram,
    batch_members: prometheus::IntCounter,
    // Single-read and multi-read flushes; preserve the existing metric labels.
    plug_flushes: [prometheus::IntCounter; 2],
}

struct StagedRead {
    path: String,
    range: Range<usize>,
    response: oneshot::Sender<ReadResult>,
    trace_id: Option<u64>,
    _waiting: ReadGauge,
    _admission: prometheus::HistogramTimer,
}

impl StagedRead {
    fn into_request(
        self,
        pending: &prometheus::IntGauge,
        queued: Option<&prometheus::IntGauge>,
    ) -> ReadRequest {
        ReadRequest {
            path: self.path,
            range: self.range,
            response: self.response,
            queued_at: Instant::now(),
            trace_id: self.trace_id,
            _pending: ReadGauge::new(pending),
            _queued: queued.map(ReadGauge::new),
        }
    }
}

fn staged_span(reads: &[StagedRead]) -> Option<Range<usize>> {
    let start = reads.first()?.range.start;
    let end = reads.iter().map(|read| read.range.end).max().unwrap();
    Some(start..end)
}

struct ReadRequest {
    path: String,
    range: Range<usize>,
    response: oneshot::Sender<ReadResult>,
    queued_at: Instant,
    trace_id: Option<u64>,
    _pending: ReadGauge,
    _queued: Option<ReadGauge>,
}

struct ReadGauge(prometheus::IntGauge);

impl ReadGauge {
    fn new(gauge: &prometheus::IntGauge) -> Self {
        gauge.inc();
        Self(gauge.clone())
    }
}

impl Drop for ReadGauge {
    fn drop(&mut self) {
        self.0.dec();
    }
}

// Keep cancellation ownership from spawn, including before the first join poll.
struct ReadTask(tokio::task::JoinHandle<()>);
impl Future for ReadTask {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}
impl Drop for ReadTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct ReadGroup {
    requests: Vec<ReadRequest>,
    _credit: (ReadGauge, OwnedSemaphorePermit),
}

struct PendingReadGroup {
    span: Range<usize>,
    group: ReadGroup,
    _queued: ReadGauge,
}

impl PendingReadGroup {
    fn new(
        queue: &GroupQueue,
        credit: OwnedSemaphorePermit,
        span: Range<usize>,
        requests: Vec<ReadRequest>,
    ) -> Self {
        Self {
            span,
            group: ReadGroup {
                requests,
                _credit: (ReadGauge::new(&queue.admitted), credit),
            },
            _queued: ReadGauge::new(&queue.queued),
        }
    }

    fn merge_span(
        &self,
        path: &str,
        range: &Range<usize>,
        members: usize,
        config: &ReadSchedulerConfig,
    ) -> Option<Range<usize>> {
        let start = self.span.start.min(range.start);
        let end = self.span.end.max(range.end);
        let gap = self
            .span
            .start
            .max(range.start)
            .saturating_sub(self.span.end.min(range.end));
        (self.group.requests[0].path == path
            && self.group.requests.len() + members <= config.max_batch_requests.get()
            && gap <= config.max_gap_bytes
            && end - start <= config.max_read_bytes.get())
        .then_some(start..end)
    }

    fn absorb_pending(&mut self, pending: &mut Vec<Self>, config: &ReadSchedulerConfig) {
        // An arriving range can bridge two pending groups. Dropping the
        // absorbed group returns its credit immediately.
        while let Some((index, span)) = pending.iter().enumerate().find_map(|(i, other)| {
            self.merge_span(
                &other.group.requests[0].path,
                &other.span,
                other.group.requests.len(),
                config,
            )
            .map(|span| (i, span))
        }) {
            self.span = span;
            self.group
                .requests
                .append(&mut pending.swap_remove(index).group.requests);
        }
    }
}

enum GroupAdmission {
    Join(usize, Range<usize>),
    New(OwnedSemaphorePermit),
}

struct GroupQueue {
    pending: Mutex<PendingReadGroups>,
    slots: Arc<Semaphore>,
    ready: Notify,
    admitted: prometheus::IntGauge,
    queued: prometheus::IntGauge,
    ready_groups: prometheus::IntGauge,
}

struct ActiveRead {
    path: String,
    span: Range<usize>,
    // None means the result or cancellation has closed the flight.
    followers: Mutex<Option<Vec<ReadRequest>>>,
}

impl ActiveRead {
    fn covers(&self, path: &str, span: &Range<usize>) -> bool {
        self.path == path && self.span.start <= span.start && span.end <= self.span.end
    }
}

#[derive(Default)]
// The queue mutex owns pending groups, G waiters, flight registration, and dispatch claims.
// Waiters can be absorbed before they obtain G. A reserved P does not freeze a group's
// range: start_read removes it from this queue; execution may still trim cancelled ends.
struct PendingReadGroups {
    groups: Vec<PendingReadGroup>,
    active: Vec<Arc<ActiveRead>>,
    waiting: HashMap<String, BTreeMap<(usize, u64), WaitingReadGroup>>,
    next_waiter: u64,
    // Physical tasks holding a P permit that have not taken a group yet.
    starting: usize,
    reported_ready: usize,
}

struct WaitingReadGroup {
    reads: Vec<StagedRead>,
    // Keep the acquire with the entry so joining also cancels its G request,
    // including a credit already assigned by the semaphore but not yet polled.
    acquire: BoxFuture<'static, Result<OwnedSemaphorePermit, AcquireError>>,
    waker: Option<Waker>,
}

// The submitting future owns the registration, even when it is cancelled.
struct GroupWaiter<'a> {
    queue: &'a GroupQueue,
    path: String,
    key: (usize, u64),
}

impl Drop for GroupWaiter<'_> {
    fn drop(&mut self) {
        self.queue
            .pending
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .remove_waiter(&self.path, self.key);
    }
}

impl PendingReadGroups {
    fn ready_count(&self) -> usize {
        self.groups.len().saturating_sub(self.starting)
    }

    // Publish this queue's contribution because schedulers can share labels.
    fn update_ready(&mut self, queue: &GroupQueue) {
        let ready = self.ready_count();
        let change = ready as i64 - self.reported_ready as i64;
        if change != 0 {
            queue.ready_groups.add(change);
            self.reported_ready = ready;
        }
    }

    fn register_waiter<'a>(
        &mut self,
        queue: &'a GroupQueue,
        reads: Vec<StagedRead>,
    ) -> GroupWaiter<'a> {
        let key = (reads[0].range.start, self.next_waiter);
        self.next_waiter += 1;
        let path = reads[0].path.clone();
        self.waiting.entry(path.clone()).or_default().insert(
            key,
            WaitingReadGroup {
                reads,
                acquire: Box::pin(queue.slots.clone().acquire_owned()),
                waker: None,
            },
        );
        GroupWaiter { queue, path, key }
    }

    fn remove_waiter(&mut self, path: &str, key: (usize, u64)) -> Option<WaitingReadGroup> {
        let waiters = self.waiting.get_mut(path)?;
        let removed = waiters.remove(&key);
        if waiters.is_empty() {
            self.waiting.remove(path);
        }
        removed
    }

    fn take_mergeable_waiter(
        &mut self,
        group: &PendingReadGroup,
        config: &ReadSchedulerConfig,
    ) -> Option<(Range<usize>, WaitingReadGroup)> {
        if group.group.requests.len() == config.max_batch_requests.get() {
            return None;
        }
        let path = &group.group.requests[0].path;
        // The original start can shift right on cancellation, by at most the
        // byte limit for a group. Anything earlier cannot reach the left edge.
        let left = group
            .span
            .start
            .saturating_sub(config.max_gap_bytes)
            .saturating_sub(config.max_read_bytes.get());
        let right = group.span.end.saturating_add(config.max_gap_bytes);
        let (key, merged_span) = self
            .waiting
            .get_mut(path)?
            .range_mut((left, 0)..=(right, u64::MAX))
            .find_map(|(&key, waiter)| {
                waiter.reads.retain(|read| !read.response.is_closed());
                let span = staged_span(&waiter.reads)?;
                let merged_span = group.merge_span(path, &span, waiter.reads.len(), config)?;
                // Preserve active reuse without taking a follower lock in inline seal.
                if self.active.iter().any(|read| read.covers(path, &span)) {
                    return None;
                }
                Some((key, merged_span))
            })?;
        Some((merged_span, self.remove_waiter(path, key).unwrap()))
    }

    fn take_covered_waiter(
        &mut self,
        path: &str,
        span: &Range<usize>,
        available: usize,
        max_read_bytes: usize,
    ) -> Option<WaitingReadGroup> {
        // Cancellation can remove the original first member and shift the start right.
        let left = span.start.saturating_sub(max_read_bytes);
        let key = self
            .waiting
            .get_mut(path)?
            .range_mut((left, 0)..(span.end, 0))
            .find_map(|(&key, waiter)| {
                waiter.reads.retain(|read| !read.response.is_closed());
                let waiting_span = staged_span(&waiter.reads)?;
                (waiter.reads.len() <= available
                    && span.start <= waiting_span.start
                    && waiting_span.end <= span.end)
                    .then_some(key)
            })?;
        self.remove_waiter(path, key)
    }

    // Observe existing flights only; this must never affect admission or reuse.
    fn partial_overlap_bytes(&self, path: &str, span: &Range<usize>) -> usize {
        let mut overlaps = Vec::new();
        for read in &self.active {
            if read.path != path || read.covers(path, span) {
                continue;
            }
            let start = read.span.start.max(span.start);
            let end = read.span.end.min(span.end);
            if start < end && read.followers.lock().unwrap().is_some() {
                overlaps.push(start..end);
            }
        }
        overlaps.sort_unstable_by_key(|range| range.start);
        let mut end = 0;
        let mut bytes = 0;
        for range in overlaps {
            bytes += range.end.saturating_sub(range.start.max(end));
            end = end.max(range.end);
        }
        bytes
    }

    fn covering_read(
        &self,
        path: &str,
        range: &Range<usize>,
        count: usize,
        max_followers: usize,
    ) -> Option<std::sync::MutexGuard<'_, Option<Vec<ReadRequest>>>> {
        self.active.iter().find_map(|read| {
            if !read.covers(path, range) {
                return None;
            }
            let mut followers = read.followers.lock().unwrap();
            let requests = followers.as_mut()?;
            requests.retain(|request| !request.response.is_closed());
            (requests.len() + count <= max_followers).then_some(followers)
        })
    }
}

impl GroupQueue {
    fn new(metrics: &ObjectStoreMetrics, media_type: &str, config: ReadSchedulerConfig) -> Self {
        Self {
            pending: Mutex::default(),
            slots: Arc::new(Semaphore::new(config.max_admitted_groups.get())),
            ready: Notify::new(),
            admitted: metrics
                .read_scheduler_groups
                .with_label_values(&[media_type, "admitted"]),
            queued: metrics
                .read_scheduler_groups
                .with_label_values(&[media_type, "queued"]),
            ready_groups: metrics
                .read_scheduler_groups
                .with_label_values(&[media_type, "ready"]),
        }
    }

    fn admission(
        &self,
        pending: &[PendingReadGroup],
        path: &str,
        range: &Range<usize>,
        members: usize,
        reserved: &mut Option<OwnedSemaphorePermit>,
        config: &ReadSchedulerConfig,
    ) -> Option<GroupAdmission> {
        if self.slots.is_closed() {
            return None;
        }
        for (index, group) in pending.iter().enumerate() {
            if let Some(span) = group.merge_span(path, range, members, config) {
                return Some(GroupAdmission::Join(index, span));
            }
        }
        reserved
            .take()
            .or_else(|| self.slots.clone().try_acquire_owned().ok())
            .map(GroupAdmission::New)
    }

    fn admit(
        &self,
        pending: &mut Vec<PendingReadGroup>,
        admission: GroupAdmission,
        span: Range<usize>,
        requests: impl IntoIterator<Item = ReadRequest>,
        config: &ReadSchedulerConfig,
    ) -> PendingReadGroup {
        match admission {
            GroupAdmission::New(credit) => {
                PendingReadGroup::new(self, credit, span, requests.into_iter().collect())
            }
            GroupAdmission::Join(index, span) => {
                let mut group = pending.swap_remove(index);
                group.span = span;
                group.group.requests.extend(requests);
                group.absorb_pending(pending, config);
                group
            }
        }
    }

    fn start_read(&self) -> Option<ReadGroup> {
        let mut pending = self.pending.lock().unwrap();
        pending.starting -= 1;
        let index = pending
            .groups
            .iter()
            .enumerate()
            .min_by_key(|(_, group)| group.group.requests.iter().map(|r| r.queued_at).min())
            .map(|(index, _)| index);
        let group = index.map(|index| pending.groups.swap_remove(index).group);
        pending.update_ready(self);
        let mut group = group?;
        #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
        replay::queue(&pending, "selected");
        group.requests.sort_unstable_by_key(|r| r.range.start);
        Some(group)
    }
}

// The worker owns queued reads even though producers can modify the queue.
struct GroupQueueWorker(Arc<GroupQueue>);

impl Drop for GroupQueueWorker {
    fn drop(&mut self) {
        let mut pending = self.0.pending.lock().unwrap_or_else(|err| err.into_inner());
        self.0.slots.close();
        pending.groups.clear();
        pending.update_ready(&self.0);
    }
}

impl ReadScheduler {
    // Keep scheduler waits on the same executor as physical reads when opted in.
    fn spawn_background(
        &self,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> tokio::task::JoinHandle<()> {
        #[cfg(not(madsim))]
        if self.config.use_dedicated_io_runtime {
            return dedicated_io_runtime(self.config.dedicated_io_runtime_workers.get())
                .spawn(task);
        }
        tokio::spawn(task)
    }

    pub(super) fn can_schedule(&self, range: &Range<usize>) -> bool {
        !range.is_empty() && range.len() <= self.config.max_read_bytes.get()
    }

    pub(super) fn new<F, R>(
        metrics: &ObjectStoreMetrics,
        media_type: &'static str,
        config: ReadSchedulerConfig,
        read: F,
    ) -> Self
    where
        F: Fn(String, Range<usize>) -> R + Send + Sync + 'static,
        R: Future<Output = ReadResult> + Send + 'static,
    {
        #[cfg(not(madsim))]
        let dedicated = config
            .use_dedicated_io_runtime
            .then(|| dedicated_io_runtime(config.dedicated_io_runtime_workers.get()));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let groups = Arc::new(GroupQueue::new(metrics, media_type, config));
        let worker = run(
            shutdown_rx,
            GroupQueueWorker(groups.clone()),
            read,
            config,
            metrics.clone(),
            media_type,
        );
        #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
        let worker = replay::dispatcher(worker);
        // The dispatcher spawns physical reads on its current runtime.
        // Range fallback stays inside the same read task.
        #[cfg(not(madsim))]
        if let Some(runtime) = dedicated {
            runtime.spawn(worker);
        } else {
            tokio::spawn(worker);
        }
        #[cfg(madsim)]
        tokio::spawn(worker);
        Self {
            _shutdown: shutdown,
            groups,
            config,
            pending: metrics
                .read_scheduler_pending
                .with_label_values(&[media_type]),
            queued: metrics
                .read_scheduler_queued
                .with_label_values(&[media_type]),
            waiting: metrics
                .read_scheduler_waiting
                .with_label_values(&[media_type]),
            admission_duration: metrics
                .read_scheduler_admission_duration
                .with_label_values(&[media_type]),
            batch_members: metrics
                .read_scheduler_events
                .with_label_values(&[media_type, "submitted_batch_members"]),
            plug_flushes: ["sealed_batch_single", "sealed_batch_multiple"].map(|event| {
                metrics
                    .read_scheduler_events
                    .with_label_values(&[media_type, event])
            }),
        }
    }

    // Box only the cold object-read admission path; do not grow cache-facing futures.
    pub(super) fn submit<'a>(
        &'a self,
        path: &'a str,
        range: Range<usize>,
        trace_id: Option<u64>,
    ) -> impl Future<Output = ObjectResult<oneshot::Receiver<ReadResult>>> + Send + 'a {
        Box::pin(async move {
            debug_assert!(self.can_schedule(&range));
            let groups = &self.groups;
            let (read, response) = self.stage_read(path, range.clone(), trace_id);
            let waiter = {
                let mut state = groups.pending.lock().unwrap();
                if let Some(mut followers) =
                    state.covering_read(path, &range, 1, self.config.max_batch_requests.get())
                {
                    followers
                        .as_mut()
                        .unwrap()
                        .push(read.into_request(&self.pending, None));
                    return Ok(response);
                }
                if let Some(admission) =
                    groups.admission(&state.groups, path, &range, 1, &mut None, &self.config)
                {
                    self.insert_group(&mut state, admission, range, std::iter::once(read));
                    return Ok(response);
                }
                state.register_waiter(groups, vec![read])
            };
            self.wait_for_group(waiter).await?;
            Ok(response)
        })
    }

    fn stage_read(
        &self,
        path: &str,
        range: Range<usize>,
        trace_id: Option<u64>,
    ) -> (StagedRead, oneshot::Receiver<ReadResult>) {
        let (response, receiver) = oneshot::channel();
        (
            StagedRead {
                path: path.to_owned(),
                range,
                response,
                trace_id,
                _waiting: ReadGauge::new(&self.waiting),
                _admission: self.admission_duration.start_timer(),
            },
            receiver,
        )
    }

    fn insert_group(
        &self,
        state: &mut PendingReadGroups,
        admission: GroupAdmission,
        span: Range<usize>,
        reads: impl IntoIterator<Item = StagedRead>,
    ) {
        let requests = reads
            .into_iter()
            .map(|read| read.into_request(&self.pending, Some(&self.queued)));
        let mut group =
            self.groups
                .admit(&mut state.groups, admission, span, requests, &self.config);
        // Take compatible waiters before publishing the group. Waking them to
        // retry would race the worker taking the group for dispatch.
        while let Some((
            span,
            WaitingReadGroup {
                reads,
                acquire,
                waker,
            },
        )) = state.take_mergeable_waiter(&group, &self.config)
        {
            drop(acquire);
            group.span = span;
            group.group.requests.extend(
                reads
                    .into_iter()
                    .map(|read| read.into_request(&self.pending, Some(&self.queued))),
            );
            group.absorb_pending(&mut state.groups, &self.config);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        state.groups.push(group);
        state.update_ready(&self.groups);
        #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
        replay::queue(state, "installed");
        self.groups.ready.notify_one();
    }

    fn submit_batch(self: &Arc<Self>, mut reads: Vec<StagedRead>) {
        self.batch_members.inc_by(reads.len() as u64);
        reads.retain(|read| !read.response.is_closed());
        reads.sort_unstable_by(|a, b| (&a.path, a.range.start).cmp(&(&b.path, b.range.start)));
        let mut groups: Vec<(Range<usize>, Vec<StagedRead>)> = Vec::new();
        for read in reads {
            if let Some((span, members)) = groups.last_mut()
                && members[0].path == read.path
                && members.len() < self.config.max_batch_requests.get()
                && read.range.start.saturating_sub(span.end) <= self.config.max_gap_bytes
                && span.end.max(read.range.end) - span.start <= self.config.max_read_bytes.get()
            {
                span.end = span.end.max(read.range.end);
                members.push(read);
            } else {
                groups.push((read.range.clone(), vec![read]));
            }
        }
        let mut deferred = Vec::new();
        for (span, reads) in groups {
            let Ok(mut state) = self.groups.pending.try_lock() else {
                deferred.push(reads);
                continue;
            };
            // The follower lock belongs to the physical task. Leave covering
            // reads to the background path to preserve in-flight reuse.
            if state
                .active
                .iter()
                .any(|read| read.covers(&reads[0].path, &span))
            {
                deferred.push(reads);
                continue;
            }
            if let Err(reads) = self.try_admit_group(&mut state, span, reads, &mut None) {
                deferred.push(reads);
            }
        }
        if !deferred.is_empty() {
            let scheduler = self.clone();
            self.spawn_background(async move {
                // Each group can progress even when another needs a new G permit.
                join_all(
                    deferred
                        .into_iter()
                        .map(|reads| scheduler.submit_group(reads)),
                )
                .await;
            });
        }
    }

    fn try_admit_group(
        &self,
        state: &mut PendingReadGroups,
        span: Range<usize>,
        reads: Vec<StagedRead>,
        reserved: &mut Option<OwnedSemaphorePermit>,
    ) -> Result<(), Vec<StagedRead>> {
        let Some(admission) = self.groups.admission(
            &state.groups,
            &reads[0].path,
            &span,
            reads.len(),
            reserved,
            &self.config,
        ) else {
            return Err(reads);
        };
        self.insert_group(state, admission, span, reads);
        Ok(())
    }

    fn try_reuse_group(
        &self,
        state: &PendingReadGroups,
        span: &Range<usize>,
        reads: Vec<StagedRead>,
    ) -> Result<(), Vec<StagedRead>> {
        let Some(mut followers) = state.covering_read(
            &reads[0].path,
            span,
            reads.len(),
            self.config.max_batch_requests.get(),
        ) else {
            return Err(reads);
        };
        followers.as_mut().unwrap().extend(
            reads
                .into_iter()
                .map(|read| read.into_request(&self.pending, None)),
        );
        Ok(())
    }

    async fn submit_group(&self, mut reads: Vec<StagedRead>) {
        reads.retain(|read| !read.response.is_closed());
        let Some(span) = staged_span(&reads) else {
            return;
        };
        let waiter = {
            let mut state = self.groups.pending.lock().unwrap();
            let Err(reads) = self.try_reuse_group(&state, &span, reads) else {
                return;
            };
            let Err(reads) = self.try_admit_group(&mut state, span, reads, &mut None) else {
                return;
            };
            state.register_waiter(&self.groups, reads)
        };
        // The result channel carries shutdown errors to each staged caller.
        let _ = self.wait_for_group(waiter).await;
    }

    async fn wait_for_group(&self, waiter: GroupWaiter<'_>) -> ObjectResult<()> {
        std::future::poll_fn(|cx| {
            let mut state = self.groups.pending.lock().unwrap();
            let Some(waiting) = state
                .waiting
                .get_mut(&waiter.path)
                .and_then(|groups| groups.get_mut(&waiter.key))
            else {
                return Poll::Ready(Ok(()));
            };
            if waiting
                .reads
                .iter_mut()
                .all(|read| read.response.poll_closed(cx).is_ready())
            {
                state.remove_waiter(&waiter.path, waiter.key);
                return Poll::Ready(Ok(()));
            }
            waiting
                .waker
                .get_or_insert_with(|| cx.waker().clone())
                .clone_from(cx.waker());
            let mut reserved = ready!(waiting.acquire.as_mut().poll(cx)).ok();
            let mut reads = state.remove_waiter(&waiter.path, waiter.key).unwrap().reads;
            reads.retain(|read| !read.response.is_closed());
            let Some(span) = staged_span(&reads) else {
                return Poll::Ready(Ok(()));
            };
            let Err(reads) = self.try_reuse_group(&state, &span, reads) else {
                return Poll::Ready(Ok(()));
            };
            if let Err(reads) = self.try_admit_group(&mut state, span, reads, &mut reserved) {
                for read in reads {
                    let _ = read
                        .response
                        .send(Err(ObjectError::internal("read scheduler stopped")));
                }
                return Poll::Ready(Err(ObjectError::internal("read scheduler stopped")));
            }
            Poll::Ready(Ok(()))
        })
        .await
    }
}

#[cfg(test)]
mod tests;
