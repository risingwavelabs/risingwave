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
//! This module owns G credits,
//! pending groups, and admission waiters; `execution` owns P reservations, backend
//! reads, and completion. Keep compatible waiters visible to pending merges so that
//! backpressure and task handoff do not unnecessarily split a physical GET.

use std::collections::{BTreeMap, HashMap};
use std::ops::{Bound, Range};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker, ready};
use std::time::Instant;

use bytes::Bytes;
use futures::future::BoxFuture;
use risingwave_common::config::storage::ReadSchedulerConfig;
use tokio::sync::{AcquireError, Notify, OwnedSemaphorePermit, Semaphore, oneshot};

use super::{ObjectError, ObjectRangeBounds, ObjectResult, ObjectStoreMetrics};

mod execution;

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
/// A submission must be polled or dropped once the semaphore grants its credit.
/// Compatible arrivals may join an existing group without consuming another G credit.
pub(super) struct ReadScheduler {
    _shutdown: oneshot::Sender<()>,
    groups: Arc<GroupQueue>,
    config: ReadSchedulerConfig,
    pending: prometheus::IntGauge,
    waiting: prometheus::IntGauge,
    queued: prometheus::IntGauge,
    admission_duration: prometheus::Histogram,
}

/// Standalone reads keep the backend's exact bounds and never participate in merging or reuse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ReadRange {
    Mergeable(Range<usize>),
    Standalone((Bound<usize>, Bound<usize>)),
}

impl ReadRange {
    fn mergeable(&self) -> Option<&Range<usize>> {
        match self {
            Self::Mergeable(range) => Some(range),
            Self::Standalone(_) => None,
        }
    }

    pub(super) fn into_bounds(self) -> (Bound<usize>, Bound<usize>) {
        match self {
            Self::Mergeable(range) => (Bound::Included(range.start), Bound::Excluded(range.end)),
            Self::Standalone(bounds) => bounds,
        }
    }
}

struct StagedRead {
    path: String,
    range: ReadRange,
    response: oneshot::Sender<ReadResult>,

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

            _pending: ReadGauge::new(pending),
            _queued: queued.map(ReadGauge::new),
        }
    }
}

struct ReadRequest {
    path: String,
    range: ReadRange,
    response: oneshot::Sender<ReadResult>,
    queued_at: Instant,

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
    span: ReadRange,
    group: ReadGroup,
    _queued: ReadGauge,
}

impl PendingReadGroup {
    fn new(
        queue: &GroupQueue,
        credit: OwnedSemaphorePermit,
        span: ReadRange,
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
        let span = self.span.mergeable()?;
        let start = span.start.min(range.start);
        let end = span.end.max(range.end);
        let gap = span
            .start
            .max(range.start)
            .saturating_sub(span.end.min(range.end));
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
                other.span.mergeable()?,
                other.group.requests.len(),
                config,
            )
            .map(|span| (i, span))
        }) {
            self.span = ReadRange::Mergeable(span);
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
    waiting: HashMap<String, BTreeMap<WaiterKey, WaitingRead>>,
    next_waiter: u64,
    // Physical tasks holding a P permit that have not taken a group yet.
    starting: usize,
    reported_ready: usize,
}

// Standalone reads sort outside every range lookup.
type WaiterKey = (Option<usize>, u64);

struct WaitingRead {
    read: StagedRead,
    // Keep the acquire with the entry so joining also cancels its G request,
    // including a credit already assigned by the semaphore but not yet polled.
    acquire: BoxFuture<'static, Result<OwnedSemaphorePermit, AcquireError>>,
    waker: Option<Waker>,
}

// The submitting future owns the registration, even when it is cancelled.
struct GroupWaiter<'a> {
    queue: &'a GroupQueue,
    path: String,
    key: WaiterKey,
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

    fn register_waiter<'a>(&mut self, queue: &'a GroupQueue, read: StagedRead) -> GroupWaiter<'a> {
        let key = (
            read.range.mergeable().map(|range| range.start),
            self.next_waiter,
        );
        self.next_waiter += 1;
        let path = read.path.clone();
        self.waiting.entry(path.clone()).or_default().insert(
            key,
            WaitingRead {
                read,
                acquire: Box::pin(queue.slots.clone().acquire_owned()),
                waker: None,
            },
        );
        GroupWaiter { queue, path, key }
    }

    fn remove_waiter(&mut self, path: &str, key: WaiterKey) -> Option<WaitingRead> {
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
    ) -> Option<(Range<usize>, WaitingRead)> {
        if group.group.requests.len() == config.max_batch_requests.get() {
            return None;
        }
        let path = &group.group.requests[0].path;
        let span = group.span.mergeable()?;
        // An earlier start would exceed the size limit even if the waiter ends inside the group.
        let left = span.end.saturating_sub(config.max_read_bytes.get());
        let right = span.end.saturating_add(config.max_gap_bytes);
        let (key, merged_span) = self
            .waiting
            .get(path)?
            .range((Some(left), 0)..=(Some(right), u64::MAX))
            .find_map(|(&key, waiter)| {
                let merged_span =
                    group.merge_span(path, waiter.read.range.mergeable()?, 1, config)?;
                Some((key, merged_span))
            })?;
        Some((merged_span, self.remove_waiter(path, key).unwrap()))
    }

    fn take_covered_waiter(&mut self, path: &str, span: &Range<usize>) -> Option<WaitingRead> {
        let key = self
            .waiting
            .get(path)?
            .range((Some(span.start), 0)..(Some(span.end), 0))
            .find_map(|(&key, waiter)| {
                (waiter.read.range.mergeable()?.end <= span.end).then_some(key)
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
        range: &ReadRange,
        reserved: Option<OwnedSemaphorePermit>,
        config: &ReadSchedulerConfig,
    ) -> Option<GroupAdmission> {
        if self.slots.is_closed() {
            return None;
        }
        if let Some(range) = range.mergeable() {
            for (index, group) in pending.iter().enumerate() {
                if let Some(span) = group.merge_span(path, range, 1, config) {
                    return Some(GroupAdmission::Join(index, span));
                }
            }
        }
        reserved
            .or_else(|| self.slots.clone().try_acquire_owned().ok())
            .map(GroupAdmission::New)
    }

    fn admit(
        &self,
        pending: &mut Vec<PendingReadGroup>,
        admission: GroupAdmission,
        request: ReadRequest,
        config: &ReadSchedulerConfig,
    ) -> PendingReadGroup {
        match admission {
            GroupAdmission::New(credit) => {
                PendingReadGroup::new(self, credit, request.range.clone(), vec![request])
            }
            GroupAdmission::Join(index, span) => {
                let mut group = pending.swap_remove(index);
                group.span = ReadRange::Mergeable(span);
                group.group.requests.push(request);
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
        drop(pending);
        let mut group = group?;
        group
            .requests
            .sort_unstable_by_key(|r| r.range.mergeable().map(|range| range.start));
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
    pub(super) fn plan(&self, range: &impl ObjectRangeBounds) -> (&'static str, ReadRange) {
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        let route = match bounds {
            (Bound::Included(start), Bound::Excluded(end)) if start < end => {
                if end - start <= self.config.max_read_bytes.get() {
                    return ("scheduled", ReadRange::Mergeable(start..end));
                }
                "scheduled_oversized"
            }
            _ => "scheduled_other",
        };
        (route, ReadRange::Standalone(bounds))
    }

    pub(super) fn new<F, R>(
        metrics: &ObjectStoreMetrics,
        media_type: &'static str,
        config: ReadSchedulerConfig,
        read: F,
    ) -> Self
    where
        F: Fn(String, ReadRange) -> R + Send + Sync + 'static,
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
        }
    }

    // Box only the cold object-read admission path; do not grow cache-facing futures.
    pub(super) fn submit_plan<'a>(
        &'a self,
        path: &'a str,
        range: ReadRange,
    ) -> impl Future<Output = ObjectResult<oneshot::Receiver<ReadResult>>> + Send + 'a {
        Box::pin(async move {
            let groups = &self.groups;
            let (read, response) = self.stage_read(path, range);
            let waiter = {
                let mut state = groups.pending.lock().unwrap();
                let Some(read) = self.try_submit(&mut state, read, None) else {
                    return Ok(response);
                };
                state.register_waiter(groups, read)
            };
            self.wait_for_group(waiter).await?;
            Ok(response)
        })
    }

    fn stage_read(
        &self,
        path: &str,
        range: ReadRange,
    ) -> (StagedRead, oneshot::Receiver<ReadResult>) {
        let (response, receiver) = oneshot::channel();
        (
            StagedRead {
                path: path.to_owned(),
                range,
                response,

                _waiting: ReadGauge::new(&self.waiting),
                _admission: self.admission_duration.start_timer(),
            },
            receiver,
        )
    }

    // Return the read only when it still needs a group credit.
    fn try_submit(
        &self,
        state: &mut PendingReadGroups,
        read: StagedRead,
        reserved: Option<OwnedSemaphorePermit>,
    ) -> Option<StagedRead> {
        let read = self.try_share_read(state, read)?;
        let Some(admission) = self.groups.admission(
            &state.groups,
            &read.path,
            &read.range,
            reserved,
            &self.config,
        ) else {
            return Some(read);
        };
        let request = read.into_request(&self.pending, Some(&self.queued));
        let mut group = self
            .groups
            .admit(&mut state.groups, admission, request, &self.config);
        // Take compatible waiters before publishing the group. Waking them to
        // retry would race the worker taking the group for dispatch.
        while let Some((
            span,
            WaitingRead {
                read,
                acquire,
                waker,
            },
        )) = state.take_mergeable_waiter(&group, &self.config)
        {
            drop(acquire);
            // A covering flight may have freed a follower slot since registration.
            if let Some(read) = self.try_share_read(state, read) {
                group.span = ReadRange::Mergeable(span);
                group
                    .group
                    .requests
                    .push(read.into_request(&self.pending, Some(&self.queued)));
                group.absorb_pending(&mut state.groups, &self.config);
            }
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        state.groups.push(group);
        state.update_ready(&self.groups);
        self.groups.ready.notify_one();
        None
    }

    // Return ownership if no active GET can take this follower.
    fn try_share_read(&self, state: &PendingReadGroups, read: StagedRead) -> Option<StagedRead> {
        let Some(range) = read.range.mergeable() else {
            return Some(read);
        };
        let Some(mut followers) =
            state.covering_read(&read.path, range, 1, self.config.max_batch_requests.get())
        else {
            return Some(read);
        };
        followers
            .as_mut()
            .unwrap()
            .push(read.into_request(&self.pending, None));
        None
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
            // The submitting future holds the receiver until admission. Its
            // GroupWaiter removes this entry if the future is dropped.
            waiting
                .waker
                .get_or_insert_with(|| cx.waker().clone())
                .clone_from(cx.waker());
            let reserved = ready!(waiting.acquire.as_mut().poll(cx)).ok();
            let read = state.remove_waiter(&waiter.path, waiter.key).unwrap().read;
            if self.try_submit(&mut state, read, reserved).is_some() {
                return Poll::Ready(Err(ObjectError::internal("read scheduler stopped")));
            }
            Poll::Ready(Ok(()))
        })
        .await
    }
}

#[cfg(test)]
mod tests;
