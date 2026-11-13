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

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker, ready};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::{BoxFuture, join_all};
use futures::stream::{FuturesUnordered, StreamExt};
use risingwave_common::config::storage::ReadSchedulerConfig;
use tokio::sync::{AcquireError, Notify, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::time::Instant as BatchInstant;
use tracing::Instrument;

use super::{ObjectError, ObjectErrorInner, ObjectResult, ObjectStoreMetrics};

#[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
pub mod replay;
#[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
use replay::PhysicalPermit;
#[cfg(not(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay")))]
type PhysicalPermit = OwnedSemaphorePermit;

pub(super) type ReadResult = ObjectResult<Bytes>;

pub(super) struct ReadScheduler {
    _shutdown: oneshot::Sender<()>,
    groups: Arc<GroupQueue>,
    config: ReadSchedulerConfig,
    pending: prometheus::IntGauge,
    waiting: prometheus::IntGauge,
    queued: prometheus::IntGauge,
    admission_duration: prometheus::Histogram,
    batch_members: prometheus::IntCounter,
    sealed_batches: [prometheus::IntCounter; 3],
    batch_releases: [prometheus::IntCounter; 3],
    batch_wait: prometheus::Histogram,
}

/// Collects external reads from one prefetch window on a single object.
pub struct ReadBatch {
    member: ReadBatchMember,
}

#[derive(Clone)]
pub struct ReadBatchMember(Arc<ReadBatchInner>);

struct ReadBatchInner {
    scheduler: Arc<ReadScheduler>,
    state: Mutex<ReadBatchState>,
    max_reads: usize,
}

enum ReadBatchState {
    Registering {
        candidates: Vec<Range<usize>>,
        reads: Vec<StagedRead>,
    },
    Sealed(Vec<BatchGroup>),
}

struct BatchGroup {
    span: Range<usize>,
    missing: Vec<Range<usize>>,
    reads: Vec<StagedRead>,
    timer: Option<(BatchInstant, ReadTask)>,
}

#[derive(Clone, Copy)]
enum BatchRelease {
    Ready,
    Deadline,
    Pressure,
}

impl ReadBatch {
    pub(super) fn new(scheduler: Arc<ReadScheduler>, max_reads: usize) -> Self {
        Self {
            member: ReadBatchMember(Arc::new(ReadBatchInner {
                scheduler,
                state: Mutex::new(ReadBatchState::Registering {
                    candidates: Vec::new(),
                    reads: Vec::new(),
                }),
                max_reads,
            })),
        }
    }

    pub fn member(&self) -> ReadBatchMember {
        self.member.clone()
    }

    /// Register a range that may still reach the store after registration ends.
    pub fn expect(&self, range: Range<usize>) {
        let batch = &self.member.0;
        if batch.scheduler.config.prefetch_batch_wait_us > 0
            && batch.scheduler.can_schedule(&range)
            && let ReadBatchState::Registering { candidates, .. } =
                &mut *batch.state.lock().unwrap()
            && candidates.len() < batch.max_reads
        {
            candidates.push(range);
        }
    }
}

impl Drop for ReadBatch {
    fn drop(&mut self) {
        // Cache producers can still have followers after the iterator leaves.
        let batch = &self.member.0;
        let mut state = batch.state.lock().unwrap();
        let ReadBatchState::Registering { candidates, reads } =
            std::mem::replace(&mut *state, ReadBatchState::Sealed(Vec::new()))
        else {
            unreachable!("the batch has only one registration owner")
        };
        batch.scheduler.sealed_batches[reads.len().min(2)].inc();
        if candidates.is_empty() {
            drop(state);
            if !reads.is_empty() {
                batch.scheduler.submit_batch(reads);
            }
            return;
        }
        let mut groups = batch.plan_groups(candidates);
        let mut ready = Vec::new();
        for read in reads {
            match groups
                .iter_mut()
                .find(|group| group.missing.contains(&read.range))
            {
                Some(group) => group.push(read),
                None => ready.push(read),
            }
        }
        groups.retain_mut(|group| {
            if group.reads.is_empty() {
                return true;
            }
            if let Some(reason) = batch.release_reason(group) {
                batch.observe_release(&group.reads, reason);
                ready.append(&mut group.reads);
                return false;
            }
            batch.arm_timeout(group);
            true
        });
        *state = ReadBatchState::Sealed(groups);
        drop(state);
        if !ready.is_empty() {
            batch.scheduler.submit_batch(ready);
        }
    }
}

struct StagedRead {
    path: String,
    range: Range<usize>,
    response: oneshot::Sender<ReadResult>,
    trace_id: Option<u64>,
    arrived: Option<BatchInstant>,
    _waiting: ReadGauge,
    _admission: prometheus::HistogramTimer,
}

impl StagedRead {
    fn into_request(self, scheduler: &ReadScheduler, queued: bool) -> ReadRequest {
        ReadRequest {
            path: self.path,
            range: self.range,
            response: self.response,
            queued_at: Instant::now(),
            trace_id: self.trace_id,
            _pending: ReadGauge::new(&scheduler.pending),
            _queued: queued.then(|| ReadGauge::new(&scheduler.queued)),
        }
    }
}

fn staged_span(reads: &[StagedRead]) -> Option<Range<usize>> {
    let start = reads.first()?.range.start;
    let end = reads.iter().map(|read| read.range.end).max().unwrap();
    Some(start..end)
}

impl BatchGroup {
    fn push(&mut self, read: StagedRead) {
        self.missing.retain(|candidate| *candidate != read.range);
        self.reads.push(read);
    }
}

impl ReadBatchInner {
    fn plan_groups(&self, mut candidates: Vec<Range<usize>>) -> Vec<BatchGroup> {
        let config = &self.scheduler.config;
        candidates.sort_unstable_by_key(|range| range.start);
        let mut groups: Vec<BatchGroup> = Vec::new();
        for range in candidates {
            if let Some(group) = groups.last_mut()
                && group.missing.len() < config.max_batch_requests.get()
                && range.start.saturating_sub(group.span.end) <= config.max_gap_bytes
                && group.span.end.max(range.end) - group.span.start <= config.max_read_bytes.get()
            {
                group.span.end = group.span.end.max(range.end);
                group.missing.push(range);
            } else {
                groups.push(BatchGroup {
                    span: range.clone(),
                    missing: vec![range],
                    reads: Vec::new(),
                    timer: None,
                });
            }
        }
        groups.retain(|group| group.missing.len() > 1);
        groups
    }

    fn release_reason(&self, group: &BatchGroup) -> Option<BatchRelease> {
        if group.missing.is_empty() {
            return Some(BatchRelease::Ready);
        }
        let first = group.reads[0].arrived.unwrap();
        if first.elapsed() >= Duration::from_micros(self.scheduler.config.prefetch_batch_wait_us) {
            return Some(BatchRelease::Deadline);
        }
        // Stop collecting at admission pressure without reserving a group
        // credit or a physical slot for the missing reads.
        if self.scheduler.waiting.get() as usize >= self.scheduler.config.max_admitted_groups.get()
            || self.scheduler.groups.slots.available_permits() == 0
        {
            return Some(BatchRelease::Pressure);
        }
        None
    }

    fn arm_timeout(self: &Arc<Self>, group: &mut BatchGroup) {
        if group.timer.is_some() {
            return;
        }
        let deadline = group.reads[0].arrived.unwrap()
            + Duration::from_micros(self.scheduler.config.prefetch_batch_wait_us);
        let batch = Arc::downgrade(self);
        let start = group.span.start;
        let task = tokio::spawn(async move {
            tokio::time::sleep_until(deadline).await;
            let Some(batch) = batch.upgrade() else {
                return;
            };
            let group = {
                let mut state = batch.state.lock().unwrap();
                let ReadBatchState::Sealed(groups) = &mut *state else {
                    unreachable!("a timeout is armed only at or after seal")
                };
                let Some(index) = groups.iter().position(|group| group.span.start == start) else {
                    return;
                };
                groups.swap_remove(index)
            };
            batch.release(group.reads, BatchRelease::Deadline);
        });
        group.timer = Some((deadline, ReadTask(task)));
    }

    fn observe_release(&self, reads: &[StagedRead], reason: BatchRelease) {
        self.scheduler.batch_releases[reason as usize].inc();
        for read in reads {
            self.scheduler
                .batch_wait
                .observe(read.arrived.unwrap().elapsed().as_secs_f64());
        }
    }

    fn release(&self, reads: Vec<StagedRead>, reason: BatchRelease) {
        self.observe_release(&reads, reason);
        self.scheduler.submit_batch(reads);
    }
}

impl ReadBatchMember {
    pub(super) fn stage(
        &self,
        scheduler: &ReadScheduler,
        path: &str,
        range: Range<usize>,
        trace_id: Option<u64>,
    ) -> Option<oneshot::Receiver<ReadResult>> {
        let batch = &self.0;
        if !std::ptr::eq(scheduler, batch.scheduler.as_ref()) {
            return None;
        }
        let mut state = batch.state.lock().unwrap();
        let make_read = || {
            scheduler.stage_read(
                path,
                range.clone(),
                trace_id,
                (scheduler.config.prefetch_batch_wait_us > 0).then(BatchInstant::now),
            )
        };
        match &mut *state {
            ReadBatchState::Registering { reads, .. } if reads.len() < batch.max_reads => {
                let (read, receiver) = make_read();
                reads.push(read);
                Some(receiver)
            }
            ReadBatchState::Registering { .. } => None,
            ReadBatchState::Sealed(groups) => {
                let index = groups
                    .iter()
                    .position(|group| group.missing.contains(&range))?;
                let group = &mut groups[index];
                if group
                    .timer
                    .as_ref()
                    .is_some_and(|(deadline, _)| BatchInstant::now() >= *deadline)
                {
                    let reads = groups.swap_remove(index).reads;
                    drop(state);
                    batch.release(reads, BatchRelease::Deadline);
                    return None;
                }
                let (read, receiver) = make_read();
                group.push(read);
                if let Some(reason) = batch.release_reason(group) {
                    let reads = groups.swap_remove(index).reads;
                    drop(state);
                    batch.release(reads, reason);
                } else {
                    batch.arm_timeout(group);
                }
                Some(receiver)
            }
        }
    }
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

struct ActiveReadGuard<'a> {
    queue: &'a GroupQueue,
    flight: Arc<ActiveRead>,
}

impl Drop for ActiveReadGuard<'_> {
    fn drop(&mut self) {
        self.queue
            .pending
            .lock()
            .unwrap()
            .active
            .retain(|active| !Arc::ptr_eq(active, &self.flight));
    }
}

#[derive(Default)]
struct PendingReadGroups {
    groups: Vec<PendingReadGroup>,
    active: Vec<Arc<ActiveRead>>,
    waiting: HashMap<String, BTreeMap<(usize, u64), WaitingReadGroup>>,
    next_waiter: u64,
    // Physical tasks holding a P permit that have not taken a group yet.
    starting: usize,
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
        let (index, _) = pending
            .groups
            .iter()
            .enumerate()
            .min_by_key(|(_, group)| group.group.requests.iter().map(|r| r.queued_at).min())?;
        let mut group = pending.groups.swap_remove(index).group;
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
    }
}

impl ReadScheduler {
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
            sealed_batches: [
                "sealed_batch_empty",
                "sealed_batch_single",
                "sealed_batch_multiple",
            ]
            .map(|event| {
                metrics
                    .read_scheduler_events
                    .with_label_values(&[media_type, event])
            }),
            batch_wait: metrics
                .read_scheduler_batch_wait
                .with_label_values(&[media_type]),
            batch_releases: ["batch_ready", "batch_deadline", "batch_pressure"].map(|event| {
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
            let (read, response) = self.stage_read(path, range.clone(), trace_id, None);
            let waiter = {
                let mut state = groups.pending.lock().unwrap();
                if let Some(mut followers) =
                    state.covering_read(path, &range, 1, self.config.max_batch_requests.get())
                {
                    followers
                        .as_mut()
                        .unwrap()
                        .push(read.into_request(self, false));
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
        arrived: Option<BatchInstant>,
    ) -> (StagedRead, oneshot::Receiver<ReadResult>) {
        let (response, receiver) = oneshot::channel();
        (
            StagedRead {
                path: path.to_owned(),
                range,
                response,
                trace_id,
                arrived,
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
        let requests = reads.into_iter().map(|read| read.into_request(self, true));
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
            group
                .group
                .requests
                .extend(reads.into_iter().map(|read| read.into_request(self, true)));
            group.absorb_pending(&mut state.groups, &self.config);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        state.groups.push(group);
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
            tokio::spawn(async move {
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
        followers
            .as_mut()
            .unwrap()
            .extend(reads.into_iter().map(|read| read.into_request(self, false)));
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

#[cfg(not(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay")))]
fn acquire_physical(
    slots: Arc<Semaphore>,
) -> impl Future<Output = Result<PhysicalPermit, AcquireError>> {
    slots.acquire_owned()
}

#[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
use replay::acquire_physical;

async fn run<F, R>(
    mut shutdown: oneshot::Receiver<()>,
    worker: GroupQueueWorker,
    read: F,
    config: ReadSchedulerConfig,
    metrics: ObjectStoreMetrics,
    media_type: &'static str,
) where
    F: Fn(String, Range<usize>) -> R + Send + Sync + 'static,
    R: Future<Output = ReadResult> + Send + 'static,
{
    let groups = &worker.0;
    let physical_slots = Arc::new(Semaphore::new(config.max_inflight_reads.get()));
    let read = Arc::new(read);
    let metrics = Arc::new(metrics);
    let cancelled_queued = metrics
        .read_scheduler_events
        .with_label_values(&[media_type, "cancelled_queued"]);
    let mut in_flight = FuturesUnordered::new();
    let mut closed = false;
    loop {
        let (has_pending, can_start) = {
            let mut pending = groups.pending.lock().unwrap();
            pending.groups.retain(|group| {
                let requests = &group.group.requests;
                if requests.iter().all(|request| request.response.is_closed()) {
                    cancelled_queued.inc_by(requests.len() as u64);
                    false
                } else {
                    true
                }
            });
            #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
            replay::queue(&pending, "dispatch_check");
            (
                !pending.groups.is_empty(),
                pending.groups.len() > pending.starting,
            )
        };
        if closed && !has_pending && in_flight.is_empty() {
            return;
        }
        tokio::select! {
            biased;
            completed = in_flight.next(), if !in_flight.is_empty() => {
                if let Some(Err(error)) = completed {
                    let error: tokio::task::JoinError = error;
                    if error.is_panic() { std::panic::resume_unwind(error.into_panic()); }
                    panic!("scheduler physical task cancelled unexpectedly: {error}");
                }
            }
            _ = std::future::poll_fn(|cx| {
                for group in &mut groups.pending.lock().unwrap().groups {
                    if group.group.requests.iter_mut().all(|r| r.response.poll_closed(cx).is_ready()) {
                        return Poll::Ready(());
                    }
                }
                Poll::Pending
            }), if has_pending => {}
            permit = acquire_physical(physical_slots.clone()), if can_start => {
                let permit = permit.expect("scheduler physical semaphore is never closed");
                {
                    let mut pending = groups.pending.lock().unwrap();
                    if pending.groups.len() <= pending.starting {
                        continue;
                    }
                    pending.starting += 1;
                    #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
                    replay::queue(&pending, "reserved");
                }
                let groups = groups.clone();
                let read = read.clone();
                let physical_slots = physical_slots.clone();
                let metrics = metrics.clone();
                let execute = async move {
                    // Keep queued ranges mergeable through the existing task handoff.
                    let Some(group) = groups.start_read() else {
                        return;
                    };
                    let group_span = tracing::debug_span!(
                        target: "object_store_read_scheduler", parent: None, "object_read_group"
                    );
                    if tracing::enabled!(target: "object_store_read_scheduler", tracing::Level::DEBUG) {
                        let request_ids: Vec<_> = group.requests.iter().filter_map(|r| r.trace_id).collect();
                        let ready: usize = groups.pending.lock().unwrap().groups.iter()
                            .map(|group| group.group.requests.len()).sum();
                        tracing::debug!(target: "object_store_read_scheduler", parent: &group_span,
                            group_id = ?request_ids.first(), ?request_ids, ready,
                            available_permits = physical_slots.available_permits(), "scheduler group selected");
                    }
                    let sharing = config.reuse_inflight_reads
                        .then_some((&*groups, config.max_batch_requests.get()));
                    #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
                    let id = permit.id();
                    let read = group.execute(&*read, &physical_slots, permit, &metrics, media_type, sharing)
                        .instrument(group_span);
                    #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
                    let read = replay::read_scope(id, read);
                    read.await;
                };
                in_flight.push(ReadTask(tokio::spawn(execute)));
            }
            _ = groups.ready.notified() => {}
            _ = &mut shutdown, if !closed => closed = true,
        }
    }
}

impl ReadGroup {
    async fn execute<F, R>(
        mut self,
        read: &F,
        physical_slots: &Semaphore,
        permit: PhysicalPermit,
        metrics: &ObjectStoreMetrics,
        media_type: &str,
        sharing: Option<(&GroupQueue, usize)>,
    ) where
        F: Fn(String, Range<usize>) -> R,
        R: Future<Output = ReadResult>,
    {
        let planned = self.requests.len();
        self.requests
            .retain(|request| !request.response.is_closed());
        metrics
            .read_scheduler_events
            .with_label_values(&[media_type, "cancelled_before_dispatch"])
            .inc_by((planned - self.requests.len()) as u64);
        let Some(first) = self.requests.first() else {
            return;
        };
        // Trim the span in case requests at either end were cancelled after planning.
        // Keep the planned group if interior members cancel: do not split it into more GETs.
        let path = first.path.clone();
        let start = first.range.start;
        let mut end = first.range.end;
        let mut gap_bytes = 0;
        for request in &self.requests[1..] {
            gap_bytes += request.range.start.saturating_sub(end);
            end = end.max(request.range.end);
        }
        let span = start..end;
        metrics
            .read_scheduler_group_size
            .with_label_values(&[media_type])
            .observe(self.requests.len() as f64);
        let queue_duration = metrics
            .read_scheduler_queue_duration
            .with_label_values(&[media_type]);
        for request in &mut self.requests {
            drop(request._queued.take());
            queue_duration.observe(request.queued_at.elapsed().as_secs_f64());
        }
        let flight = if let Some((queue, max_followers)) = sharing {
            let mut state = queue.pending.lock().unwrap();
            // A covering GET can start between this group's admission and dispatch.
            if let Some(mut followers) =
                state.covering_read(&path, &span, self.requests.len(), max_followers)
            {
                followers.as_mut().unwrap().append(&mut self.requests);
                return;
            }
            let flight = Arc::new(ActiveRead {
                path: path.clone(),
                span: span.clone(),
                followers: Mutex::new(Some(Vec::new())),
            });
            state.active.push(flight.clone());
            Some(ActiveReadGuard { queue, flight })
        } else {
            None
        };
        // Tokens identify actual outer reads. Build this list only for trace collection,
        // and emit dispatch inside the polled read future (not merely at dequeue).
        let trace_members =
            (tracing::enabled!(target: "state_store_read_batch", tracing::Level::DEBUG)
                || tracing::enabled!(target: "object_store_read_scheduler", tracing::Level::DEBUG))
            .then(|| {
                self.requests
                    .iter()
                    .filter_map(|request| request.trace_id)
                    .collect::<Vec<_>>()
            });
        let group_id = trace_members
            .as_ref()
            .and_then(|members| members.first())
            .copied();
        let physical = async {
            let _permit = permit;
            #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
            _permit.backend_begin();
            let _in_flight = ReadGauge::new(
                &metrics
                    .read_scheduler_in_flight
                    .with_label_values(&[media_type]),
            );
            tracing::debug!(target: "state_store_read_batch",?group_id, request_ids = ?trace_members, start = span.start,
                end = span.end, "read scheduler group dispatch");
            tracing::debug!(target: "object_store_read_scheduler", ?group_id,
                request_ids = ?trace_members, start = span.start, end = span.end,
                "scheduler physical read started");
            let result = {
                let _timer = metrics
                    .read_scheduler_physical_duration
                    .with_label_values(&[media_type, "group"])
                    .start_timer();
                read(path, span.clone()).await
            };
            tracing::debug!(target: "object_store_read_scheduler", ?group_id,
                ok = result.is_ok(), "scheduler physical read completed");
            tracing::debug!(target: "state_store_read_batch",
                ?group_id,
                ok = result.is_ok(),
                "read scheduler group completed"
            );
            result
        };
        let initial_members = self.requests.len();
        let result = if let Some(flight) = flight {
            let result = tokio::select! {
                biased;
                _ = std::future::poll_fn(|cx| {
                    if !self.requests.iter_mut().all(|request| request.response.poll_closed(cx).is_ready()) {
                        return Poll::Pending;
                    }
                    let mut followers = flight.flight.followers.lock().unwrap();
                    if followers.as_mut().unwrap().iter_mut().all(|request| request.response.poll_closed(cx).is_ready()) {
                        // Close registration while holding the lock so a new follower
                        // cannot join a GET that is about to be cancelled.
                        followers.take();
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }) => None,
                result = physical => Some(result),
            };
            let followers = flight.flight.followers.lock().unwrap().take();
            drop(flight);
            if let Some(followers) = followers {
                self.requests.extend(followers);
            }
            result
        } else {
            read_while_wanted(&mut self.requests, physical).await
        };
        let Some(result) = result else {
            metrics
                .read_scheduler_events
                .with_label_values(&[media_type, "group_cancelled"])
                .inc();
            tracing::debug!(target: "state_store_read_batch",?group_id, "read scheduler group cancelled");
            return;
        };
        let result = result.and_then(|bytes| {
            if bytes.len() == span.len() {
                Ok(bytes)
            } else {
                Err(ObjectError::read_length_mismatch(format!(
                    "merged read of {span:?} returned {} bytes",
                    bytes.len()
                )))
            }
        });
        metrics
            .read_scheduler_events
            .with_label_values(&[
                media_type,
                if result.is_ok() {
                    "group_ok"
                } else {
                    "group_error"
                },
            ])
            .inc();
        if result.is_ok() {
            if gap_bytes > 0 {
                metrics
                    .read_scheduler_overread_bytes
                    .with_label_values(&[media_type])
                    .inc_by(gap_bytes as u64);
                metrics
                    .read_scheduler_events
                    .with_label_values(&[media_type, "group_with_gap"])
                    .inc();
            }
            metrics
                .read_scheduler_events
                .with_label_values(&[media_type, "merged_requests"])
                .inc_by(initial_members.saturating_sub(1) as u64);
            if self.requests.len() > initial_members {
                metrics
                    .read_scheduler_events
                    .with_label_values(&[media_type, "inflight_reused"])
                    .inc_by((self.requests.len() - initial_members) as u64);
            }
        }
        if self.requests.len() == 1 {
            let _ = self.requests.pop().unwrap().response.send(result);
            return;
        }
        if result.as_ref().is_err_and(|err| err.is_read_range_error()) {
            metrics
                .read_scheduler_events
                .with_label_values(&[media_type, "range_fallback_requests"])
                .inc_by(self.requests.len() as u64);
            // A member that extends past EOF must not fail other valid reads. Only range
            // failures are retried individually, so an outage does not multiply retries.
            join_all(self.requests.into_iter().map(|mut request| async move {
                let path = request.path.clone();
                let range = request.range.clone();
                let trace_id = request.trace_id;
                let future = async {
                    // The merged read released its permit before fallback. Each child
                    // shares the same physical budget and can be cancelled while waiting.
                    let _permit = {
                        let _timer = metrics.read_scheduler_physical_duration
                            .with_label_values(&[media_type, "fallback_wait"]).start_timer();
                        physical_slots.acquire().await.expect("scheduler physical semaphore is never closed")
                    };
                    let _in_flight = ReadGauge::new(&metrics.read_scheduler_in_flight.with_label_values(&[media_type]));
                    tracing::debug!(target: "state_store_read_batch",?group_id, request_id = ?trace_id,
                        "read scheduler range fallback dispatch");
                    let result = {
                        let _timer = metrics.read_scheduler_physical_duration
                            .with_label_values(&[media_type, "fallback"]).start_timer();
                        read(path, range).await
                    };
                    tracing::debug!(target: "state_store_read_batch",?group_id, request_id = ?trace_id, ok = result.is_ok(),
                        "read scheduler range fallback completed");
                    result
                };
                if let Some(result) =
                    read_while_wanted(std::slice::from_mut(&mut request), future).await
                {
                    let _ = request.response.send(result);
                }
            }))
            .await;
            return;
        }
        let result = result.map_err(Arc::new);
        for request in self.requests {
            if request.response.is_closed() {
                continue;
            }
            let result = match &result {
                Ok(bytes) if request.range == span => Ok(bytes.clone()),
                // A block cache entry may outlive all other members of this read.
                Ok(bytes) => Ok(Bytes::copy_from_slice(
                    &bytes[request.range.start - start..request.range.end - start],
                )),
                Err(error) => Err(ObjectErrorInner::SharedRead(error.clone()).into()),
            };
            let _ = request.response.send(result);
        }
    }
}

async fn read_while_wanted(
    requests: &mut [ReadRequest],
    read: impl Future<Output = ReadResult>,
) -> Option<ReadResult> {
    tokio::select! {
        biased;
        _ = join_all(requests.iter_mut().map(|request| request.response.closed())) => None,
        result = read => Some(result),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use super::super::{InMemObjectStore, ObjectStore, ObjectStoreConfig};
    use super::*;

    #[tokio::test]
    async fn read_coalesces_adjacent_ranges_on_the_same_object() {
        let metrics = Arc::new(ObjectStoreMetrics::new(&prometheus::Registry::new()));
        let config = ObjectStoreConfig {
            read_scheduler: ReadSchedulerConfig {
                max_batch_requests: 4.try_into().unwrap(),
                max_read_bytes: 8.try_into().unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };
        let max_bytes = config.read_scheduler.max_read_bytes.get();
        let mut store = InMemObjectStore::default().monitored(metrics.clone(), Arc::new(config));
        store
            .upload("first", Bytes::from_static(b"abcdefghijkl"))
            .await
            .unwrap();
        store
            .upload("second", Bytes::from_static(b"mnopqrstuvwx"))
            .await
            .unwrap();
        assert_eq!(store.read("first", 0..2).await.unwrap(), "ab");
        assert_eq!(
            metrics
                .read_request_duration
                .with_label_values(&[store.media_type(), "direct"])
                .get_sample_count(),
            1
        );
        store.enable_read_coalescing();

        let (a, b, c, d) = tokio::join!(
            store.read("first", 0..2),
            store.read("first", 2..4),
            store.read("first", 8..10),
            store.read("second", 0..2),
        );
        assert_eq!(a.unwrap(), "ab");
        assert_eq!(b.unwrap(), "cd");
        assert_eq!(c.unwrap(), "ij");
        assert_eq!(d.unwrap(), "mn");
        assert_eq!(
            metrics
                .operation_size
                .with_label_values(&["read"])
                .get_sample_count(),
            4
        );
        // Adjacent requests may fill the byte cap exactly, but must not cross it.
        let payload = Bytes::from(vec![7; max_bytes + 1]);
        store.upload("boundary", payload.clone()).await.unwrap();
        let middle = max_bytes / 2;
        let (a, b, c) = tokio::join!(
            store.read("boundary", 0..middle),
            store.read("boundary", middle..max_bytes),
            store.read("boundary", max_bytes..max_bytes + 1),
        );
        assert_eq!(a.unwrap(), payload.slice(0..middle));
        assert_eq!(b.unwrap(), payload.slice(middle..max_bytes));
        assert_eq!(c.unwrap(), payload.slice(max_bytes..));
        assert_eq!(
            metrics
                .operation_size
                .with_label_values(&["read"])
                .get_sample_count(),
            6
        );
        assert_eq!(
            metrics
                .read_request_duration
                .with_label_values(&[store.media_type(), "scheduled"])
                .get_sample_count(),
            7
        );
        assert_eq!(
            metrics
                .read_outcomes
                .with_label_values(&[store.media_type(), "physical", "ok"])
                .get(),
            6
        );
        assert_eq!(
            metrics
                .read_scheduler_events
                .with_label_values(&[store.media_type(), "merged_requests"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .read_scheduler_pending
                .with_label_values(&[store.media_type()])
                .get(),
            0
        );

        // Overlap and duplicate requests still receive their own exact ranges.
        let (a, b, c) = tokio::join!(
            store.read("first", 0..4),
            store.read("first", 2..6),
            store.read("first", 0..4),
        );
        assert_eq!(a.unwrap(), "abcd");
        assert_eq!(b.unwrap(), "cdef");
        assert_eq!(c.unwrap(), "abcd");
        assert_eq!(
            metrics
                .operation_size
                .with_label_values(&["read"])
                .get_sample_count(),
            7
        );
    }

    #[tokio::test]
    async fn contained_read_shares_a_started_get_until_its_last_waiter_leaves() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let (calls, mut reads) = mpsc::unbounded_channel();
            let release = Arc::new(Notify::new());
            let scheduler = ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_groups: 1.try_into().unwrap(),
                    max_inflight_reads: 1.try_into().unwrap(),
                    reuse_inflight_reads: true,
                    ..Default::default()
                },
                {
                    let release = release.clone();
                    move |_, range: Range<usize>| {
                        let release = release.clone();
                        let calls = calls.clone();
                        async move {
                            calls.send(range.clone()).unwrap();
                            if range == (0..4) {
                                release.notified().await;
                            }
                            Ok(Bytes::from_static(&b"abcdef"[range]))
                        }
                    }
                },
            );
            let leader = scheduler.submit("file", 0..4, None).await.unwrap();
            assert_eq!(reads.recv().await.unwrap(), 0..4);
            assert_eq!(scheduler.groups.slots.available_permits(), 0);
            let follower = scheduler.submit("file", 1..3, None).await.unwrap();
            let partial = scheduler.submit("file", 3..5, None);
            tokio::pin!(partial);
            assert!(futures::poll!(&mut partial).is_pending());
            drop(leader);
            release.notify_one();
            assert_eq!(follower.await.unwrap().unwrap(), "bc");
            assert_eq!(partial.await.unwrap().await.unwrap().unwrap(), "de");
            assert_eq!(reads.recv().await.unwrap(), 3..5);
            assert!(reads.try_recv().is_err());
            assert_eq!(
                metrics
                    .read_scheduler_events
                    .with_label_values(&["test", "inflight_reused"])
                    .get(),
                1,
            );

            let leader = scheduler.submit("file", 0..4, None).await.unwrap();
            assert_eq!(reads.recv().await.unwrap(), 0..4);
            let follower = scheduler.submit("file", 1..3, None).await.unwrap();
            drop(leader);
            drop(follower);
            drop(
                scheduler
                    .groups
                    .slots
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap(),
            );
            assert!(scheduler.groups.pending.lock().unwrap().active.is_empty());
            assert_eq!(scheduler.pending.get(), 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn sealed_batch_waits_for_one_group_credit_without_splitting() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let (calls, mut reads) = mpsc::unbounded_channel();
            let release = Arc::new(Notify::new());
            let scheduler = Arc::new(ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_groups: 1.try_into().unwrap(),
                    max_inflight_reads: 1.try_into().unwrap(),
                    ..Default::default()
                },
                {
                    let release = release.clone();
                    move |path, range: Range<usize>| {
                        let release = release.clone();
                        let calls = calls.clone();
                        async move {
                            calls.send((path.clone(), range.clone())).unwrap();
                            if path == "busy" {
                                release.notified().await;
                            }
                            Ok(Bytes::from_static(&b"abcd"[range]))
                        }
                    }
                },
            ));
            let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
            assert_eq!(reads.recv().await.unwrap(), ("busy".to_owned(), 0..1));

            let batch = ReadBatch::new(scheduler.clone(), 2);
            let member = batch.member();
            let b = member.stage(&scheduler, "file", 2..4, None).unwrap();
            let a = member.stage(&scheduler, "file", 0..2, None).unwrap();
            drop(batch);
            assert!(member.stage(&scheduler, "file", 0..1, None).is_none());

            let cancelled = ReadBatch::new(scheduler.clone(), 2);
            let cancelled_response = cancelled
                .member()
                .stage(&scheduler, "cancelled", 0..1, None);
            assert!(cancelled_response.is_some());
            drop(cancelled);
            while scheduler.batch_members.get() < 3 {
                tokio::task::yield_now().await;
            }
            assert_eq!(scheduler.groups.slots.available_permits(), 0);
            assert_eq!(scheduler.waiting.get(), 3);
            assert!(reads.try_recv().is_err());
            drop(cancelled_response);
            while scheduler.waiting.get() != 2 {
                tokio::task::yield_now().await;
            }

            release.notify_one();
            assert_eq!(busy.await.unwrap().unwrap(), "a");
            assert_eq!(a.await.unwrap().unwrap(), "ab");
            assert_eq!(b.await.unwrap().unwrap(), "cd");
            assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 0..4));
            assert!(reads.try_recv().is_err());
            drop(
                scheduler
                    .groups
                    .slots
                    .clone()
                    .acquire_owned()
                    .await
                    .unwrap(),
            );
            assert_eq!(scheduler.waiting.get(), 0);
            assert_eq!(scheduler.pending.get(), 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn seal_merges_ready_group_while_another_group_waits_for_credit() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let (calls, mut reads) = mpsc::unbounded_channel();
            let release = Arc::new(Notify::new());
            let scheduler = Arc::new(ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_groups: 2.try_into().unwrap(),
                    max_inflight_reads: 1.try_into().unwrap(),
                    ..Default::default()
                },
                {
                    let release = release.clone();
                    move |path, range: Range<usize>| {
                        let release = release.clone();
                        let calls = calls.clone();
                        async move {
                            calls.send((path.clone(), range.clone())).unwrap();
                            if path == "busy" {
                                release.notified().await;
                            }
                            Ok(Bytes::from_static(&b"abcd"[range]))
                        }
                    }
                },
            ));

            let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
            assert_eq!(reads.recv().await.unwrap(), ("busy".to_owned(), 0..1));
            let queued = scheduler.submit("file", 0..1, None).await.unwrap();
            assert_eq!(scheduler.groups.slots.available_permits(), 0);

            let batch = ReadBatch::new(scheduler.clone(), 3);
            let member = batch.member();
            let deferred = member.stage(&scheduler, "after", 0..1, None).unwrap();
            let c = member.stage(&scheduler, "file", 2..3, None).unwrap();
            let b = member.stage(&scheduler, "file", 1..2, None).unwrap();
            drop(batch);

            // No task has had a chance to poll since sealing the batch.
            {
                let pending = scheduler.groups.pending.lock().unwrap();
                assert_eq!(pending.groups.len(), 1);
                assert_eq!(pending.groups[0].span, 0..3);
                assert_eq!(pending.groups[0].group.requests.len(), 3);
            }
            assert_eq!(scheduler.waiting.get(), 1);
            drop(deferred);
            release.notify_one();
            assert_eq!(busy.await.unwrap().unwrap(), "a");
            assert_eq!(queued.await.unwrap().unwrap(), "a");
            assert_eq!(b.await.unwrap().unwrap(), "b");
            assert_eq!(c.await.unwrap().unwrap(), "c");
            assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 0..3));
            assert!(reads.try_recv().is_err());
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn group_headroom_admits_demand_but_preserves_prefetch_dispatch_order() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for group_limit in [2, 3] {
                let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
                let (send_call, mut calls) = mpsc::unbounded_channel();
                let release_active = Arc::new(Notify::new());
                let release_queued = Arc::new(Notify::new());
                let scheduler = Arc::new(ReadScheduler::new(
                    &metrics,
                    "test",
                    ReadSchedulerConfig {
                        max_admitted_groups: group_limit.try_into().unwrap(),
                        max_inflight_reads: 1.try_into().unwrap(),
                        prefetch_batch_wait_us: 0,
                        ..Default::default()
                    },
                    {
                        let release_active = release_active.clone();
                        let release_queued = release_queued.clone();
                        move |path, range: Range<usize>| {
                            let send_call = send_call.clone();
                            let release = match path.as_str() {
                                "active-prefetch" => Some(release_active.clone()),
                                "queued-prefetch" => Some(release_queued.clone()),
                                _ => None,
                            };
                            async move {
                                send_call.send((path, range.clone())).unwrap();
                                if let Some(release) = release {
                                    release.notified().await;
                                }
                                Ok(Bytes::from_static(&b"abcd"[range]))
                            }
                        }
                    },
                ));

                let active_batch = ReadBatch::new(scheduler.clone(), 1);
                let active = active_batch
                    .member()
                    .stage(&scheduler, "active-prefetch", 0..1, None)
                    .unwrap();
                drop(active_batch);
                assert_eq!(
                    calls.recv().await.unwrap(),
                    ("active-prefetch".into(), 0..1)
                );

                let queued_batch = ReadBatch::new(scheduler.clone(), 2);
                let member = queued_batch.member();
                let a = member
                    .stage(&scheduler, "queued-prefetch", 0..1, None)
                    .unwrap();
                let b = member
                    .stage(&scheduler, "queued-prefetch", 1..2, None)
                    .unwrap();
                drop(queued_batch);
                assert_eq!(scheduler.groups.admitted.get(), 2);
                assert_eq!(scheduler.groups.queued.get(), 1);

                let mut demand = std::pin::pin!(scheduler.submit("demand", 2..3, None));
                let admitted = match futures::poll!(&mut demand) {
                    Poll::Pending => None,
                    Poll::Ready(result) => Some(result.unwrap()),
                };
                assert_eq!(admitted.is_some(), group_limit == 3);
                assert_eq!(scheduler.groups.slots.available_permits(), 0);
                assert!(calls.try_recv().is_err());

                let waiters = scheduler.waiting.get();
                {
                    let mut cancelled = std::pin::pin!(scheduler.submit("cancelled", 3..4, None));
                    assert!(futures::poll!(&mut cancelled).is_pending());
                    assert_eq!(scheduler.waiting.get(), waiters + 1);
                }
                assert_eq!(scheduler.waiting.get(), waiters);

                release_active.notify_one();
                assert_eq!(active.await.unwrap().unwrap(), "a");
                let demand = match admitted {
                    Some(response) => response,
                    None => demand.await.unwrap(),
                };
                assert_eq!(
                    calls.recv().await.unwrap(),
                    ("queued-prefetch".into(), 0..2)
                );
                assert!(calls.try_recv().is_err());

                release_queued.notify_one();
                assert_eq!(a.await.unwrap().unwrap(), "a");
                assert_eq!(b.await.unwrap().unwrap(), "b");
                assert_eq!(calls.recv().await.unwrap(), ("demand".into(), 2..3));
                assert_eq!(demand.await.unwrap().unwrap(), "c");
                assert!(calls.try_recv().is_err());
                drop(
                    scheduler
                        .groups
                        .slots
                        .clone()
                        .acquire_many_owned(group_limit as u32)
                        .await
                        .unwrap(),
                );
                assert_eq!(scheduler.groups.admitted.get(), 0);
                assert_eq!(scheduler.groups.queued.get(), 0);
                assert_eq!(scheduler.pending.get(), 0);
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn prefetch_batch_wait_merges_late_neighbors_without_blocking_point_reads() {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (calls, mut reads) = mpsc::unbounded_channel();
        let scheduler = Arc::new(ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                prefetch_batch_wait_us: 10_000,
                max_admitted_groups: 4.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                ..Default::default()
            },
            move |path, range: Range<usize>| {
                calls.send((path, range.clone())).unwrap();
                std::future::ready(Ok(Bytes::from_static(&b"abcdef"[range])))
            },
        ));
        let batch = ReadBatch::new(scheduler.clone(), 4);
        for range in [0..1, 1..2, 3..4, 4..5] {
            batch.expect(range);
        }
        let member = batch.member();
        let a = member.stage(&scheduler, "file", 0..1, None).unwrap();
        drop(batch);
        let d = member.stage(&scheduler, "file", 3..4, None).unwrap();
        assert_eq!(scheduler.groups.slots.available_permits(), 4);

        let point = scheduler.submit("point", 1..2, None).await.unwrap();
        assert_eq!(point.await.unwrap().unwrap(), "b");
        assert_eq!(reads.recv().await.unwrap(), ("point".to_owned(), 1..2));
        assert!(reads.try_recv().is_err());

        let b = member.stage(&scheduler, "file", 1..2, None).unwrap();
        assert_eq!(a.await.unwrap().unwrap(), "a");
        assert_eq!(b.await.unwrap().unwrap(), "b");
        assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 0..2));
        assert!(reads.try_recv().is_err());

        // E never joins D's batch. Only D is submitted when the timer expires.
        tokio::time::advance(Duration::from_millis(11)).await;
        assert_eq!(d.await.unwrap().unwrap(), "d");
        assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 3..4));
        assert!(member.stage(&scheduler, "file", 4..5, None).is_none());
        let e = scheduler.submit("file", 4..5, None).await.unwrap();
        assert_eq!(e.await.unwrap().unwrap(), "e");
        assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 4..5));
        assert_eq!(
            scheduler.batch_releases[BatchRelease::Ready as usize].get(),
            1
        );
        assert_eq!(
            scheduler.batch_releases[BatchRelease::Deadline as usize].get(),
            1
        );
        assert_eq!(scheduler.batch_wait.get_sample_count(), 3);
        assert_eq!(scheduler.waiting.get(), 0);
        assert_eq!(scheduler.groups.slots.available_permits(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn prefetch_batch_wait_releases_under_pressure_and_drops_cancelled_reads() {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (calls, mut reads) = mpsc::unbounded_channel();
        let scheduler = Arc::new(ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                prefetch_batch_wait_us: 10_000,
                max_admitted_groups: 2.try_into().unwrap(),
                ..Default::default()
            },
            move |path, range: Range<usize>| {
                calls.send((path, range.clone())).unwrap();
                std::future::ready(Ok(Bytes::from_static(&b"abcd"[range])))
            },
        ));
        let batch = ReadBatch::new(scheduler.clone(), 2);
        batch.expect(0..1);
        batch.expect(1..2);
        let member = batch.member();
        drop(batch);
        let cancelled = member.stage(&scheduler, "cancelled", 0..1, None).unwrap();
        drop(cancelled);
        // Production drops the member with the cancelled object-read future.
        drop(member);
        assert_eq!(scheduler.waiting.get(), 0);

        let batch = ReadBatch::new(scheduler.clone(), 2);
        batch.expect(0..1);
        batch.expect(1..2);
        let member = batch.member();
        drop(batch);
        let slots = &scheduler.groups.slots;
        let busy = slots.clone().try_acquire_many_owned(2).unwrap();
        let a = member.stage(&scheduler, "file", 0..1, None).unwrap();
        assert_eq!(
            scheduler.batch_releases[BatchRelease::Pressure as usize].get(),
            1
        );
        assert!(member.stage(&scheduler, "file", 1..2, None).is_none());
        assert!(reads.try_recv().is_err());
        drop(busy);
        assert_eq!(a.await.unwrap().unwrap(), "a");
        assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 0..1));
        tokio::time::advance(Duration::from_millis(11)).await;
        assert!(reads.try_recv().is_err());
        assert_eq!(scheduler.waiting.get(), 0);
        assert_eq!(slots.available_permits(), 2);
    }

    #[tokio::test]
    async fn queued_group_stays_mergeable_until_physical_task_starts() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for (neighbor, cancel) in [(true, false), (false, false), (false, true)] {
                let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
                let config = ReadSchedulerConfig {
                    max_admitted_groups: 1.try_into().unwrap(),
                    max_inflight_reads: 2.try_into().unwrap(),
                    ..Default::default()
                };
                let groups = Arc::new(GroupQueue::new(&metrics, "test", config));
                let (shutdown, shutdown_rx) = oneshot::channel();
                let (reads, mut physical) = mpsc::unbounded_channel();
                let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
                let submit = |range: Range<usize>| {
                    let (response, receiver) = oneshot::channel();
                    let mut state = groups.pending.lock().unwrap();
                    let request = ReadRequest {
                        path: "file".to_owned(),
                        range: range.clone(),
                        response,
                        queued_at: Instant::now(),
                        trace_id: None,
                        _pending: ReadGauge::new(&pending),
                        _queued: None,
                    };
                    let admission = groups
                        .admission(&state.groups, "file", &range, 1, &mut None, &config)
                        .expect("adjacent read must join even with no free group credit");
                    let group =
                        groups.admit(&mut state.groups, admission, range, [request], &config);
                    state.groups.push(group);
                    groups.ready.notify_one();
                    receiver
                };
                let a = submit(0..2);
                let dispatcher = run(
                    shutdown_rx,
                    GroupQueueWorker(groups.clone()),
                    move |_, range: Range<usize>| {
                        reads.send(range.clone()).unwrap();
                        std::future::ready(Ok(Bytes::from_static(&b"abcd"[range])))
                    },
                    config,
                    metrics,
                    "test",
                );
                tokio::pin!(dispatcher);
                // Drive the dispatcher to spawn on this current-thread runtime,
                // without yielding to the spawned task.
                assert!(futures::poll!(&mut dispatcher).is_pending());
                assert_eq!(groups.queued.get(), 1);
                assert_eq!(groups.pending.lock().unwrap().starting, 1);
                let b = neighbor.then(|| submit(2..4));
                drop(shutdown);
                if cancel {
                    drop(a);
                    dispatcher.await;
                } else {
                    dispatcher.await;
                    assert_eq!(a.await.unwrap().unwrap(), "ab");
                    if let Some(b) = b {
                        assert_eq!(b.await.unwrap().unwrap(), "cd");
                    }
                    assert_eq!(
                        physical.try_recv().unwrap(),
                        0..if neighbor { 4 } else { 2 }
                    );
                }
                assert!(physical.try_recv().is_err());
                assert_eq!(pending.get(), 0);
                assert_eq!(groups.slots.available_permits(), 1);
                assert_eq!(groups.pending.lock().unwrap().starting, 0);
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stopping_dispatcher_cancels_physical_reads() {
        // The spawned future must also be cancelled if its owner is dropped
        // before either the child or its join handle has been polled.
        let unstarted_slot = Arc::new(Semaphore::new(1));
        let permit = unstarted_slot.clone().acquire_owned().await.unwrap();
        let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let child_polled = polled.clone();
        let task = ReadTask(tokio::spawn(async move {
            let _permit = permit;
            child_polled.store(true, std::sync::atomic::Ordering::Relaxed);
        }));
        drop(task);
        tokio::time::timeout(Duration::from_secs(5), async {
            let _permit = unstarted_slot.acquire().await.unwrap();
        })
        .await
        .unwrap();
        assert!(!polled.load(std::sync::atomic::Ordering::Relaxed));

        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
        let active = metrics
            .read_scheduler_in_flight
            .with_label_values(&["test"]);
        let groups = Arc::new(GroupQueue::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 1.try_into().unwrap(),
                ..Default::default()
            },
        ));
        let (_shutdown, shutdown_rx) = oneshot::channel();
        let (response, result) = oneshot::channel();
        {
            let mut pending_groups = groups.pending.lock().unwrap();
            let permit = groups.slots.clone().try_acquire_owned().unwrap();
            pending_groups.groups.push(PendingReadGroup::new(
                &groups,
                permit,
                0..1,
                vec![ReadRequest {
                    path: "file".to_owned(),
                    range: 0..1,
                    response,
                    queued_at: Instant::now(),
                    trace_id: None,
                    _pending: ReadGauge::new(&pending),
                    _queued: None,
                }],
            ));
        }
        let started = Arc::new(Notify::new());
        let dispatcher = tokio::spawn(run(
            shutdown_rx,
            GroupQueueWorker(groups.clone()),
            {
                let started = started.clone();
                move |_, _| {
                    let started = started.clone();
                    async move {
                        started.notify_one();
                        std::future::pending::<ReadResult>().await
                    }
                }
            },
            ReadSchedulerConfig::default(),
            metrics,
            "test",
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            started.notified().await;
            assert_eq!(active.get(), 1);
            let waiting = groups.slots.clone().acquire_owned();
            tokio::pin!(waiting);
            assert!(futures::poll!(&mut waiting).is_pending());
            dispatcher.abort();
            assert!(dispatcher.await.unwrap_err().is_cancelled());
            assert!(result.await.is_err());
            assert!(waiting.await.is_err());
            assert_eq!(active.get(), 0);
            assert_eq!(pending.get(), 0);
            assert!(groups.slots.is_closed());
            assert_eq!(groups.slots.available_permits(), 1);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cancellation_after_planning_is_accounted_before_dispatch() {
        for cancel_all in [false, true] {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
            let slots = Arc::new(Semaphore::new(1));
            let groups = metrics
                .read_scheduler_groups
                .with_label_values(&["test", "admitted"]);
            let mut requests = Vec::new();
            let mut responses = Vec::new();
            for start in [0, 4, 8] {
                let (response, receiver) = oneshot::channel();
                requests.push(ReadRequest {
                    path: "file".to_owned(),
                    range: start..start + 4,
                    response,
                    queued_at: Instant::now(),
                    trace_id: None,
                    _pending: ReadGauge::new(&pending),
                    _queued: None,
                });
                responses.push(receiver);
            }
            let group = ReadGroup {
                requests,
                _credit: (
                    ReadGauge::new(&groups),
                    slots.clone().try_acquire_owned().unwrap(),
                ),
            };
            let physical_slots = Arc::new(Semaphore::new(1));
            // Force cancellation after planning, before the group's first poll.
            drop(responses.remove(1));
            if cancel_all {
                responses.clear();
            }
            let read = |_, range| async move {
                assert!(!cancel_all, "a fully cancelled group must not issue IO");
                assert_eq!(range, 0..12);
                Ok(Bytes::from_static(b"abcdefghijkl"))
            };
            group
                .execute(
                    &read,
                    &physical_slots,
                    acquire_physical(physical_slots.clone()).await.unwrap(),
                    &metrics,
                    "test",
                    None,
                )
                .await;
            if !cancel_all {
                assert_eq!(responses.remove(0).await.unwrap().unwrap(), "abcd");
                assert_eq!(responses.remove(0).await.unwrap().unwrap(), "ijkl");
            }
            assert_eq!(
                metrics
                    .read_scheduler_events
                    .with_label_values(&["test", "cancelled_before_dispatch"])
                    .get(),
                if cancel_all { 3 } else { 1 }
            );
            assert_eq!(pending.get(), 0);
            assert_eq!(slots.available_permits(), 1);
        }
    }

    #[tokio::test]
    async fn non_range_error_is_shared_without_individual_retries() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let scheduler = ReadScheduler::new(
            &ObjectStoreMetrics::unused(),
            "test",
            ReadSchedulerConfig::default(),
            {
                let calls = calls.clone();
                move |_, range| {
                    let calls = calls.clone();
                    async move {
                        assert_eq!(range, 0..4);
                        calls.fetch_add(1, Ordering::Relaxed);
                        Err(ObjectError::internal("backend unavailable"))
                    }
                }
            },
        );
        let first = scheduler.submit("file", 0..2, None).await.unwrap();
        let second = scheduler.submit("file", 2..4, None).await.unwrap();
        for response in [first, second] {
            let error = response.await.unwrap().unwrap_err();
            assert!(matches!(error.inner(), ObjectErrorInner::SharedRead(_)));
            assert!(!error.is_read_range_error());
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn out_of_bounds_read_does_not_block_adjacent_valid_reads() {
        let release_slow_read = Arc::new(Notify::new());
        let scheduler = ReadScheduler::new(
            &ObjectStoreMetrics::unused(),
            "test",
            ReadSchedulerConfig {
                max_inflight_reads: 2.try_into().unwrap(),
                ..Default::default()
            },
            {
                let release_slow_read = release_slow_read.clone();
                move |_, range| {
                    let release_slow_read = release_slow_read.clone();
                    async move {
                        if range == (0..2) {
                            release_slow_read.notified().await;
                        }
                        b"data".get(range).map(Bytes::from_static).ok_or_else(|| {
                            ObjectError::read_length_mismatch("range exceeds object".to_owned())
                        })
                    }
                }
            },
        );
        let slow = scheduler.submit("file", 0..2, None).await.unwrap();
        let valid = scheduler.submit("file", 2..4, None).await.unwrap();
        let invalid = scheduler.submit("file", 4..6, None).await.unwrap();
        // Normal shutdown drains already admitted work, including range fallback.
        drop(scheduler);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), valid)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            "ta"
        );
        assert!(invalid.await.unwrap().unwrap_err().is_read_range_error());
        release_slow_read.notify_one();
        assert_eq!(slow.await.unwrap().unwrap(), "da");
    }

    #[tokio::test]
    async fn bridge_reuses_group_credit_at_capacity() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let calls = Arc::new(Mutex::new(Vec::new()));
            let scheduler = ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_groups: 3.try_into().unwrap(),
                    max_inflight_reads: 1.try_into().unwrap(),
                    ..Default::default()
                },
                {
                    let started = started.clone();
                    let release = release.clone();
                    let calls = calls.clone();
                    move |path, range| {
                        let started = started.clone();
                        let release = release.clone();
                        let calls = calls.clone();
                        async move {
                            calls.lock().unwrap().push((path.clone(), range.clone()));
                            if path == "busy" {
                                started.notify_one();
                                release.notified().await;
                            }
                            Ok(Bytes::from_static(&b"abcdefgh"[range]))
                        }
                    }
                },
            );
            let groups = &scheduler.groups;
            let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
            started.notified().await;
            let a = scheduler.submit("file", 0..2, None).await.unwrap();
            let c = scheduler.submit("file", 4..6, None).await.unwrap();
            assert_eq!(groups.admitted.get(), 3);
            let other = scheduler.submit("aaa", 0..1, None);
            tokio::pin!(other);
            assert!(futures::poll!(&mut other).is_pending());

            let b = scheduler.submit("file", 2..4, None).await.unwrap();
            let other = other.await.unwrap();
            assert_eq!(groups.admitted.get(), 3);
            assert_eq!(groups.queued.get(), 2);

            drop(other);
            let newer = scheduler.submit("newer", 0..1, None).await.unwrap();
            release.notify_one();
            assert_eq!(busy.await.unwrap().unwrap(), "a");
            assert_eq!(a.await.unwrap().unwrap(), "ab");
            assert_eq!(b.await.unwrap().unwrap(), "cd");
            assert_eq!(c.await.unwrap().unwrap(), "ef");
            assert_eq!(newer.await.unwrap().unwrap(), "a");
            assert_eq!(
                *calls.lock().unwrap(),
                vec![
                    ("busy".to_owned(), 0..1),
                    ("file".to_owned(), 0..6),
                    ("newer".to_owned(), 0..1),
                ]
            );
            // Wait for the execution owners to drop their credits after sending results.
            drop(groups.slots.clone().acquire_many_owned(3).await.unwrap());
            assert_eq!(groups.queued.get(), 0);
            assert_eq!(groups.admitted.get(), 0);
            assert_eq!(scheduler.pending.get(), 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn pending_group_admits_compatible_g_waiters_before_dispatch() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for deferred in [false, true] {
                // Also cover a cancelled waiter and a G grant that C has not polled.
                for (extend, cancel, grant) in [
                    (false, false, false),
                    (true, false, false),
                    (true, true, false),
                    (true, false, true),
                ] {
                    let credits = 1 + usize::from(extend) + usize::from(grant);
                    let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
                    let (calls, mut reads) = mpsc::unbounded_channel();
                    let release = Arc::new(Notify::new());
                    let scheduler = Arc::new(ReadScheduler::new(
                        &metrics,
                        "test",
                        ReadSchedulerConfig {
                            max_admitted_groups: credits.try_into().unwrap(),
                            max_inflight_reads: 1.try_into().unwrap(),
                            max_batch_requests: 3.try_into().unwrap(),
                            max_gap_bytes: 0,
                            reuse_inflight_reads: true,
                            ..Default::default()
                        },
                        {
                            let release = release.clone();
                            move |path, range: Range<usize>| {
                                let release = release.clone();
                                let calls = calls.clone();
                                async move {
                                    calls.send((path.clone(), range.clone())).unwrap();
                                    if path == "busy" {
                                        release.notified().await;
                                    }
                                    Ok(Bytes::from_static(&b"abc"[range]))
                                }
                            }
                        },
                    ));
                    let groups = &scheduler.groups;
                    let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
                    assert_eq!(reads.recv().await.unwrap(), ("busy".to_owned(), 0..1));

                    let mut a = std::pin::pin!(scheduler.submit("file", 0..1, None));
                    let admitted_a = match futures::poll!(&mut a) {
                        Poll::Ready(result) if extend => Some(result.unwrap()),
                        Poll::Pending if !extend => None,
                        _ => panic!("unexpected admission for A"),
                    };
                    let held = grant.then(|| groups.slots.clone().try_acquire_owned().unwrap());
                    let c_range = if extend { 2..3 } else { 1..2 };
                    let mut ordinary = None;
                    let mut staged = None;
                    if deferred {
                        let batch = ReadBatch::new(scheduler.clone(), 1);
                        staged = batch.member().stage(&scheduler, "file", c_range, None);
                        assert!(staged.is_some());
                        drop(batch);
                        while groups
                            .pending
                            .lock()
                            .unwrap()
                            .waiting
                            .get("file")
                            .map_or(0, BTreeMap::len)
                            < if extend { 1 } else { 2 }
                        {
                            tokio::task::yield_now().await;
                        }
                    } else {
                        let mut submit = Box::pin(scheduler.submit("file", c_range, None));
                        assert!(futures::poll!(&mut submit).is_pending());
                        ordinary = Some(submit);
                    }
                    assert_eq!(groups.slots.available_permits(), 0);

                    if cancel {
                        drop(ordinary.take());
                        drop(staged.take());
                        while groups.pending.lock().unwrap().waiting.contains_key("file") {
                            tokio::task::yield_now().await;
                        }
                    }
                    let mut unrelated = std::pin::pin!(scheduler.submit("other", 0..1, None));
                    if grant {
                        assert!(futures::poll!(&mut unrelated).is_pending());
                        // The semaphore assigns the returned credit to C, which is
                        // still suspended. B must return it without waiting for C.
                        drop(held);
                        assert_eq!(groups.slots.available_permits(), 0);
                    }
                    let b = if extend {
                        let b = scheduler.submit("file", 1..2, None).await.unwrap();
                        assert_eq!(groups.slots.available_permits(), 0);
                        assert!(!groups.pending.lock().unwrap().waiting.contains_key("file"));
                        Some(b)
                    } else {
                        None
                    };
                    let other = if grant {
                        let Poll::Ready(result) = futures::poll!(&mut unrelated) else {
                            panic!("merging C must release its assigned credit");
                        };
                        Some(result.unwrap())
                    } else {
                        None
                    };
                    assert!(reads.try_recv().is_err());
                    release.notify_one();
                    let a = match admitted_a {
                        Some(a) => a,
                        None => a.await.unwrap(),
                    };
                    // In the ordinary case C has still never been polled since registering.
                    let end = if extend && !cancel { 3 } else { 2 };
                    assert_eq!(reads.recv().await.unwrap(), ("file".to_owned(), 0..end));
                    assert_eq!(busy.await.unwrap().unwrap(), "a");
                    assert_eq!(a.await.unwrap().unwrap(), "a");
                    if let Some(b) = b {
                        assert_eq!(b.await.unwrap().unwrap(), "b");
                    }
                    if !cancel {
                        let c = match ordinary {
                            Some(c) => c.await.unwrap(),
                            None => staged.unwrap(),
                        };
                        assert_eq!(c.await.unwrap().unwrap(), if extend { "c" } else { "b" });
                    }
                    if let Some(other) = other {
                        assert_eq!(reads.recv().await.unwrap(), ("other".to_owned(), 0..1));
                        assert_eq!(other.await.unwrap().unwrap(), "a");
                    }
                    drop(
                        groups
                            .slots
                            .clone()
                            .acquire_many_owned(credits as u32)
                            .await
                            .unwrap(),
                    );
                    assert!(reads.try_recv().is_err());
                    assert!(groups.pending.lock().unwrap().waiting.is_empty());
                    assert_eq!(groups.admitted.get(), 0);
                    assert_eq!(scheduler.waiting.get(), 0);
                    assert_eq!(scheduler.pending.get(), 0);
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cancelled_batch_prefix_does_not_hide_a_mergeable_g_waiter() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let (calls, mut reads) = mpsc::unbounded_channel();
            let release = Arc::new(Notify::new());
            let scheduler = Arc::new(ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_groups: 2.try_into().unwrap(),
                    max_inflight_reads: 1.try_into().unwrap(),
                    max_batch_requests: 4.try_into().unwrap(),
                    max_read_bytes: 16.try_into().unwrap(),
                    max_gap_bytes: 4,
                    ..Default::default()
                },
                {
                    let release = release.clone();
                    move |path, range: Range<usize>| {
                        let release = release.clone();
                        let calls = calls.clone();
                        async move {
                            calls.send(range.clone()).unwrap();
                            if path == "busy" {
                                release.notified().await;
                            }
                            Ok(Bytes::from_iter(range.map(|offset| offset as u8)))
                        }
                    }
                },
            ));
            let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
            assert_eq!(reads.recv().await.unwrap(), 0..1);
            let a = scheduler.submit("file", 100..101, None).await.unwrap();
            let batch = ReadBatch::new(scheduler.clone(), 2);
            let member = batch.member();
            let cancelled = member.stage(&scheduler, "file", 81..92, None).unwrap();
            let c = member.stage(&scheduler, "file", 92..97, None).unwrap();
            drop(batch);
            let groups = &scheduler.groups;
            while !groups.pending.lock().unwrap().waiting.contains_key("file") {
                tokio::task::yield_now().await;
            }

            drop(cancelled);
            let b = scheduler.submit("file", 101..102, None).await.unwrap();
            assert_eq!(groups.slots.available_permits(), 0);
            assert!(groups.pending.lock().unwrap().waiting.is_empty());
            release.notify_one();
            busy.await.unwrap().unwrap();
            assert_eq!(reads.recv().await.unwrap(), 92..102);
            assert_eq!(c.await.unwrap().unwrap().as_ref(), &[92, 93, 94, 95, 96]);
            assert_eq!(a.await.unwrap().unwrap().as_ref(), &[100]);
            assert_eq!(b.await.unwrap().unwrap().as_ref(), &[101]);
            drop(groups.slots.clone().acquire_many_owned(2).await.unwrap());
            assert!(reads.try_recv().is_err());
            assert_eq!(groups.admitted.get(), 0);
            assert_eq!(scheduler.waiting.get(), 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn range_fallback_shares_the_physical_limit() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let active = metrics
                .read_scheduler_in_flight
                .with_label_values(&["test"]);
            let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
            let scheduler = ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_inflight_reads: 1.try_into().unwrap(),
                    max_admitted_groups: 1.try_into().unwrap(),
                    ..Default::default()
                },
                {
                    let active = active.clone();
                    let calls = calls.clone();
                    move |_, range| {
                        let active = active.clone();
                        let calls = calls.clone();
                        async move {
                            calls.lock().unwrap().push(range.clone());
                            assert_eq!(active.get(), 1);
                            // Let other fallback children poll; an unbounded join_all
                            // would raise the physical gauge above one here.
                            tokio::task::yield_now().await;
                            assert_eq!(active.get(), 1);
                            b"data".get(range).map(Bytes::from_static).ok_or_else(|| {
                                ObjectError::read_length_mismatch("past EOF".to_owned())
                            })
                        }
                    }
                },
            );
            let a = scheduler.submit("file", 0..2, None).await.unwrap();
            let b = scheduler.submit("file", 2..4, None).await.unwrap();
            let c = scheduler.submit("file", 4..6, None).await.unwrap();
            assert_eq!(a.await.unwrap().unwrap(), "da");
            assert_eq!(b.await.unwrap().unwrap(), "ta");
            assert!(c.await.unwrap().unwrap_err().is_read_range_error());
            assert_eq!(*calls.lock().unwrap(), vec![0..6, 0..2, 2..4, 4..6]);
            assert_eq!(active.get(), 0);
            assert_eq!(scheduler.pending.get(), 0);
            assert_eq!(scheduler.queued.get(), 0);
            let next = scheduler.submit("next", 0..1, None).await.unwrap();
            assert_eq!(next.await.unwrap().unwrap(), "d");
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn fallback_holds_group_credit_while_physical_capacity_is_available() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let (send_call, mut calls) = mpsc::unbounded_channel();
            let release = Arc::new(Notify::new());
            let scheduler = ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_groups: 1.try_into().unwrap(),
                    max_inflight_reads: 2.try_into().unwrap(),
                    ..Default::default()
                },
                {
                    let release = release.clone();
                    move |path, range| {
                        let send_call = send_call.clone();
                        let release = release.clone();
                        async move {
                            send_call.send((path.clone(), range.clone())).unwrap();
                            if path == "file" && range == (0..2) {
                                release.notified().await;
                            }
                            b"ab".get(range).map(Bytes::from_static).ok_or_else(|| {
                                ObjectError::read_length_mismatch("past EOF".to_owned())
                            })
                        }
                    }
                },
            );
            let a = scheduler.submit("file", 0..2, None).await.unwrap();
            let b = scheduler.submit("file", 2..4, None).await.unwrap();
            assert_eq!(calls.recv().await.unwrap(), ("file".to_owned(), 0..4));
            assert_eq!(calls.recv().await.unwrap(), ("file".to_owned(), 0..2));
            assert_eq!(calls.recv().await.unwrap(), ("file".to_owned(), 2..4));
            assert!(b.await.unwrap().unwrap_err().is_read_range_error());

            let other = scheduler.submit("other", 0..1, None);
            tokio::pin!(other);
            assert!(futures::poll!(&mut other).is_pending());
            assert_eq!(
                metrics
                    .read_scheduler_in_flight
                    .with_label_values(&["test"])
                    .get(),
                1
            );

            release.notify_one();
            assert_eq!(a.await.unwrap().unwrap(), "ab");
            assert_eq!(other.await.unwrap().await.unwrap().unwrap(), "a");
            assert_eq!(calls.recv().await.unwrap(), ("other".to_owned(), 0..1));
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn merged_read_is_cancelled_after_both_waiters_drop() {
        struct NotifyOnDrop(Arc<Notify>);

        impl Drop for NotifyOnDrop {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }

        let started = Arc::new(Notify::new());
        let cancelled = Arc::new(Notify::new());
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let scheduler = ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_groups: 1.try_into().unwrap(),
                ..Default::default()
            },
            {
                let started = started.clone();
                let cancelled = cancelled.clone();
                move |_, range| {
                    let started = started.clone();
                    let cancelled = cancelled.clone();
                    async move {
                        assert_eq!(range, 0..4);
                        let _guard = NotifyOnDrop(cancelled);
                        started.notify_one();
                        std::future::pending::<ReadResult>().await
                    }
                }
            },
        );
        let first = scheduler.submit("file", 0..2, None).await.unwrap();
        let second = scheduler.submit("file", 2..4, None).await.unwrap();
        started.notified().await;
        {
            let blocked = scheduler.submit("file", 4..6, None);
            tokio::pin!(blocked);
            assert!(futures::poll!(&mut blocked).is_pending());
            assert_eq!(scheduler.waiting.get(), 1);
        }
        assert_eq!(scheduler.waiting.get(), 0);
        assert_eq!(
            metrics
                .read_scheduler_pending
                .with_label_values(&["test"])
                .get(),
            2
        );
        drop(first);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), cancelled.notified())
                .await
                .is_err()
        );
        drop(second);
        tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(
            metrics
                .read_scheduler_pending
                .with_label_values(&["test"])
                .get(),
            0
        );
        assert_eq!(
            metrics
                .read_scheduler_events
                .with_label_values(&["test", "group_cancelled"])
                .get(),
            1
        );
    }
}
