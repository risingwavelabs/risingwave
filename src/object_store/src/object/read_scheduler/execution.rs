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

//! Physical dispatch, in-flight reuse, cancellation, and response delivery.
//!
//! Reserve P before spawning, but leave the group mergeable until the task selects it.
//! Selection stops range merging; execution trims cancelled ends before issuing the GET.
//! Followers can only reuse ranges fully covered by that GET.
//! Range-error fallback releases the parent's P before acquiring P for each child.

use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Instant;

use bytes::Bytes;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use risingwave_common::config::storage::ReadSchedulerConfig;
use tokio::sync::{AcquireError, Semaphore, oneshot};
use tracing::Instrument;

use super::super::ObjectErrorInner;
#[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
use super::replay;
use super::{
    ActiveRead, GroupQueue, GroupQueueWorker, ObjectError, ObjectStoreMetrics, PhysicalPermit,
    ReadGauge, ReadGroup, ReadRange, ReadRequest, ReadResult, ReadTask, WaitingRead,
};

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

// The task owns a claim on a queue position until it selects a group.
struct QueuedDispatch {
    queue: Arc<GroupQueue>,
    selected: bool,
}

impl QueuedDispatch {
    fn select(&mut self) -> Option<ReadGroup> {
        self.selected = true;
        self.queue.start_read()
    }
}

impl Drop for QueuedDispatch {
    fn drop(&mut self) {
        if !self.selected {
            let mut pending = self
                .queue
                .pending
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            pending.starting -= 1;
            pending.update_ready(&self.queue);
            self.queue.ready.notify_one();
        }
    }
}

pub(super) struct PhysicalReadReservation {
    pub(super) reserved: ReadGauge,
    pub(super) permit: PhysicalPermit,
    pub(super) reserved_at: Instant,
    pub(super) dispatch_wait: prometheus::Histogram,
}

impl PhysicalReadReservation {
    fn start(self) -> PhysicalPermit {
        drop(self.reserved);
        self.dispatch_wait
            .observe(self.reserved_at.elapsed().as_secs_f64());
        self.permit
    }
}

#[cfg(not(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay")))]
pub(super) fn acquire_physical(
    slots: Arc<Semaphore>,
) -> impl Future<Output = Result<PhysicalPermit, AcquireError>> {
    slots.acquire_owned()
}

#[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
pub(super) use replay::acquire_physical;

pub(super) async fn run<F, R>(
    mut shutdown: oneshot::Receiver<()>,
    worker: GroupQueueWorker,
    read: F,
    config: ReadSchedulerConfig,
    metrics: ObjectStoreMetrics,
    media_type: &'static str,
) where
    F: Fn(String, ReadRange) -> R + Send + Sync + 'static,
    R: Future<Output = ReadResult> + Send + 'static,
{
    let groups = &worker.0;
    let physical_slots = Arc::new(Semaphore::new(config.max_inflight_reads.get()));
    let read = Arc::new(read);
    let metrics = Arc::new(metrics);
    let cancelled_queued = metrics
        .read_scheduler_events
        .with_label_values(&[media_type, "cancelled_queued"]);
    let reserved_reads = metrics
        .read_scheduler_reserved
        .with_label_values(&[media_type]);
    let dispatch_wait = metrics
        .read_scheduler_physical_duration
        .with_label_values(&[media_type, "reservation_to_start"]);
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
            pending.update_ready(groups);
            #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
            replay::queue(&pending, "dispatch_check");
            (!pending.groups.is_empty(), pending.ready_count() > 0)
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
                let reserved_at = Instant::now();
                let reservation = PhysicalReadReservation {
                    permit,
                    reserved: ReadGauge::new(&reserved_reads),
                    reserved_at,
                    dispatch_wait: dispatch_wait.clone(),
                };
                let mut queued_dispatch = {
                    let mut pending = groups.pending.lock().unwrap();
                    if pending.ready_count() == 0 {
                        continue;
                    }
                    pending.starting += 1;
                    pending.update_ready(groups);
                    #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
                    replay::queue(&pending, "reserved");
                    QueuedDispatch { queue: groups.clone(), selected: false }
                };
                let read = read.clone();
                let physical_slots = physical_slots.clone();
                let metrics = metrics.clone();
                let execute = async move {
                    // Keep queued ranges mergeable through the existing task handoff.
                    let Some(group) = queued_dispatch.select() else {
                        return;
                    };
                    let groups = &*queued_dispatch.queue;
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
                    let sharing = config.reuse_inflight_reads.then_some((groups, &config));
                    #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
                    let id = reservation.permit.id();
                    let read = group.execute(&*read, &physical_slots, reservation, &metrics, media_type, sharing)
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
    pub(super) async fn execute<F, R>(
        mut self,
        read: &F,
        physical_slots: &Semaphore,
        reservation: PhysicalReadReservation,
        metrics: &ObjectStoreMetrics,
        media_type: &str,
        sharing: Option<(&GroupQueue, &ReadSchedulerConfig)>,
    ) where
        F: Fn(String, ReadRange) -> R,
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
        let mut span = first.range.clone();
        let mut gap_bytes = 0;
        if let ReadRange::Mergeable(span) = &mut span {
            for request in &self.requests[1..] {
                let range = request
                    .range
                    .mergeable()
                    .expect("merged group contains only bounded ranges");
                gap_bytes += range.start.saturating_sub(span.end);
                span.end = span.end.max(range.end);
            }
        }
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
        let mut partial_overlap_bytes = 0;
        let flight = if let (Some((queue, config)), Some(span)) = (sharing, span.mergeable()) {
            let mut state = queue.pending.lock().unwrap();
            let max_followers = config.max_batch_requests.get();
            // A covering GET can start between this group's admission and dispatch.
            if let Some(mut followers) =
                state.covering_read(&path, span, self.requests.len(), max_followers)
            {
                followers.as_mut().unwrap().append(&mut self.requests);
                return;
            }
            partial_overlap_bytes = state.partial_overlap_bytes(&path, span);
            // Move covered G waiters into the flight before starting the GET.
            // Waking them to retry would race a fast physical completion.
            let mut followers = Vec::new();
            while followers.len() < max_followers
                && let Some(WaitingRead {
                    read,
                    acquire,
                    waker,
                }) = state.take_covered_waiter(&path, span)
            {
                drop(acquire);
                let pending = metrics
                    .read_scheduler_pending
                    .with_label_values(&[media_type]);
                followers.push(read.into_request(&pending, None));
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
            let flight = Arc::new(ActiveRead {
                path: path.clone(),
                span: span.clone(),
                followers: Mutex::new(Some(followers)),
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
            let _permit = reservation.start();
            if partial_overlap_bytes > 0 {
                metrics
                    .read_scheduler_events
                    .with_label_values(&[media_type, "partial_overlap_dispatch"])
                    .inc();
                metrics
                    .read_scheduler_partial_overlap_bytes
                    .with_label_values(&[media_type])
                    .inc_by(partial_overlap_bytes as u64);
            }
            #[cfg(all(target_os = "linux", not(madsim), feature = "read-scheduler-replay"))]
            _permit.backend_begin();
            let _in_flight = ReadGauge::new(
                &metrics
                    .read_scheduler_in_flight
                    .with_label_values(&[media_type]),
            );
            tracing::debug!(target: "state_store_read_batch",?group_id, request_ids = ?trace_members, start = span.mergeable().map(|r| r.start), end = span.mergeable().map(|r| r.end), "read scheduler group dispatch");
            tracing::debug!(target: "object_store_read_scheduler", ?group_id,
                request_ids = ?trace_members, start = span.mergeable().map(|r| r.start), end = span.mergeable().map(|r| r.end),
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
            if span
                .mergeable()
                .is_none_or(|range| bytes.len() == range.len())
            {
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
                Ok(bytes) => {
                    let start = span.mergeable().expect("shared range").start;
                    let range = request.range.mergeable().expect("shared member range");
                    Ok(Bytes::copy_from_slice(
                        &bytes[range.start - start..range.end - start],
                    ))
                }
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
