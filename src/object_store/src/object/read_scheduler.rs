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

use std::hash::{BuildHasher, RandomState};
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;
use risingwave_common::config::storage::ReadSchedulerConfig;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tracing::Instrument;

use super::{ObjectError, ObjectErrorInner, ObjectResult, ObjectStoreMetrics};

pub(super) type ReadResult = ObjectResult<Bytes>;

pub(super) struct ReadScheduler {
    requests: mpsc::UnboundedSender<Vec<ReadRequest>>,
    preparation_slots: Arc<Semaphore>,
    preparing: prometheus::IntGauge,
    prepare_duration: prometheus::HistogramVec,
    prepared_batch_size: prometheus::Histogram,
    media_type: &'static str,
    events: prometheus::IntCounterVec,
    slots: Arc<Semaphore>,
    config: ReadSchedulerConfig,
    pending: prometheus::IntGauge,
    waiting: prometheus::IntGauge,
    queued: prometheus::IntGauge,
    admission_duration: prometheus::Histogram,
}

struct ReadRequest {
    path: String,
    range: Range<usize>,
    response: oneshot::Sender<ReadResult>,
    queued_at: Instant,
    trace_id: Option<u64>,
    // Hold the slot while the scheduler retains the request, including in an in-flight group.
    _slot: OwnedSemaphorePermit,
    _pending: ReadGauge,
    _queued: Option<ReadGauge>,
}

struct ReadGauge(prometheus::IntGauge, i64);
impl ReadGauge {
    fn new(gauge: &prometheus::IntGauge) -> Self {
        Self::with_count(gauge, 1)
    }

    fn with_count(gauge: &prometheus::IntGauge, count: i64) -> Self {
        gauge.add(count);
        Self(gauge.clone(), count)
    }

    fn shrink_to(&mut self, count: i64) {
        debug_assert!(count <= self.1);
        self.0.sub(self.1 - count);
        self.1 = count;
    }
}
impl Drop for ReadGauge {
    fn drop(&mut self) {
        self.0.sub(self.1);
    }
}

/// Retains descriptors owned by the window until they are admitted or dropped.
pub(super) struct ReadPreparation {
    permits: OwnedSemaphorePermit,
    gauge: ReadGauge,
}

impl ReadPreparation {
    fn retain(&mut self, count: usize) -> usize {
        let released = self.permits.num_permits() - count;
        if released > 0 {
            drop(self.take(released));
        }
        released
    }

    fn take(&mut self, count: usize) -> OwnedSemaphorePermit {
        let permits = self.permits.split(count).unwrap();
        self.gauge.shrink_to(self.permits.num_permits() as i64);
        permits
    }
}

pub(super) struct PreparedRead {
    path: String,
    range: Range<usize>,
    response: oneshot::Sender<ReadResult>,
    trace_id: Option<u64>,
}

impl PreparedRead {
    fn is_closed(&self) -> bool {
        self.response.is_closed()
    }

    fn trace_id(&self) -> Option<u64> {
        self.trace_id
    }

    pub(super) fn new(
        path: &str,
        range: Range<usize>,
        trace_id: Option<u64>,
    ) -> (Self, oneshot::Receiver<ReadResult>) {
        let (response, receiver) = oneshot::channel();
        (
            Self {
                path: path.to_owned(),
                range,
                response,
                trace_id,
            },
            receiver,
        )
    }

    fn admit(self, slot: OwnedSemaphorePermit, scheduler: &ReadScheduler) -> ReadRequest {
        ReadRequest {
            path: self.path,
            range: self.range,
            response: self.response,
            trace_id: self.trace_id,
            queued_at: Instant::now(),
            _slot: slot,
            _pending: ReadGauge::new(&scheduler.pending),
            _queued: Some(ReadGauge::new(&scheduler.queued)),
        }
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
        // Every descriptor entering the inbox owns either a Q or a preparation permit.
        // Those two pools bound the inbox and ready queue together with in-flight reads.
        let (requests, receiver) = mpsc::unbounded_channel();
        let started_at = Instant::now();
        tokio::spawn(run(
            receiver,
            read,
            started_at,
            config,
            metrics.clone(),
            media_type,
        ));
        Self {
            requests,
            preparation_slots: Arc::new(Semaphore::new(config.max_admitted_requests.get())),
            preparing: metrics
                .read_scheduler_preparing
                .with_label_values(&[media_type]),
            prepare_duration: metrics.read_scheduler_prepare_duration.clone(),
            prepared_batch_size: metrics
                .read_scheduler_prepared_batch_size
                .with_label_values(&[media_type]),
            media_type,
            events: metrics.read_scheduler_events.clone(),
            slots: Arc::new(Semaphore::new(config.max_admitted_requests.get())),
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
    pub(super) fn submit<'a>(
        &'a self,
        path: &'a str,
        range: Range<usize>,
        trace_id: Option<u64>,
    ) -> impl Future<Output = ObjectResult<oneshot::Receiver<ReadResult>>> + Send + 'a {
        Box::pin(async move {
            debug_assert!(self.can_schedule(&range));
            // Wait in the existing caller/foyer producer. Allocate retained request state
            // only after admission, and never turn saturation into independent IO.
            let slot = {
                let _waiting = ReadGauge::new(&self.waiting);
                let _timer = self.admission_duration.start_timer();
                tokio::select! {
                    biased;
                    _ = self.requests.closed() => return Err(ObjectError::internal("read scheduler stopped")),
                    slot = self.slots.clone().acquire_owned() => slot.map_err(|_| ObjectError::internal("read scheduler admission closed"))?,
                }
            };
            let (request, receiver) = PreparedRead::new(path, range, trace_id);
            let request = request.admit(slot, self);
            self.requests
                .send(vec![request])
                .map_err(|_| ObjectError::internal("read scheduler stopped"))?;
            Ok(receiver)
        })
    }

    pub(super) fn event(&self, event: &'static str) {
        self.events
            .with_label_values(&[self.media_type, event])
            .inc();
    }

    pub(super) async fn closed(&self) {
        self.requests.closed().await;
    }

    pub(super) fn preparation_timeout(&self) -> Duration {
        Duration::from_millis(self.config.batch_prepare_timeout_ms.into())
    }

    pub(super) fn reserve_preparation(&self, count: usize) -> Option<ReadPreparation> {
        if count <= 1 || self.preparation_timeout().is_zero() {
            return None;
        }
        let count = u32::try_from(count).ok()?;
        let Ok(permits) = self.preparation_slots.clone().try_acquire_many_owned(count) else {
            self.event("window_budget_reject");
            return None;
        };
        self.event("window_started");
        Some(ReadPreparation {
            permits,
            gauge: ReadGauge::with_count(&self.preparing, count.into()),
        })
    }

    pub(super) async fn submit_window(
        &self,
        mut requests: Vec<PreparedRead>,
        mut preparation: ReadPreparation,
        reason: &'static str,
        elapsed: Duration,
    ) {
        self.prepare_duration
            .with_label_values(&[self.media_type, reason])
            .observe(elapsed.as_secs_f64());
        requests.retain(|request| !request.is_closed());
        let released = preparation.retain(requests.len());
        if tracing::enabled!(target: "object_store_read_scheduler", tracing::Level::DEBUG) {
            let request_ids: Vec<_> = requests.iter().filter_map(PreparedRead::trace_id).collect();
            tracing::debug!(target: "object_store_read_scheduler",
                reason, ?request_ids, released_permits = released, "prepared window released");
        }
        self.prepared_batch_size.observe(requests.len() as f64);
        if reason == "window_complete" {
            match self.try_submit_whole_window(requests, &mut preparation) {
                Ok(()) => return,
                Err(unsubmitted) => requests = unsubmitted,
            }
        }
        self.submit_prepared(requests, preparation).await;
    }

    fn try_submit_whole_window(
        &self,
        requests: Vec<PreparedRead>,
        preparation: &mut ReadPreparation,
    ) -> Result<(), Vec<PreparedRead>> {
        let count = requests.len();
        if count < 2 || count > self.packet_limit() {
            return Err(requests);
        }
        let start = Instant::now();
        let (mut slots, uses_preparation) =
            match self.slots.clone().try_acquire_many_owned(count as u32) {
                Ok(slots) => {
                    preparation.retain(0);
                    (slots, false)
                }
                Err(_) if self.is_mergeable_window(&requests) => {
                    // Carry the existing descriptor budget into the queue, without waiting
                    // for a group of Q permits or consuming the Q left for ordinary reads.
                    (preparation.take(count), true)
                }
                Err(_) => return Err(requests),
            };
        let admission_duration = start.elapsed().as_secs_f64();
        let packet = requests
            .into_iter()
            .map(|request| {
                self.admission_duration.observe(admission_duration);
                request.admit(slots.split(1).unwrap(), self)
            })
            .collect();
        if self.requests.send(packet).is_ok() {
            self.event("window_packet");
            if uses_preparation {
                self.event("window_admitted_with_preparation");
            }
        }
        Ok(())
    }

    fn is_mergeable_window(&self, requests: &[PreparedRead]) -> bool {
        if requests
            .iter()
            .any(|request| request.path != requests[0].path)
        {
            return false;
        }
        // Producer polling can stage the neighbors in any order. Leave the
        // submission order intact if the window falls back to prefix admission.
        let mut ranges = requests
            .iter()
            .map(|request| &request.range)
            .sorted_unstable_by_key(|range| range.start);
        let first = ranges.next().unwrap();
        let mut end = first.end;
        ranges.all(|range| {
            if range.start > end {
                return false;
            }
            end = end.max(range.end);
            end - first.start <= self.config.max_read_bytes.get()
        })
    }

    fn packet_limit(&self) -> usize {
        self.config
            .max_batch_requests
            .get()
            .min(self.config.max_admitted_requests.get())
    }

    async fn submit_prepared(&self, requests: Vec<PreparedRead>, mut preparation: ReadPreparation) {
        // Count every logical read while it waits, including the unpublished tail
        // of a prepared window. Prefix splitting must not change metric semantics.
        let mut requests: std::collections::VecDeque<_> = requests
            .into_iter()
            .map(|request| {
                (
                    request,
                    ReadGauge::new(&self.waiting),
                    self.admission_duration.start_timer(),
                )
            })
            .collect();
        let limit = self.packet_limit();
        while let Some((mut first, waiting, timer)) = requests.pop_front() {
            let slot = {
                tokio::select! {
                    biased;
                    _ = self.requests.closed() => return,
                    _ = first.response.closed() => {
                        preparation.retain(requests.len());
                        continue;
                    }
                    slot = self.slots.clone().acquire_owned() => slot.expect("admission semaphore is never closed"),
                }
            };
            drop((waiting, timer));
            let mut packet = vec![first.admit(slot, self)];
            while packet.len() < limit {
                let Some((next, _, _)) = requests.front() else {
                    break;
                };
                if next.is_closed() {
                    requests.pop_front();
                    continue;
                }
                let Ok(slot) = self.slots.clone().try_acquire_owned() else {
                    self.event("window_split_admission");
                    tracing::debug!(target: "object_store_read_scheduler",
                        admitted_request_id = ?packet[0].trace_id,
                        next_request_id = ?requests.front().and_then(|(request, _, _)| request.trace_id()),
                        "prepared window admission split");
                    break;
                };
                let (request, waiting, timer) = requests.pop_front().unwrap();
                drop((waiting, timer));
                packet.push(request.admit(slot, self));
            }
            if packet.len() == limit && !requests.is_empty() {
                self.event("window_split_limit");
            }
            // One publication: another worker cannot select the first member
            // before seeing the other admitted members of this packet.
            preparation.retain(requests.len());
            if self.requests.send(packet).is_err() {
                return;
            }
            self.event("window_packet");
            // Never await further Q while retaining an unpublished admitted prefix.
        }
    }
}

async fn run<F, R>(
    mut receiver: mpsc::UnboundedReceiver<Vec<ReadRequest>>,
    read: F,
    started_at: Instant,
    config: ReadSchedulerConfig,
    metrics: ObjectStoreMetrics,
    media_type: &'static str,
) where
    F: Fn(String, Range<usize>) -> R + Send + Sync + 'static,
    R: Future<Output = ReadResult> + Send + 'static,
{
    let physical_slots = Arc::new(Semaphore::new(config.max_inflight_reads.get()));
    let read = Arc::new(read);
    let metrics = Arc::new(metrics);
    let max_batch = config
        .max_batch_requests
        .get()
        .min(config.max_admitted_requests.get());
    // Keep a packet whole when it does not fit the remaining logical receive
    // budget. It already owns retention permits; the next pass consumes it first.
    let mut deferred_packet = None;
    let mut incoming = Vec::new();
    let mut ready = Vec::new();
    let mut in_flight = FuturesUnordered::new();
    let mut closed = false;
    let trace_hash = RandomState::new();
    loop {
        let received = ready.len();
        ready.retain(|request: &ReadRequest| !request.response.is_closed());
        metrics
            .read_scheduler_events
            .with_label_values(&[media_type, "cancelled_queued"])
            .inc_by((received - ready.len()) as u64);
        let has_ready = !ready.is_empty();
        if closed && !has_ready && in_flight.is_empty() && deferred_packet.is_none() {
            return;
        }
        tokio::select! {
            // Progress existing work before accepting more input. A stream of
            // cancelled/replaced requests must not starve an older ready request.
            biased;
            completed = in_flight.next(), if !in_flight.is_empty() => {
                if let Some(Err(error)) = completed {
                    let error: tokio::task::JoinError = error;
                    if error.is_panic() { std::panic::resume_unwind(error.into_panic()); }
                    panic!("scheduler physical task cancelled unexpectedly: {error}");
                }
            }
            _ = std::future::poll_fn(|cx| {
                for request in &mut ready {
                    if request.response.poll_closed(cx).is_ready() {
                        return Poll::Ready(());
                    }
                }
                Poll::Pending
            }), if has_ready => {}
            permit = physical_slots.clone().acquire_owned(), if has_ready => {
                let permit = permit.expect("scheduler physical semaphore is never closed");
                // Include already-available neighbors without waiting or indefinitely
                // draining a producer that continually replaces cancelled requests.
                closed |= collect_ready_requests(&mut receiver, &mut deferred_packet, &mut incoming, max_batch);
                record_read_batch(&incoming, &trace_hash, started_at, &metrics, media_type);
                ready.append(&mut incoming);
                let received = ready.len();
                ready.retain(|request| !request.response.is_closed());
                metrics.read_scheduler_events.with_label_values(&[media_type, "cancelled_queued"])
                    .inc_by((received - ready.len()) as u64);
                if !ready.is_empty() {
                    let group = take_read_group(&mut ready, config.max_read_bytes.get(), &metrics, media_type);
                    let group_span = tracing::debug_span!(
                        target: "object_store_read_scheduler", parent: None, "object_read_group"
                    );
                    if tracing::enabled!(target: "object_store_read_scheduler", tracing::Level::DEBUG) {
                        let request_ids: Vec<_> = group.requests.iter().filter_map(|r| r.trace_id).collect();
                        tracing::debug!(target: "object_store_read_scheduler", parent: &group_span,
                            group_id = ?request_ids.first(), ?request_ids,
                            ready = ready.len(), channel = receiver.len(),
                            available_permits = physical_slots.available_permits(),
                            "scheduler group selected");
                    }
                    let read = read.clone();
                    let physical_slots = physical_slots.clone();
                    let metrics = metrics.clone();
                    let execute = async move { group.execute(&*read, &physical_slots, permit, &metrics, media_type).await }
                        .instrument(group_span);
                    in_flight.push(ReadTask(tokio::spawn(execute)));
                }
            }
            packet = async {
                match deferred_packet.take() {
                    Some(packet) => Some(packet),
                    None => receiver.recv().await,
                }
            }, if !closed || deferred_packet.is_some() => {
                if let Some(mut packet) = packet {
                    incoming.append(&mut packet);
                    closed |= collect_ready_requests(&mut receiver, &mut deferred_packet, &mut incoming, max_batch);
                } else {
                    closed = true;
                }
                record_read_batch(&incoming, &trace_hash, started_at, &metrics, media_type);
                ready.append(&mut incoming);
            }
        }
    }
}

/// Collect already-ready work within a logical request budget. Never wait for
/// more requests, split a packet, or turn B packets into B-squared planning work.
fn collect_ready_requests(
    receiver: &mut mpsc::UnboundedReceiver<Vec<ReadRequest>>,
    deferred: &mut Option<Vec<ReadRequest>>,
    incoming: &mut Vec<ReadRequest>,
    limit: usize,
) -> bool {
    while incoming.len() < limit {
        let mut packet = match deferred
            .take()
            .map(Ok)
            .unwrap_or_else(|| receiver.try_recv())
        {
            Ok(packet) => packet,
            Err(mpsc::error::TryRecvError::Empty) => return false,
            Err(mpsc::error::TryRecvError::Disconnected) => return true,
        };
        if incoming.len() + packet.len() > limit {
            *deferred = Some(packet);
            break;
        }
        incoming.append(&mut packet);
    }
    false
}

fn record_read_batch(
    ready: &[ReadRequest],
    hash: &RandomState,
    started_at: Instant,
    metrics: &ObjectStoreMetrics,
    media_type: &str,
) {
    if ready.is_empty() {
        return;
    }
    metrics
        .read_scheduler_batch_size
        .with_label_values(&[media_type])
        .observe(ready.len() as f64);
    if !tracing::enabled!(target: "state_store_read_batch", tracing::Level::DEBUG)
        && !tracing::enabled!(target: "object_store_read_scheduler", tracing::Level::DEBUG)
    {
        return;
    }
    let dequeued_us = started_at.elapsed().as_micros() as u64;
    // Record (object hash, start, end, enqueue time in microseconds) without exposing object paths.
    let requests: Vec<_> = ready
        .iter()
        .map(|request| {
            (
                hash.hash_one(&request.path),
                request.range.start,
                request.range.end,
                request.queued_at.duration_since(started_at).as_micros() as u64,
            )
        })
        .collect();
    let request_ids: Vec<_> = ready.iter().map(|request| request.trace_id).collect();
    let dequeue_span = tracing::debug_span!(
        target: "object_store_read_scheduler", parent: None, "object_read_dequeue"
    );
    tracing::debug!(target: "state_store_read_batch", parent: &dequeue_span,
        scheduler = hash.hash_one(started_at),
        ?request_ids,
        dequeued_us,
        ?requests,
        "read scheduler batch"
    );
    tracing::debug!(target: "object_store_read_scheduler", parent: &dequeue_span,
        scheduler = hash.hash_one(started_at), ?request_ids, dequeued_us, ?requests,
        "scheduler requests dequeued"
    );
}

// Plan only a group that can dispatch now. Unsubmitted members remain available
// for merging with later arrivals. The oldest member chooses the next group so
// sorting by object cannot starve a different object's older request.
fn take_read_group(
    ready: &mut Vec<ReadRequest>,
    max_bytes: usize,
    metrics: &ObjectStoreMetrics,
    media_type: &str,
) -> ReadGroup {
    ready.sort_unstable_by(|a, b| (&a.path, a.range.start).cmp(&(&b.path, b.range.start)));
    let oldest = ready
        .iter()
        .enumerate()
        .min_by_key(|(_, request)| request.queued_at)
        .expect("dispatch requires a pending request")
        .0;
    let mut start = 0;
    let mut span = ready[0].range.clone();
    let mut end = 1;
    while end < ready.len() {
        let next = &ready[end];
        let reason = if ready[start].path != next.path {
            Some("split_object")
        } else if next.range.start > span.end {
            Some("split_gap")
        } else if span.end.max(next.range.end) - span.start > max_bytes {
            Some("split_bytes")
        } else {
            None
        };
        if let Some(reason) = reason {
            if oldest < end {
                // Count only the boundary of the selected group, not all groups
                // considered again while they are waiting for dispatch.
                metrics
                    .read_scheduler_events
                    .with_label_values(&[media_type, reason])
                    .inc();
                if reason == "split_gap" {
                    metrics
                        .read_scheduler_gap_bytes
                        .with_label_values(&[media_type])
                        .observe((next.range.start - span.end) as f64);
                }
                break;
            }
            start = end;
            span = next.range.clone();
        } else {
            span.end = span.end.max(next.range.end);
        }
        end += 1;
    }
    ReadGroup {
        requests: ready.drain(start..end).collect(),
    }
}

impl ReadGroup {
    async fn execute<F, R>(
        mut self,
        read: &F,
        physical_slots: &Semaphore,
        permit: OwnedSemaphorePermit,
        metrics: &ObjectStoreMetrics,
        media_type: &str,
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
        let end = self
            .requests
            .iter()
            .map(|request| request.range.end)
            .max()
            .unwrap();
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
            let result = read(path, span.clone()).await;
            tracing::debug!(target: "object_store_read_scheduler", ?group_id,
                ok = result.is_ok(), "scheduler physical read completed");
            tracing::debug!(target: "state_store_read_batch",
                ?group_id,
                ok = result.is_ok(),
                "read scheduler group completed"
            );
            result
        };
        let result = read_while_wanted(&mut self.requests, physical).await;
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
            metrics
                .read_scheduler_events
                .with_label_values(&[media_type, "merged_requests"])
                .inc_by(self.requests.len().saturating_sub(1) as u64);
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
                    let _permit = physical_slots.acquire().await
                        .expect("scheduler physical semaphore is never closed");
                    let _in_flight = ReadGauge::new(&metrics.read_scheduler_in_flight.with_label_values(&[media_type]));
                    tracing::debug!(target: "state_store_read_batch",?group_id, request_id = ?trace_id,
                        "read scheduler range fallback dispatch");
                    let result = read(path, range).await;
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

    use tokio::sync::Notify;

    use super::super::{InMemObjectStore, ObjectStore, ObjectStoreConfig};
    use super::*;

    fn batch_scheduler(q: usize) -> (Arc<ReadScheduler>, Arc<std::sync::Mutex<Vec<Range<usize>>>>) {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let scheduler = Arc::new(ReadScheduler::new(
            &ObjectStoreMetrics::new(&prometheus::Registry::new()),
            "test",
            ReadSchedulerConfig {
                max_admitted_requests: q.try_into().unwrap(),
                batch_prepare_timeout_ms: 100,
                ..Default::default()
            },
            {
                let calls = calls.clone();
                move |_, range| {
                    let calls = calls.clone();
                    async move {
                        calls.lock().unwrap().push(range.clone());
                        Ok(Bytes::from_static(&b"abcdefgh"[range]))
                    }
                }
            },
        ));
        (scheduler, calls)
    }

    #[tokio::test]
    async fn explicit_windows_preserve_ready_request_collection() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let scheduler = ReadScheduler::new(
            &ObjectStoreMetrics::new(&prometheus::Registry::new()),
            "test",
            ReadSchedulerConfig {
                batch_prepare_timeout_ms: 1,
                max_batch_requests: 4.try_into().unwrap(),
                ..Default::default()
            },
            {
                let calls = calls.clone();
                move |path, range| {
                    let calls = calls.clone();
                    async move {
                        calls.lock().unwrap().push((path, range.clone()));
                        Ok(Bytes::from_static(&b"abcdefgh"[range]))
                    }
                }
            },
        );
        // All three packets are ready before the dispatcher runs. Enabling
        // explicit windows must not hide B behind a different object's packet.
        let a = scheduler.submit("file", 0..2, None).await.unwrap();
        let other = scheduler.submit("other", 0..2, None).await.unwrap();
        let b = scheduler.submit("file", 2..4, None).await.unwrap();
        assert_eq!(a.await.unwrap().unwrap(), "ab");
        assert_eq!(b.await.unwrap().unwrap(), "cd");
        assert_eq!(other.await.unwrap().unwrap(), "ab");
        let calls = calls.lock().unwrap();
        let file_reads: Vec<_> = calls
            .iter()
            .filter(|(path, _)| path == "file")
            .map(|(_, range)| range.clone())
            .collect();
        assert_eq!(file_reads, vec![0..4]);
    }

    #[tokio::test]
    async fn packet_overflow_preserves_logical_budget_and_shutdown_progress() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let batches = metrics
                .read_scheduler_batch_size
                .with_label_values(&["test"]);
            let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
            let scheduler = ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    batch_prepare_timeout_ms: 1,
                    max_batch_requests: 3.try_into().unwrap(),
                    ..Default::default()
                },
                |_, range| async move { Ok(Bytes::from_static(&b"abcdefghijkl"[range])) },
            );
            let mut responses = Vec::new();
            for start in [0, 4, 8] {
                let mut packet = Vec::new();
                for start in [start, start + 2] {
                    let (request, response) = PreparedRead::new("file", start..start + 2, None);
                    packet.push(request);
                    responses.push(response);
                }
                let preparation = scheduler.reserve_preparation(packet.len()).unwrap();
                scheduler.submit_prepared(packet, preparation).await;
            }
            // A second two-member packet cannot fit a remaining budget of one.
            // Closing the input must still drain that deferred packet whole.
            drop(scheduler);
            for (i, response) in responses.into_iter().enumerate() {
                assert_eq!(
                    &response.await.unwrap().unwrap()[..],
                    &b"abcdefghijkl"[i * 2..i * 2 + 2]
                );
            }
            assert_eq!(batches.get_sample_count(), 3);
            assert_eq!(batches.get_sample_sum(), 6.0);
            assert_eq!(pending.get(), 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn window_publishes_ready_members_together() {
        let (scheduler, calls) = batch_scheduler(3);
        let batch = super::super::ReadBatch::new(scheduler.clone(), 3).unwrap();
        let (a_role, a) = batch.register().unwrap();
        let (b_role, b) = batch.register().unwrap();
        let (hit_role, hit) = batch.register().unwrap();
        // Outer IO can be polled before registration has returned its role.
        let a = a.stage("file", 0..2, None);
        tokio::pin!(a);
        assert!(futures::poll!(&mut a).is_pending());
        a_role.set_leader(true);
        let a = a.await.unwrap();
        b_role.set_leader(true);
        let b = b.stage("file", 2..4, None).await.unwrap();
        batch.seal();
        tokio::task::yield_now().await;
        assert!(calls.lock().unwrap().is_empty());
        // A memory hit / joined producer excludes this slot immediately, without
        // waiting for a retained fallback or for the iterator to consume it.
        let hit = hit.stage("file", 4..6, None);
        tokio::pin!(hit);
        assert!(futures::poll!(&mut hit).is_pending());
        hit_role.set_leader(false);
        assert!(hit.await.is_none());
        assert_eq!(a.await.unwrap().unwrap(), "ab");
        assert_eq!(b.await.unwrap().unwrap(), "cd");
        assert_eq!(*calls.lock().unwrap(), vec![0..4]);
        assert_eq!(scheduler.preparing.get(), 0);
        assert_eq!(scheduler.pending.get(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn whole_window_progresses_with_partial_q_and_releases_its_permits() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let in_flight = metrics
                .read_scheduler_in_flight
                .with_label_values(&["test"]);
            let (record, mut calls) = mpsc::unbounded_channel();
            let release_window = Arc::new(Notify::new());
            let scheduler = Arc::new(ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_requests: 3.try_into().unwrap(),
                    max_inflight_reads: 3.try_into().unwrap(),
                    ..Default::default()
                },
                {
                    let release_window = release_window.clone();
                    move |path: String, range: Range<usize>| {
                        let record = record.clone();
                        let release_window = release_window.clone();
                        async move {
                            record.send((path.clone(), range.clone())).unwrap();
                            match path.as_str() {
                                "blocker" => std::future::pending().await,
                                "window" => release_window.notified().await,
                                _ => {}
                            }
                            Ok(Bytes::from_static(&b"abcdefgh"[range]))
                        }
                    }
                },
            ));

            // One real GET holds two Q permits and only one P permit.
            let (x, x_result) = PreparedRead::new("blocker", 0..2, None);
            let (y, y_result) = PreparedRead::new("blocker", 2..4, None);
            let preparation = scheduler.reserve_preparation(2).unwrap();
            scheduler.submit_prepared(vec![x, y], preparation).await;
            assert_eq!(calls.recv().await.unwrap(), ("blocker".to_owned(), 0..4));
            assert_eq!(scheduler.slots.available_permits(), 1);

            for cancel in [false, true] {
                let batch = super::super::ReadBatch::new(scheduler.clone(), 2).unwrap();
                let (b_role, b) = batch.register().unwrap();
                let (a_role, a) = batch.register().unwrap();
                b_role.set_leader(true);
                a_role.set_leader(true);
                let b = b.stage("window", 2..4, None).await.unwrap();
                let a = a.stage("window", 0..2, None).await.unwrap();
                batch.seal();
                assert_eq!(calls.recv().await.unwrap(), ("window".to_owned(), 0..4));
                assert_eq!(scheduler.slots.available_permits(), 1);
                assert_eq!(scheduler.preparation_slots.available_permits(), 1);

                // The other two GETs are held, so this must use the remaining Q and P.
                let point = scheduler.submit("point", 4..5, None).await.unwrap();
                assert_eq!(point.await.unwrap().unwrap(), "e");
                assert_eq!(calls.recv().await.unwrap(), ("point".to_owned(), 4..5));

                if cancel {
                    drop((a, b));
                } else {
                    release_window.notify_one();
                    assert_eq!(a.await.unwrap().unwrap(), "ab");
                    assert_eq!(b.await.unwrap().unwrap(), "cd");
                }
                drop(
                    scheduler
                        .preparation_slots
                        .clone()
                        .acquire_many_owned(3)
                        .await
                        .unwrap(),
                );
                assert_eq!(scheduler.preparing.get(), 0);
                assert_eq!(scheduler.pending.get(), 2);
                assert_eq!(scheduler.slots.available_permits(), 1);
                assert_eq!(in_flight.get(), 1);
                assert!(calls.try_recv().is_err());
            }
            drop((x_result, y_result));
            drop(scheduler.slots.clone().acquire_many_owned(3).await.unwrap());
            assert_eq!(scheduler.pending.get(), 0);
            assert_eq!(in_flight.get(), 0);
            assert_eq!(
                scheduler
                    .events
                    .with_label_values(&["test", "window_admitted_with_preparation"])
                    .get(),
                2
            );
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn window_splits_at_available_q_without_holding_partial_admission() {
        tokio::time::timeout(Duration::from_secs(5), async {
            for (q, expected_splits) in [(2, 1), (3, 0)] {
                let (scheduler, calls) = batch_scheduler(q);
                let held = scheduler.slots.clone().acquire_owned().await.unwrap();
                let batch = super::super::ReadBatch::new(scheduler.clone(), 2).unwrap();
                let (a_role, a) = batch.register().unwrap();
                let (b_role, b) = batch.register().unwrap();
                a_role.set_leader(true);
                b_role.set_leader(true);
                let a = a.stage("file", 0..2, None).await.unwrap();
                // A gap makes this window ineligible to use preparation permits.
                let b = b.stage("file", 4..6, None).await.unwrap();
                batch.seal();
                assert_eq!(a.await.unwrap().unwrap(), "ab");
                assert_eq!(b.await.unwrap().unwrap(), "ef");
                assert_eq!(*calls.lock().unwrap(), vec![0..2, 4..6]);
                assert_eq!(
                    scheduler
                        .events
                        .with_label_values(&["test", "window_complete"])
                        .get(),
                    1
                );
                assert_eq!(
                    scheduler
                        .events
                        .with_label_values(&["test", "window_split_admission"])
                        .get(),
                    expected_splits
                );
                assert_eq!(scheduler.slots.available_permits(), q - 1);
                assert_eq!(scheduler.preparation_slots.available_permits(), q);
                drop(held);
            }
            let (q1, _) = batch_scheduler(1);
            assert!(super::super::ReadBatch::new(q1.clone(), 2).is_none());
            q1.submit("file", 0..2, None)
                .await
                .unwrap()
                .await
                .unwrap()
                .unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn published_prefix_releases_preparation_while_tail_waits() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let (record, mut calls) = mpsc::unbounded_channel();
            let scheduler = Arc::new(ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_requests: 2.try_into().unwrap(),
                    ..Default::default()
                },
                move |_, range: Range<usize>| {
                    let record = record.clone();
                    async move {
                        record.send(range.clone()).unwrap();
                        if range.start == 0 {
                            std::future::pending::<()>().await;
                        }
                        Ok(Bytes::from_static(&b"abcdefgh"[range]))
                    }
                },
            ));
            let held = scheduler.slots.clone().acquire_owned().await.unwrap();
            let batch = super::super::ReadBatch::new(scheduler.clone(), 2).unwrap();
            let (first_role, first) = batch.register().unwrap();
            let (tail_role, tail) = batch.register().unwrap();
            first_role.set_leader(true);
            tail_role.set_leader(true);
            let first = first.stage("file", 0..2, None).await.unwrap();
            // The gap keeps the window on the ordinary admission path.
            let tail = tail.stage("file", 4..6, None).await.unwrap();
            batch.seal();

            assert_eq!(calls.recv().await.unwrap(), 0..2);
            assert_eq!(scheduler.slots.available_permits(), 0);
            assert_eq!(scheduler.preparation_slots.available_permits(), 1);
            assert_eq!(scheduler.preparing.get(), 1);
            drop(held);
            assert_eq!(tail.await.unwrap().unwrap(), "ef");
            assert_eq!(calls.recv().await.unwrap(), 4..6);
            assert_eq!(scheduler.preparation_slots.available_permits(), 2);
            assert_eq!(scheduler.preparing.get(), 0);
            drop(first);
            drop(scheduler.slots.clone().acquire_many_owned(2).await.unwrap());
        })
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn window_timeout_retains_preparation_lease_through_admission() {
        let (scheduler, calls) = batch_scheduler(3);
        let held = scheduler.slots.clone().acquire_many_owned(3).await.unwrap();
        let batch = super::super::ReadBatch::new(scheduler.clone(), 3).unwrap();
        let (role, member) = batch.register().unwrap();
        let (late_role, late) = batch.register().unwrap();
        role.set_leader(true);
        late_role.set_leader(true);
        let response = member.stage("file", 0..2, None).await.unwrap();
        batch.seal();
        tokio::time::advance(Duration::from_millis(101)).await;
        tokio::task::yield_now().await;
        assert!(calls.lock().unwrap().is_empty());
        assert_eq!(scheduler.preparing.get(), 1);
        assert_eq!(scheduler.preparation_slots.available_permits(), 2);
        let next = super::super::ReadBatch::new(scheduler.clone(), 2).unwrap();
        assert_eq!(scheduler.preparation_slots.available_permits(), 0);
        drop(next);
        assert!(late.stage("file", 2..4, None).await.is_none());
        drop(held);
        assert_eq!(response.await.unwrap().unwrap(), "ab");
        assert_eq!(*calls.lock().unwrap(), vec![0..2]);
        assert_eq!(scheduler.preparing.get(), 0);
        assert_eq!(scheduler.preparation_slots.available_permits(), 3);
    }

    #[tokio::test]
    async fn window_cancellation_dissolves_without_cancelling_shared_reads() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (scheduler, calls) = batch_scheduler(2);
            let batch = super::super::ReadBatch::new(scheduler.clone(), 2).unwrap();
            let (role, member) = batch.register().unwrap();
            let (late_role, late) = batch.register().unwrap();
            role.set_leader(true);
            let response = member.stage("file", 0..2, None).await.unwrap();
            batch.seal();
            drop(batch);
            // Dissolving releases staged work; it does not abort a Foyer producer.
            assert_eq!(response.await.unwrap().unwrap(), "ab");
            late_role.set_leader(true);
            assert!(late.stage("file", 2..4, None).await.is_none());
            assert_eq!(*calls.lock().unwrap(), vec![0..2]);
            assert_eq!(scheduler.preparing.get(), 0);

            let held = scheduler.slots.clone().acquire_many_owned(2).await.unwrap();
            let batch = super::super::ReadBatch::new(scheduler.clone(), 2).unwrap();
            let (role, member) = batch.register().unwrap();
            role.set_leader(true);
            let response = member.stage("file", 4..6, None).await.unwrap();
            batch.seal();
            tokio::task::yield_now().await;
            drop(response);
            // Cancelled admission must release preparation without requiring Q.
            while scheduler.preparing.get() != 0 {
                tokio::task::yield_now().await;
            }
            assert_eq!(*calls.lock().unwrap(), vec![0..2]);
            drop(held);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn window_unpolled_member_and_registration_drop_resolve_once() {
        let (scheduler, calls) = batch_scheduler(3);
        let batch = super::super::ReadBatch::new(scheduler.clone(), 3).unwrap();
        let (role, member) = batch.register().unwrap();
        let (unused_role, unused) = batch.register().unwrap();
        let (unknown_role, unknown) = batch.register().unwrap();
        role.set_leader(true);
        let response = member.stage("file", 0..2, None).await.unwrap();
        batch.seal();
        // Hit/disk-hit producers can drop their outer fetch before role returns.
        drop(unused);
        unused_role.set_leader(true);
        drop(unknown_role);
        assert!(unknown.stage("file", 2..4, None).await.is_none());
        assert_eq!(response.await.unwrap().unwrap(), "ab");
        assert_eq!(*calls.lock().unwrap(), vec![0..2]);
        assert_eq!(scheduler.preparing.get(), 0);
    }

    #[tokio::test]
    async fn read_coalesces_adjacent_ranges_on_the_same_object() {
        let metrics = Arc::new(ObjectStoreMetrics::new(&prometheus::Registry::new()));
        let config = ObjectStoreConfig {
            read_scheduler: ReadSchedulerConfig {
                max_batch_requests: 4.try_into().unwrap(),
                max_admitted_requests: 7.try_into().unwrap(),
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
        let slots = Arc::new(Semaphore::new(1));
        let (sender, receiver) = mpsc::unbounded_channel();
        let (response, result) = oneshot::channel();
        sender
            .send(vec![ReadRequest {
                path: "file".to_owned(),
                range: 0..1,
                response,
                queued_at: Instant::now(),
                trace_id: None,
                _slot: slots.clone().acquire_owned().await.unwrap(),
                _pending: ReadGauge::new(&pending),
                _queued: None,
            }])
            .unwrap();
        let started = Arc::new(Notify::new());
        let dispatcher = tokio::spawn(run(
            receiver,
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
            Instant::now(),
            ReadSchedulerConfig::default(),
            metrics,
            "test",
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            started.notified().await;
            assert_eq!(active.get(), 1);
            dispatcher.abort();
            assert!(dispatcher.await.unwrap_err().is_cancelled());
            assert!(result.await.is_err());
            assert_eq!(active.get(), 0);
            assert_eq!(pending.get(), 0);
            assert_eq!(slots.available_permits(), 1);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cancellation_after_planning_is_accounted_before_dispatch() {
        for cancel_all in [false, true] {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
            let slots = Arc::new(Semaphore::new(3));
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
                    _slot: slots.clone().try_acquire_owned().unwrap(),
                    _pending: ReadGauge::new(&pending),
                    _queued: None,
                });
                responses.push(receiver);
            }
            let group = take_read_group(&mut requests, 12, &metrics, "test");
            assert!(requests.is_empty());
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
                    physical_slots.clone().acquire_owned().await.unwrap(),
                    &metrics,
                    "test",
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
            assert_eq!(slots.available_permits(), 3);
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
    async fn ready_reads_dispatch_before_draining_all_input() {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let scheduler = ReadScheduler::new(
            &metrics,
            "test",
            ReadSchedulerConfig {
                max_admitted_requests: 6.try_into().unwrap(),
                max_batch_requests: 1.try_into().unwrap(),
                max_inflight_reads: 1.try_into().unwrap(),
                ..Default::default()
            },
            |_, range| async move {
                if range.start == 0 {
                    // The selected range records which requests were visible at selection.
                    // The scheduler may dequeue more before the spawned read is polled.
                    assert!(range.end <= 2, "first selected range: {range:?}");
                }
                Ok(Bytes::from_static(&b"xxxxxx"[range]))
            },
        );
        let mut responses = Vec::new();
        for start in 0..6 {
            responses.push(
                scheduler
                    .submit("file", start..start + 1, None)
                    .await
                    .unwrap(),
            );
        }
        for response in responses {
            assert_eq!(response.await.unwrap().unwrap(), "x");
        }
    }

    #[tokio::test]
    async fn pending_neighbors_merge_when_physical_capacity_returns() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let batches = metrics
                .read_scheduler_batch_size
                .with_label_values(&["test"]);
            let started = Arc::new(Notify::new());
            let release = Arc::new(Notify::new());
            let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
            let scheduler = ReadScheduler::new(
                &metrics,
                "test",
                ReadSchedulerConfig {
                    max_admitted_requests: 5.try_into().unwrap(),
                    max_inflight_reads: 1.try_into().unwrap(),
                    max_batch_requests: 1.try_into().unwrap(),
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
            // A single request dispatches without waiting for more work.
            let busy = scheduler.submit("busy", 0..1, None).await.unwrap();
            started.notified().await;
            let a = scheduler.submit("file", 0..2, None).await.unwrap();
            while batches.get_sample_count() < 2 {
                tokio::task::yield_now().await;
            }
            // A is already dequeued, but must still be available to merge with B.
            let b = scheduler.submit("file", 2..4, None).await.unwrap();
            let cancelled = scheduler.submit("cancelled", 0..1, None).await.unwrap();
            while batches.get_sample_count() < 4 {
                tokio::task::yield_now().await;
            }
            drop(cancelled);
            while scheduler.pending.get() != 3 {
                tokio::task::yield_now().await;
            }
            assert_eq!(scheduler.queued.get(), 2);
            // Sorting by object must not put this newer request ahead of A+B.
            let newer = scheduler.submit("aaa", 0..1, None).await.unwrap();
            while batches.get_sample_count() < 5 {
                tokio::task::yield_now().await;
            }
            assert_eq!(calls.lock().unwrap().len(), 1);
            release.notify_one();
            assert_eq!(busy.await.unwrap().unwrap(), "a");
            assert_eq!(a.await.unwrap().unwrap(), "ab");
            assert_eq!(b.await.unwrap().unwrap(), "cd");
            assert_eq!(newer.await.unwrap().unwrap(), "a");
            assert_eq!(
                *calls.lock().unwrap(),
                vec![
                    ("busy".to_owned(), 0..1),
                    ("file".to_owned(), 0..4),
                    ("aaa".to_owned(), 0..1),
                ]
            );
            assert_eq!(scheduler.queued.get(), 0);
            assert_eq!(scheduler.pending.get(), 0);
            assert_eq!(scheduler.slots.available_permits(), 5);
            assert_eq!(
                metrics
                    .read_scheduler_in_flight
                    .with_label_values(&["test"])
                    .get(),
                0
            );
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
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn saturated_reads_wait_and_still_coalesce_after_admission() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let scheduler = ReadScheduler::new(
            &ObjectStoreMetrics::new(&prometheus::Registry::new()),
            "test",
            ReadSchedulerConfig {
                max_admitted_requests: 2.try_into().unwrap(),
                ..Default::default()
            },
            {
                let started = started.clone();
                let release = release.clone();
                let calls = calls.clone();
                move |_, range| {
                    let started = started.clone();
                    let release = release.clone();
                    let calls = calls.clone();
                    async move {
                        calls.lock().unwrap().push(range.clone());
                        if range == (0..4) {
                            started.notify_one();
                            release.notified().await;
                        }
                        Ok(Bytes::from_static(&b"abcdefgh"[range]))
                    }
                }
            },
        );
        let a = scheduler.submit("file", 0..2, None).await.unwrap();
        let b = scheduler.submit("file", 2..4, None).await.unwrap();
        started.notified().await;
        let c = scheduler.submit("file", 4..6, None);
        let d = scheduler.submit("file", 6..8, None);
        tokio::pin!(c, d);
        assert!(futures::poll!(&mut c).is_pending());
        assert!(futures::poll!(&mut d).is_pending());
        assert_eq!(scheduler.waiting.get(), 2);
        assert_eq!(scheduler.pending.get(), 2);
        assert_eq!(*calls.lock().unwrap(), vec![0..4]);
        release.notify_one();
        assert_eq!(a.await.unwrap().unwrap(), "ab");
        assert_eq!(b.await.unwrap().unwrap(), "cd");
        // Poll both newly admitted requests before yielding to the dispatcher.
        // This forces a ready batch without relying on a wall-clock delay.
        let (c, d) = tokio::join!(c, d);
        assert_eq!(c.unwrap().await.unwrap().unwrap(), "ef");
        assert_eq!(d.unwrap().await.unwrap().unwrap(), "gh");
        assert_eq!(*calls.lock().unwrap(), vec![0..4, 4..8]);
        assert_eq!(scheduler.waiting.get(), 0);
        assert_eq!(scheduler.pending.get(), 0);
        assert_eq!(scheduler.slots.available_permits(), 2);
        assert_eq!(scheduler.admission_duration.get_sample_count(), 4);
    }

    #[tokio::test]
    async fn dispatcher_closure_wakes_admission_waiters() {
        let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
        let (requests, receiver) = mpsc::unbounded_channel();
        let scheduler = ReadScheduler {
            requests,
            preparation_slots: Arc::new(Semaphore::new(1)),
            preparing: metrics
                .read_scheduler_preparing
                .with_label_values(&["test"]),
            prepare_duration: metrics.read_scheduler_prepare_duration.clone(),
            prepared_batch_size: metrics
                .read_scheduler_prepared_batch_size
                .with_label_values(&["test"]),
            media_type: "test",
            events: metrics.read_scheduler_events.clone(),
            slots: Arc::new(Semaphore::new(1)),
            config: ReadSchedulerConfig::default(),
            pending: metrics.read_scheduler_pending.with_label_values(&["test"]),
            waiting: metrics.read_scheduler_waiting.with_label_values(&["test"]),
            queued: metrics.read_scheduler_queued.with_label_values(&["test"]),
            admission_duration: metrics
                .read_scheduler_admission_duration
                .with_label_values(&["test"]),
        };
        let slot = scheduler.slots.clone().acquire_owned().await.unwrap();
        let blocked = scheduler.submit("file", 0..2, None);
        tokio::pin!(blocked);
        assert!(futures::poll!(&mut blocked).is_pending());
        assert_eq!(scheduler.waiting.get(), 1);
        drop(receiver);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), blocked)
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(scheduler.waiting.get(), 0);
        assert_eq!(scheduler.pending.get(), 0);
        drop(slot);
        assert_eq!(scheduler.slots.available_permits(), 1);
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
                max_admitted_requests: 2.try_into().unwrap(),
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
        started.notified().await;
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
