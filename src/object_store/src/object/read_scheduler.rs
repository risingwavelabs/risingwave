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
use std::time::Instant;

use bytes::Bytes;
use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use prometheus::Histogram;
use risingwave_common::config::storage::ReadSchedulerConfig;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::{ObjectError, ObjectErrorInner, ObjectResult, ObjectStoreMetrics};

type ReadResult = ObjectResult<Bytes>;

pub(super) struct ReadScheduler {
    requests: mpsc::Sender<ReadRequest>,
    slots: Arc<Semaphore>,
    config: ReadSchedulerConfig,
    pending: prometheus::IntGauge,
}

struct ReadRequest {
    path: String,
    range: Range<usize>,
    response: oneshot::Sender<ReadResult>,
    queued_at: Instant,
    trace_id: Option<u64>,
    // Hold the slot while the scheduler retains the request, including in an in-flight group.
    _slot: OwnedSemaphorePermit,
    _pending: PendingRead,
}

struct PendingRead(prometheus::IntGauge);
impl Drop for PendingRead {
    fn drop(&mut self) {
        self.0.dec();
    }
}

struct ReadGroup {
    range: Range<usize>,
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
        let (requests, receiver) = mpsc::channel(config.max_admitted_requests.get());
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
            slots: Arc::new(Semaphore::new(config.max_admitted_requests.get())),
            config,
            pending: metrics
                .read_scheduler_pending
                .with_label_values(&[media_type]),
        }
    }

    pub(super) fn try_submit(
        &self,
        path: &str,
        range: Range<usize>,
        trace_id: Option<u64>,
    ) -> Option<oneshot::Receiver<ReadResult>> {
        debug_assert!(self.can_schedule(&range));
        let slot = self.slots.clone().try_acquire_owned().ok()?;
        let (response, receiver) = oneshot::channel();
        self.pending.inc();
        let request = ReadRequest {
            path: path.to_owned(),
            range,
            response,
            queued_at: Instant::now(),
            trace_id,
            _slot: slot,
            _pending: PendingRead(self.pending.clone()),
        };
        self.requests.try_send(request).ok()?;
        Some(receiver)
    }
}

async fn run<F, R>(
    mut receiver: mpsc::Receiver<ReadRequest>,
    read: F,
    started_at: Instant,
    config: ReadSchedulerConfig,
    metrics: ObjectStoreMetrics,
    media_type: &'static str,
) where
    F: Fn(String, Range<usize>) -> R + Sync,
    R: Future<Output = ReadResult> + Send,
{
    let group_size = metrics
        .read_scheduler_group_size
        .with_label_values(&[media_type]);
    let queue_duration = metrics
        .read_scheduler_queue_duration
        .with_label_values(&[media_type]);
    let max_batch = config
        .max_batch_requests
        .get()
        .min(config.max_admitted_requests.get());
    let mut ready = Vec::new();
    let mut in_flight = FuturesUnordered::new();
    let mut closed = false;
    let trace_hash = RandomState::new();
    loop {
        tokio::select! {
            count = receiver.recv_many(&mut ready, max_batch), if !closed => {
                closed = count == 0;
                let received = ready.len();
                ready.retain(|request| !request.response.is_closed());
                metrics.read_scheduler_events.with_label_values(&[media_type, "cancelled_queued"]).inc_by((received - ready.len()) as u64);
                if !ready.is_empty() { metrics.read_scheduler_batch_size.with_label_values(&[media_type]).observe(ready.len() as f64); }
                if tracing::enabled!(target: "state_store_read_batch", tracing::Level::TRACE) && !ready.is_empty() {
                    trace_read_batch(&ready, &trace_hash, started_at);
                }
                for group in plan_reads(&mut ready, config.max_read_bytes.get(), &metrics, media_type) {
                    in_flight.push(group.execute(&read, &group_size, &queue_duration, &metrics, media_type));
                }
            }
            _ = in_flight.next(), if !in_flight.is_empty() => {}
            else => return,
        }
    }
}

fn trace_read_batch(ready: &[ReadRequest], hash: &RandomState, started_at: Instant) {
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
    tracing::trace!(target: "state_store_read_batch",
        scheduler = hash.hash_one(started_at),
        ?request_ids,
        dequeued_us,
        ?requests,
        "read scheduler batch"
    );
}

fn plan_reads(
    ready: &mut Vec<ReadRequest>,
    max_bytes: usize,
    metrics: &ObjectStoreMetrics,
    media_type: &str,
) -> Vec<ReadGroup> {
    ready.sort_unstable_by(|a, b| (&a.path, a.range.start).cmp(&(&b.path, b.range.start)));
    let mut groups: Vec<ReadGroup> = Vec::new();
    for request in ready.drain(..) {
        if let Some(group) = groups.last_mut()
            && group.requests[0].path == request.path
            && request.range.start <= group.range.end
            && group.range.end.max(request.range.end) - group.range.start <= max_bytes
        {
            group.range.end = group.range.end.max(request.range.end);
            group.requests.push(request);
        } else {
            if let Some(previous) = groups.last() {
                let reason = if previous.requests[0].path != request.path {
                    "split_object"
                } else if request.range.start > previous.range.end {
                    metrics
                        .read_scheduler_gap_bytes
                        .with_label_values(&[media_type])
                        .observe((request.range.start - previous.range.end) as f64);
                    "split_gap"
                } else {
                    "split_bytes"
                };
                metrics
                    .read_scheduler_events
                    .with_label_values(&[media_type, reason])
                    .inc();
            }
            groups.push(ReadGroup {
                range: request.range.clone(),
                requests: vec![request],
            });
        }
    }
    groups
}

impl ReadGroup {
    async fn execute<F, R>(
        mut self,
        read: &F,
        group_size: &Histogram,
        queue_duration: &Histogram,
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
        group_size.observe(self.requests.len() as f64);
        for request in &self.requests {
            queue_duration.observe(request.queued_at.elapsed().as_secs_f64());
        }
        // Tokens identify actual outer reads. Build this list only for trace collection,
        // and emit dispatch inside the polled read future (not merely at dequeue).
        let trace_members =
            tracing::enabled!(target: "state_store_read_batch", tracing::Level::TRACE).then(|| {
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
            tracing::trace!(target: "state_store_read_batch",?group_id, request_ids = ?trace_members, start = span.start,
                end = span.end, "read scheduler group dispatch");
            let result = read(path, span.clone()).await;
            tracing::trace!(target: "state_store_read_batch",
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
            tracing::trace!(target: "state_store_read_batch",?group_id, "read scheduler group cancelled");
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
                    tracing::trace!(target: "state_store_read_batch",?group_id, request_id = ?trace_id,
                        "read scheduler range fallback dispatch");
                    let result = read(path, range).await;
                    tracing::trace!(target: "state_store_read_batch",?group_id, request_id = ?trace_id, ok = result.is_ok(),
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

    #[tokio::test]
    async fn read_coalesces_adjacent_ranges_on_the_same_object() {
        let metrics = Arc::new(ObjectStoreMetrics::new(&prometheus::Registry::new()));
        let config = ObjectStoreConfig {
            read_scheduler: ReadSchedulerConfig {
                max_batch_requests: 4.try_into().unwrap(),
                max_admitted_requests: 7.try_into().unwrap(),
                max_read_bytes: 8.try_into().unwrap(),
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
    async fn cancellation_after_planning_is_accounted_before_dispatch() {
        for cancel_all in [false, true] {
            let metrics = ObjectStoreMetrics::new(&prometheus::Registry::new());
            let pending = metrics.read_scheduler_pending.with_label_values(&["test"]);
            let slots = Arc::new(Semaphore::new(3));
            let mut requests = Vec::new();
            let mut responses = Vec::new();
            for start in [0, 4, 8] {
                let (response, receiver) = oneshot::channel();
                pending.inc();
                requests.push(ReadRequest {
                    path: "file".to_owned(),
                    range: start..start + 4,
                    response,
                    queued_at: Instant::now(),
                    trace_id: None,
                    _slot: slots.clone().try_acquire_owned().unwrap(),
                    _pending: PendingRead(pending.clone()),
                });
                responses.push(receiver);
            }
            let mut groups = plan_reads(&mut requests, 12, &metrics, "test");
            assert_eq!(groups.len(), 1);
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
            groups
                .pop()
                .unwrap()
                .execute(
                    &read,
                    &metrics
                        .read_scheduler_group_size
                        .with_label_values(&["test"]),
                    &metrics
                        .read_scheduler_queue_duration
                        .with_label_values(&["test"]),
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
        let first = scheduler.try_submit("file", 0..2, None).unwrap();
        let second = scheduler.try_submit("file", 2..4, None).unwrap();
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
            ReadSchedulerConfig::default(),
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
        let slow = scheduler.try_submit("file", 0..2, None).unwrap();
        let valid = scheduler.try_submit("file", 2..4, None).unwrap();
        let invalid = scheduler.try_submit("file", 4..6, None).unwrap();
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
        let first = scheduler.try_submit("file", 0..2, None).unwrap();
        let second = scheduler.try_submit("file", 2..4, None).unwrap();
        assert!(scheduler.try_submit("file", 4..6, None).is_none());
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
