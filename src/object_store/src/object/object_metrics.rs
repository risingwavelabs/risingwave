// Copyright 2022 RisingWave Labs
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

use std::sync::LazyLock;

use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    HistogramVec, IntCounterVec, IntGaugeVec, Registry, exponential_buckets, histogram_opts,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, register_int_gauge_vec_with_registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

pub static GLOBAL_OBJECT_STORE_METRICS: LazyLock<ObjectStoreMetrics> =
    LazyLock::new(|| ObjectStoreMetrics::new(&GLOBAL_METRICS_REGISTRY));

#[derive(Clone)]
pub struct ObjectStoreMetrics {
    pub write_bytes: GenericCounter<AtomicU64>,
    pub read_bytes: GenericCounter<AtomicU64>,
    pub operation_latency: HistogramVec,
    pub operation_size: HistogramVec,
    pub failure_count: GenericCounterVec<AtomicU64>,
    pub request_retry_count: GenericCounterVec<AtomicU64>,
    pub(super) read_outcomes: IntCounterVec,
    pub(super) read_in_flight: IntGaugeVec,
    pub(super) read_scheduler_events: IntCounterVec,
    pub(super) read_scheduler_pending: IntGaugeVec,
    pub(super) read_scheduler_batch_size: HistogramVec,
    pub(super) read_requests: GenericCounterVec<AtomicU64>,
    pub(super) read_request_duration: HistogramVec,
    pub(super) read_request_bytes: HistogramVec,
    pub(super) read_scheduler_gap_bytes: HistogramVec,
    pub(super) read_scheduler_group_size: HistogramVec,
    pub(super) read_scheduler_queue_duration: HistogramVec,
}

impl ObjectStoreMetrics {
    pub(super) fn new(registry: &Registry) -> Self {
        let read_outcomes = register_int_counter_vec_with_registry!(
            "object_store_read_outcomes", "Read completions by layer; attempts are backend invocations, not SDK-internal HTTP retries", &["media_type", "stage", "outcome"], registry).unwrap();
        let read_in_flight = register_int_gauge_vec_with_registry!(
            "object_store_read_in_flight",
            "Currently executing reads by layer",
            &["media_type", "stage"],
            registry
        )
        .unwrap();
        let read_scheduler_events = register_int_counter_vec_with_registry!(
            "object_store_read_scheduler_events",
            "Scheduler planning, completion and fallback events",
            &["media_type", "event"],
            registry
        )
        .unwrap();
        let read_scheduler_pending = register_int_gauge_vec_with_registry!(
            "object_store_read_scheduler_pending",
            "Admitted logical reads, queued or in flight",
            &["media_type"],
            registry
        )
        .unwrap();
        let read_scheduler_batch_size = register_histogram_vec_with_registry!(
            histogram_opts!(
                "object_store_read_scheduler_batch_size",
                "Live requests available together in a scheduler dequeue",
                exponential_buckets(1.0, 2.0, 16).unwrap()
            ),
            &["media_type"],
            registry
        )
        .unwrap();
        let read_bytes = register_int_counter_with_registry!(
            "object_store_read_bytes",
            "Total bytes of requests read from object store",
            registry
        )
        .unwrap();
        let write_bytes = register_int_counter_with_registry!(
            "object_store_write_bytes",
            "Total bytes of requests read from object store",
            registry
        )
        .unwrap();

        let latency_opts = histogram_opts!(
            "object_store_operation_latency",
            "Total latency of operation on object store",
            exponential_buckets(0.001, 2.0, 22).unwrap(), // max 209s
        );
        let operation_latency =
            register_histogram_vec_with_registry!(latency_opts, &["media_type", "type"], registry)
                .unwrap();
        let mut buckets = vec![];
        for i in 0..4 {
            buckets.push((4096 << (i * 2)) as f64);
        }
        for i in 0..4 {
            buckets.push((4096 << (i + 10)) as f64);
        }
        let mut step = *buckets.last().unwrap(); // 32MB
        for _ in 0..4 {
            let base = *buckets.last().unwrap() + step;
            for i in 0..4 {
                buckets.push(base + step * i as f64);
            }
            step *= 2.0;
        }
        let bytes_opts = histogram_opts!(
            "object_store_operation_bytes",
            "Size of operation result on object store",
            buckets, // max 1952MB
        );
        let operation_size =
            register_histogram_vec_with_registry!(bytes_opts, &["type"], registry).unwrap();

        let failure_count = register_int_counter_vec_with_registry!(
            "object_store_failure_count",
            "The number of failures of object store operations",
            &["type"],
            registry
        )
        .unwrap();

        let request_retry_count = register_int_counter_vec_with_registry!(
            "object_store_request_retry_count",
            "The number of retry times of object store request",
            &["type"],
            registry
        )
        .unwrap();
        let read_request_bytes = register_histogram_vec_with_registry!(
            histogram_opts!(
                "object_store_read_request_bytes",
                "Requested bytes of bounded logical reads, before coalescing",
                exponential_buckets(1024.0, 2.0, 22).unwrap()
            ),
            &["media_type", "route"],
            registry
        )
        .unwrap();
        let read_scheduler_gap_bytes = register_histogram_vec_with_registry!(
            histogram_opts!("object_store_read_scheduler_gap_bytes", "Positive gaps separating same-object ranges within a ready batch; gaps are not read", exponential_buckets(1.0, 2.0, 30).unwrap()), &["media_type"], registry).unwrap();
        let read_scheduler_group_size = register_histogram_vec_with_registry!(
            histogram_opts!(
                "object_store_read_scheduler_group_size",
                "Number of logical range reads in each scheduled physical read",
                exponential_buckets(1.0, 2.0, 16).unwrap()
            ),
            &["media_type"],
            registry
        )
        .unwrap();
        let read_requests = register_int_counter_vec_with_registry!(
            "object_store_read_requests",
            "Logical object store read calls by dispatch route",
            &["media_type", "route"],
            registry
        )
        .unwrap();
        let read_request_duration = register_histogram_vec_with_registry!(
            histogram_opts!(
                "object_store_read_request_duration_seconds",
                "Time to complete a logical object store read, including scheduling and retries",
                exponential_buckets(0.000_01, 1.5, 44).unwrap()
            ),
            &["media_type", "route"],
            registry
        )
        .unwrap();
        let read_scheduler_queue_duration = register_histogram_vec_with_registry!(
            histogram_opts!(
                "object_store_read_scheduler_queue_duration_seconds",
                "Time from admitting a range read to dispatching it",
                exponential_buckets(0.000_01, 2.0, 18).unwrap()
            ),
            &["media_type"],
            registry
        )
        .unwrap();

        Self {
            write_bytes,
            read_bytes,
            operation_latency,
            operation_size,
            failure_count,
            request_retry_count,
            read_outcomes,
            read_in_flight,
            read_scheduler_events,
            read_scheduler_pending,
            read_scheduler_batch_size,
            read_requests,
            read_request_duration,
            read_request_bytes,
            read_scheduler_gap_bytes,
            read_scheduler_group_size,
            read_scheduler_queue_duration,
        }
    }

    /// Creates a new `HummockStateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        GLOBAL_OBJECT_STORE_METRICS.clone()
    }
}

/// Account for dropped futures as cancellation, including during retries or scheduler waits.
/// The guard is created only when an object read is polled; cache hits do not pay for it.
pub(super) struct ReadGuard {
    outcomes: IntCounterVec,
    in_flight: prometheus::IntGauge,
    media_type: &'static str,
    stage: &'static str,
    outcome: &'static str,
}

impl ObjectStoreMetrics {
    pub(super) fn begin_read(&self, media_type: &'static str, stage: &'static str) -> ReadGuard {
        let in_flight = self.read_in_flight.with_label_values(&[media_type, stage]);
        in_flight.inc();
        ReadGuard {
            outcomes: self.read_outcomes.clone(),
            in_flight,
            media_type,
            stage,
            outcome: "cancelled",
        }
    }
}
impl ReadGuard {
    pub(super) fn finish<T, E>(&mut self, result: &Result<T, E>) {
        self.outcome = if result.is_ok() { "ok" } else { "error" };
    }
}
impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.in_flight.dec();
        self.outcomes
            .with_label_values(&[self.media_type, self.stage, self.outcome])
            .inc();
    }
}
