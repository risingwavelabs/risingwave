// Copyright 2023 RisingWave Labs
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

use std::sync::OnceLock;

use prometheus::{
    exponential_buckets, histogram_opts, linear_buckets, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_vec_with_registry, Histogram, Registry,
};
use risingwave_common::config::MetricLevel;
use risingwave_common::metrics::{
    RelabeledCounterVec, RelabeledGuardedHistogramVec, RelabeledHistogramVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::register_guarded_histogram_vec_with_registry;

/// [`MonitoredStorageMetrics`] stores the performance and IO metrics of Storage.
#[derive(Debug, Clone)]
pub struct MonitoredStorageMetrics {
    pub get_duration: RelabeledGuardedHistogramVec<1>,
    pub get_key_size: RelabeledHistogramVec,
    pub get_value_size: RelabeledHistogramVec,

    pub iter_size: RelabeledHistogramVec,
    pub iter_item: RelabeledHistogramVec,
    pub iter_init_duration: RelabeledGuardedHistogramVec<1>,
    pub iter_scan_duration: RelabeledGuardedHistogramVec<1>,
    pub may_exist_duration: RelabeledHistogramVec,

    pub iter_in_process_counts: RelabeledCounterVec,

    pub sync_duration: Histogram,
    pub sync_size: Histogram,
}

pub static GLOBAL_STORAGE_METRICS: OnceLock<MonitoredStorageMetrics> = OnceLock::new();

pub fn global_storage_metrics(metric_level: MetricLevel) -> MonitoredStorageMetrics {
    GLOBAL_STORAGE_METRICS
        .get_or_init(|| MonitoredStorageMetrics::new(&GLOBAL_METRICS_REGISTRY, metric_level))
        .clone()
}

impl MonitoredStorageMetrics {
    pub fn new(registry: &Registry, metric_level: MetricLevel) -> Self {
        // 256B ~ max 4GB
        let size_buckets = exponential_buckets(256.0, 16.0, 7).unwrap();
        // 10ms ~ max 2.7h
        let time_buckets = exponential_buckets(0.01, 10.0, 7).unwrap();
        // ----- get -----
        let opts = histogram_opts!(
            "state_store_get_key_size",
            "Total key bytes of get that have been issued to state store",
            size_buckets.clone()
        );
        let get_key_size =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let get_key_size = RelabeledHistogramVec::with_metric_level(
            MetricLevel::Debug,
            get_key_size,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_get_value_size",
            "Total value bytes that have been requested from remote storage",
            size_buckets.clone()
        );
        let get_value_size =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let get_value_size = RelabeledHistogramVec::with_metric_level(
            MetricLevel::Debug,
            get_value_size,
            metric_level,
        );

        let mut buckets = exponential_buckets(0.000004, 2.0, 4).unwrap(); // 4 ~ 32us
        buckets.extend(linear_buckets(0.00006, 0.00004, 5).unwrap()); // 60 ~ 220us.
        buckets.extend(linear_buckets(0.0003, 0.0001, 3).unwrap()); // 300 ~ 500us.
        buckets.extend(exponential_buckets(0.001, 2.0, 5).unwrap()); // 1 ~ 16ms.
        buckets.extend(exponential_buckets(0.05, 4.0, 5).unwrap()); // 0.05 ~ 1.28s.
        buckets.push(16.0); // 16s

        // 1ms - 100s
        let mut state_store_read_time_buckets = exponential_buckets(0.001, 10.0, 5).unwrap();
        state_store_read_time_buckets.push(40.0);
        state_store_read_time_buckets.push(100.0);

        let get_duration_opts = histogram_opts!(
            "state_store_get_duration",
            "Total latency of get that have been issued to state store",
            state_store_read_time_buckets.clone(),
        );
        let get_duration = register_guarded_histogram_vec_with_registry!(
            get_duration_opts,
            &["table_id"],
            registry
        )
        .unwrap();
        let get_duration = RelabeledGuardedHistogramVec::with_metric_level(
            MetricLevel::Critical,
            get_duration,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_iter_size",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            size_buckets.clone()
        );
        let iter_size =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let iter_size =
            RelabeledHistogramVec::with_metric_level(MetricLevel::Debug, iter_size, metric_level);

        let opts = histogram_opts!(
            "state_store_iter_item",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            size_buckets
        );
        let iter_item =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let iter_item =
            RelabeledHistogramVec::with_metric_level(MetricLevel::Debug, iter_item, metric_level);

        let opts = histogram_opts!(
            "state_store_iter_init_duration",
            "Histogram of the time spent on iterator initialization.",
            state_store_read_time_buckets.clone(),
        );
        let iter_init_duration =
            register_guarded_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let iter_init_duration = RelabeledGuardedHistogramVec::with_metric_level(
            MetricLevel::Critical,
            iter_init_duration,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_iter_scan_duration",
            "Histogram of the time spent on iterator scanning.",
            state_store_read_time_buckets.clone(),
        );
        let iter_scan_duration =
            register_guarded_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let iter_scan_duration = RelabeledGuardedHistogramVec::with_metric_level(
            MetricLevel::Critical,
            iter_scan_duration,
            metric_level,
        );

        let iter_in_process_counts = register_int_counter_vec_with_registry!(
            "state_store_iter_in_process_counts",
            "Total number of iter_in_process that have been issued to state store",
            &["table_id"],
            registry
        )
        .unwrap();
        let iter_in_process_counts = RelabeledCounterVec::with_metric_level(
            MetricLevel::Debug,
            iter_in_process_counts,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_may_exist_duration",
            "Histogram of may exist time that have been issued to state store",
            buckets,
        );
        let may_exist_duration =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let may_exist_duration = RelabeledHistogramVec::with_metric_level(
            MetricLevel::Debug,
            may_exist_duration,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_sync_duration",
            "Histogram of time spent on compacting shared buffer to remote storage",
            time_buckets.clone()
        );
        let sync_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_sync_size",
            "Total size of upload to l0 every epoch",
            time_buckets
        );
        let sync_size = register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            get_duration,
            get_key_size,
            get_value_size,
            iter_size,
            iter_item,
            iter_init_duration,
            iter_scan_duration,
            may_exist_duration,
            iter_in_process_counts,
            sync_duration,
            sync_size,
        }
    }

    pub fn unused() -> Self {
        global_storage_metrics(MetricLevel::Disabled)
    }
}
