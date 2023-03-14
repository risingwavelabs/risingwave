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

use prometheus::core::{AtomicU64, GenericCounterVec};
use prometheus::{
    exponential_buckets, histogram_opts, linear_buckets, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_vec_with_registry, Histogram,
    HistogramVec, Registry,
};

/// [`MonitoredStorageMetrics`] stores the performance and IO metrics of Storage.
#[derive(Debug)]
pub struct MonitoredStorageMetrics {
    pub get_duration: HistogramVec,
    pub get_key_size: HistogramVec,
    pub get_value_size: HistogramVec,

    pub iter_size: HistogramVec,
    pub iter_item: HistogramVec,
    pub iter_duration: HistogramVec,
    pub iter_scan_duration: HistogramVec,
    pub may_exist_duration: HistogramVec,

    pub iter_in_process_counts: GenericCounterVec<AtomicU64>,

    pub sync_duration: Histogram,
    pub sync_size: Histogram,
}

impl MonitoredStorageMetrics {
    pub fn new(registry: Registry) -> Self {
        // ----- get -----
        let opts = histogram_opts!(
            "state_store_get_key_size",
            "Total key bytes of get that have been issued to state store",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );
        let get_key_size =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let opts = histogram_opts!(
            "state_store_get_value_size",
            "Total value bytes that have been requested from remote storage",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );
        let get_value_size =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let mut buckets = exponential_buckets(0.000004, 2.0, 4).unwrap(); // 4 ~ 32us
        buckets.extend(linear_buckets(0.00006, 0.00004, 5).unwrap()); // 60 ~ 220us.
        buckets.extend(linear_buckets(0.0003, 0.0001, 3).unwrap()); // 300 ~ 500us.
        buckets.extend(exponential_buckets(0.001, 2.0, 5).unwrap()); // 1 ~ 16ms.
        buckets.extend(exponential_buckets(0.05, 4.0, 5).unwrap()); // 0.05 ~ 1.28s.
        buckets.push(16.0); // 16s
        let get_duration_opts = histogram_opts!(
            "state_store_get_duration",
            "Total latency of get that have been issued to state store",
            buckets.clone(),
        );
        let get_duration =
            register_histogram_vec_with_registry!(get_duration_opts, &["table_id"], registry)
                .unwrap();

        let opts = histogram_opts!(
            "state_store_iter_size",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            exponential_buckets(1.0, 2.0, 25).unwrap() // max 16MB
        );
        let iter_size =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let opts = histogram_opts!(
            "state_store_iter_item",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            exponential_buckets(1.0, 2.0, 20).unwrap() // max 2^20 items
        );
        let iter_item =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let opts = histogram_opts!(
            "state_store_iter_duration",
            "Histogram of iterator scan and initialization time that have been issued to state store",
            buckets.clone(),
        );
        let iter_duration =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let opts = histogram_opts!(
            "state_store_iter_scan_duration",
            "Histogram of iterator scan time that have been issued to state store",
            buckets.clone(),
        );
        let iter_scan_duration =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let iter_in_process_counts = register_int_counter_vec_with_registry!(
            "state_store_iter_in_process_counts",
            "Total number of iter_in_process that have been issued to state store",
            &["table_id"],
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "state_store_may_exist_duration",
            "Histogram of may exist time that have been issued to state store",
            buckets,
        );
        let may_exist_duration =
            register_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();

        let opts = histogram_opts!(
            "state_store_sync_duration",
            "Histogram of time spent from compacting shared buffer to remote storage",
            exponential_buckets(0.01, 2.0, 16).unwrap() // max 327s
        );
        let sync_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_sync_size",
            "Total size of upload to l0 every epoch",
            exponential_buckets(10.0, 2.0, 25).unwrap()
        );
        let sync_size = register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            get_duration,
            get_key_size,
            get_value_size,
            iter_size,
            iter_item,
            iter_duration,
            iter_scan_duration,
            may_exist_duration,
            iter_in_process_counts,
            sync_duration,
            sync_size,
        }
    }

    /// Creates a new `HummockStateStoreMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(Registry::new())
    }
}
