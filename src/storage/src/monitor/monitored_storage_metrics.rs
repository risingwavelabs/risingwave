// Copyright 2024 RisingWave Labs
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

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use prometheus::core::{AtomicU64, GenericLocalCounter};
use prometheus::local::LocalHistogram;
use prometheus::{
    exponential_buckets, histogram_opts, linear_buckets, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_vec_with_registry, Histogram, Registry,
};
use risingwave_common::config::MetricLevel;
use risingwave_common::metrics::{
    LabelGuardedMetric, RelabeledCounterVec, RelabeledGuardedHistogramVec, RelabeledHistogramVec,
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
            size_buckets.clone(),
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
            time_buckets,
        );
        let sync_duration = register_histogram_with_registry!(opts, registry).unwrap();

        let opts = histogram_opts!(
            "state_store_sync_size",
            "Total size of upload to l0 every epoch",
            size_buckets,
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

    pub fn local_metrics(&self, table_label: &str) -> LocalStorageMetrics {
        let iter_init_duration = self
            .iter_init_duration
            .with_label_values(&[table_label])
            .local();
        let iter_in_process_counts = self
            .iter_in_process_counts
            .with_label_values(&[table_label])
            .local();
        let iter_scan_duration = self
            .iter_scan_duration
            .with_label_values(&[table_label])
            .local();
        let iter_item = self.iter_item.with_label_values(&[table_label]).local();
        let iter_size = self.iter_size.with_label_values(&[table_label]).local();

        let get_duration = self.get_duration.with_label_values(&[table_label]).local();
        let get_key_size = self.get_key_size.with_label_values(&[table_label]).local();
        let get_value_size = self
            .get_value_size
            .with_label_values(&[table_label])
            .local();

        LocalStorageMetrics {
            iter_init_duration,
            iter_scan_duration,
            iter_in_process_counts,
            iter_item,
            iter_size,
            get_duration,
            get_key_size,
            get_value_size,
            report_count: 0,
        }
    }
}

pub struct LocalStorageMetrics {
    iter_init_duration: LabelGuardedMetric<LocalHistogram, 1>,
    iter_scan_duration: LabelGuardedMetric<LocalHistogram, 1>,
    iter_in_process_counts: GenericLocalCounter<AtomicU64>,
    iter_item: LocalHistogram,
    iter_size: LocalHistogram,

    get_duration: LabelGuardedMetric<LocalHistogram, 1>,
    get_key_size: LocalHistogram,
    get_value_size: LocalHistogram,
    report_count: usize,
}

impl LocalStorageMetrics {
    pub fn may_flush(&mut self) {
        self.report_count += 1;
        if self.report_count > MAX_FLUSH_TIMES {
            self.iter_scan_duration.flush();
            self.iter_init_duration.flush();
            self.iter_in_process_counts.flush();
            self.iter_item.flush();
            self.iter_size.flush();
            self.get_duration.flush();
            self.get_key_size.flush();
            self.get_value_size.flush();
            self.report_count = 0;
        }
    }
}

pub struct MonitoredStateStoreIterStats {
    pub iter_init_duration: Duration,
    pub iter_scan_time: Instant,
    pub total_items: usize,
    pub total_size: usize,
    pub table_id: u32,
    pub metrics: Arc<MonitoredStorageMetrics>,
}

thread_local!(static LOCAL_METRICS: RefCell<HashMap<u32, LocalStorageMetrics>> = RefCell::new(HashMap::default()));
const MAX_FLUSH_TIMES: usize = 64;

impl MonitoredStateStoreIterStats {
    pub fn new(
        table_id: u32,
        iter_init_duration: Duration,
        metrics: Arc<MonitoredStorageMetrics>,
    ) -> Self {
        Self {
            iter_init_duration,
            iter_scan_time: Instant::now(),
            total_items: 0,
            total_size: 0,
            table_id,
            metrics,
        }
    }

    pub fn report(&self) {
        LOCAL_METRICS.with_borrow_mut(|local_metrics| {
            let table_metrics = local_metrics.entry(self.table_id).or_insert_with(|| {
                let table_label = self.table_id.to_string();
                self.metrics.local_metrics(&table_label)
            });
            let iter_scan_duration = self.iter_scan_time.elapsed();
            table_metrics
                .iter_scan_duration
                .observe(iter_scan_duration.as_secs_f64());
            table_metrics
                .iter_init_duration
                .observe(self.iter_init_duration.as_secs_f64());
            table_metrics.iter_in_process_counts.inc();
            table_metrics.iter_item.observe(self.total_items as f64);
            table_metrics.iter_size.observe(self.total_size as f64);
            table_metrics.may_flush();
        });
    }
}

impl Drop for MonitoredStateStoreIterStats {
    fn drop(&mut self) {
        self.report();
    }
}

pub struct MonitoredStateStoreGetStats {
    pub get_duration: Instant,
    pub get_key_size: usize,
    pub get_value_size: usize,
    pub table_id: u32,
    pub metrics: Arc<MonitoredStorageMetrics>,
}

impl MonitoredStateStoreGetStats {
    pub fn new(table_id: u32, metrics: Arc<MonitoredStorageMetrics>) -> Self {
        Self {
            get_duration: Instant::now(),
            get_key_size: 0,
            get_value_size: 0,
            table_id,
            metrics,
        }
    }

    pub fn report(&self) {
        LOCAL_METRICS.with_borrow_mut(|local_metrics| {
            let table_metrics = local_metrics.entry(self.table_id).or_insert_with(|| {
                let table_label = self.table_id.to_string();
                self.metrics.local_metrics(&table_label)
            });
            let get_duration = self.get_duration.elapsed();
            table_metrics
                .get_duration
                .observe(get_duration.as_secs_f64());
            table_metrics.get_key_size.observe(self.get_key_size as _);
            if self.get_value_size > 0 {
                table_metrics
                    .get_value_size
                    .observe(self.get_value_size as _);
            }
            table_metrics.may_flush();
        });
    }
}
