// Copyright 2025 RisingWave Labs
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

use prometheus::{
    exponential_buckets, histogram_opts, linear_buckets, register_histogram_with_registry,
    Histogram, Registry,
};
use risingwave_common::config::MetricLevel;
use risingwave_common::metrics::{
    LabelGuardedIntCounterVec, LabelGuardedIntGauge, LabelGuardedLocalHistogram,
    LabelGuardedLocalIntCounter, RelabeledGuardedHistogramVec, RelabeledGuardedIntCounterVec,
    RelabeledGuardedIntGaugeVec,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::{
    register_guarded_histogram_vec_with_registry, register_guarded_int_counter_vec_with_registry,
    register_guarded_int_gauge_vec_with_registry,
};

use crate::store::{
    ChangeLogValue, IterItem, StateStoreKeyedRow, StateStoreKeyedRowRef, StateStoreReadLogItem,
    StateStoreReadLogItemRef,
};

/// [`MonitoredStorageMetrics`] stores the performance and IO metrics of Storage.
#[derive(Debug, Clone)]
pub struct MonitoredStorageMetrics {
    pub get_duration: RelabeledGuardedHistogramVec<1>,
    pub get_key_size: RelabeledGuardedHistogramVec<1>,
    pub get_value_size: RelabeledGuardedHistogramVec<1>,

    // [table_id, iter_type: {"iter", "iter_log"}]
    pub iter_size: RelabeledGuardedHistogramVec<2>,
    pub iter_item: RelabeledGuardedHistogramVec<2>,
    pub iter_init_duration: RelabeledGuardedHistogramVec<2>,
    pub iter_scan_duration: RelabeledGuardedHistogramVec<2>,
    pub iter_counts: RelabeledGuardedIntCounterVec<2>,
    pub iter_in_progress_counts: RelabeledGuardedIntGaugeVec<2>,

    // [table_id, op_type]
    pub iter_log_op_type_counts: LabelGuardedIntCounterVec<2>,

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
        // 256B ~ max 64GB
        let size_buckets = exponential_buckets(256.0, 16.0, 8).unwrap();
        // 10ms ~ max 2.7h
        let time_buckets = exponential_buckets(0.01, 10.0, 7).unwrap();
        // ----- get -----
        let opts = histogram_opts!(
            "state_store_get_key_size",
            "Total key bytes of get that have been issued to state store",
            size_buckets.clone()
        );
        let get_key_size =
            register_guarded_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let get_key_size = RelabeledGuardedHistogramVec::with_metric_level(
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
            register_guarded_histogram_vec_with_registry!(opts, &["table_id"], registry).unwrap();
        let get_value_size = RelabeledGuardedHistogramVec::with_metric_level(
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
            MetricLevel::Info,
            get_duration,
            metric_level,
        );

        let opts = histogram_opts!(
            "state_store_iter_size",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            size_buckets.clone()
        );
        let iter_size = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "iter_type"],
            registry
        )
        .unwrap();
        let iter_size = RelabeledGuardedHistogramVec::with_metric_level_relabel_n(
            MetricLevel::Debug,
            iter_size,
            metric_level,
            1,
        );

        let opts = histogram_opts!(
            "state_store_iter_item",
            "Total bytes gotten from state store scan(), for calculating read throughput",
            size_buckets.clone(),
        );
        let iter_item = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "iter_type"],
            registry
        )
        .unwrap();
        let iter_item = RelabeledGuardedHistogramVec::with_metric_level_relabel_n(
            MetricLevel::Debug,
            iter_item,
            metric_level,
            1,
        );

        let opts = histogram_opts!(
            "state_store_iter_init_duration",
            "Histogram of the time spent on iterator initialization.",
            state_store_read_time_buckets.clone(),
        );
        let iter_init_duration = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "iter_type"],
            registry
        )
        .unwrap();
        let iter_init_duration = RelabeledGuardedHistogramVec::with_metric_level_relabel_n(
            MetricLevel::Info,
            iter_init_duration,
            metric_level,
            1,
        );

        let opts = histogram_opts!(
            "state_store_iter_scan_duration",
            "Histogram of the time spent on iterator scanning.",
            state_store_read_time_buckets.clone(),
        );
        let iter_scan_duration = register_guarded_histogram_vec_with_registry!(
            opts,
            &["table_id", "iter_type"],
            registry
        )
        .unwrap();
        let iter_scan_duration = RelabeledGuardedHistogramVec::with_metric_level_relabel_n(
            MetricLevel::Info,
            iter_scan_duration,
            metric_level,
            1,
        );

        let iter_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_iter_counts",
            "Total number of iter that have been issued to state store",
            &["table_id", "iter_type"],
            registry
        )
        .unwrap();
        let iter_counts = RelabeledGuardedIntCounterVec::with_metric_level_relabel_n(
            MetricLevel::Debug,
            iter_counts,
            metric_level,
            1,
        );

        let iter_in_progress_counts = register_guarded_int_gauge_vec_with_registry!(
            "state_store_iter_in_progress_counts",
            "Total number of existing iter",
            &["table_id", "iter_type"],
            registry
        )
        .unwrap();
        let iter_in_progress_counts = RelabeledGuardedIntGaugeVec::with_metric_level_relabel_n(
            MetricLevel::Debug,
            iter_in_progress_counts,
            metric_level,
            1,
        );

        let iter_log_op_type_counts = register_guarded_int_counter_vec_with_registry!(
            "state_store_iter_log_op_type_counts",
            "Counter of each op type in iter_log",
            &["table_id", "op_type"],
            registry
        )
        .unwrap();

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
            iter_counts,
            iter_in_progress_counts,
            iter_log_op_type_counts,
            sync_duration,
            sync_size,
        }
    }

    pub fn unused() -> Self {
        global_storage_metrics(MetricLevel::Disabled)
    }

    fn local_iter_metrics(&self, table_label: &str) -> LocalIterMetrics {
        let inner = self.new_local_iter_metrics_inner(table_label, "iter");
        LocalIterMetrics {
            inner,
            report_count: 0,
        }
    }

    fn new_local_iter_metrics_inner(
        &self,
        table_label: &str,
        iter_type: &str,
    ) -> LocalIterMetricsInner {
        let iter_init_duration = self
            .iter_init_duration
            .with_guarded_label_values(&[table_label, iter_type])
            .local();
        let iter_counts = self
            .iter_counts
            .with_guarded_label_values(&[table_label, iter_type])
            .local();
        let iter_scan_duration = self
            .iter_scan_duration
            .with_guarded_label_values(&[table_label, iter_type])
            .local();
        let iter_item = self
            .iter_item
            .with_guarded_label_values(&[table_label, iter_type])
            .local();
        let iter_size = self
            .iter_size
            .with_guarded_label_values(&[table_label, iter_type])
            .local();
        let iter_in_progress_counts = self
            .iter_in_progress_counts
            .with_guarded_label_values(&[table_label, iter_type]);

        LocalIterMetricsInner {
            iter_init_duration,
            iter_scan_duration,
            iter_counts,
            iter_item,
            iter_size,
            iter_in_progress_counts,
        }
    }

    fn local_iter_log_metrics(&self, table_label: &str) -> LocalIterLogMetrics {
        let iter_metrics = self.new_local_iter_metrics_inner(table_label, "iter_log");
        let insert_count = self
            .iter_log_op_type_counts
            .with_guarded_label_values(&[table_label, "INSERT"])
            .local();
        let update_count = self
            .iter_log_op_type_counts
            .with_guarded_label_values(&[table_label, "UPDATE"])
            .local();
        let delete_count = self
            .iter_log_op_type_counts
            .with_guarded_label_values(&[table_label, "DELETE"])
            .local();
        LocalIterLogMetrics {
            iter_metrics,
            insert_count,
            update_count,
            delete_count,
            report_count: 0,
        }
    }

    fn local_get_metrics(&self, table_label: &str) -> LocalGetMetrics {
        let get_duration = self
            .get_duration
            .with_guarded_label_values(&[table_label])
            .local();
        let get_key_size = self
            .get_key_size
            .with_guarded_label_values(&[table_label])
            .local();
        let get_value_size = self
            .get_value_size
            .with_guarded_label_values(&[table_label])
            .local();

        LocalGetMetrics {
            get_duration,
            get_key_size,
            get_value_size,
            report_count: 0,
        }
    }
}

struct LocalIterMetricsInner {
    iter_init_duration: LabelGuardedLocalHistogram<2>,
    iter_scan_duration: LabelGuardedLocalHistogram<2>,
    iter_counts: LabelGuardedLocalIntCounter<2>,
    iter_item: LabelGuardedLocalHistogram<2>,
    iter_size: LabelGuardedLocalHistogram<2>,
    iter_in_progress_counts: LabelGuardedIntGauge<2>,
}

struct LocalIterMetrics {
    inner: LocalIterMetricsInner,
    report_count: usize,
}

impl LocalIterMetrics {
    fn may_flush(&mut self) {
        self.report_count += 1;
        if self.report_count > MAX_FLUSH_TIMES {
            self.inner.flush();
            self.report_count = 0;
        }
    }
}

impl LocalIterMetricsInner {
    fn flush(&mut self) {
        self.iter_scan_duration.flush();
        self.iter_init_duration.flush();
        self.iter_counts.flush();
        self.iter_item.flush();
        self.iter_size.flush();
    }
}

struct LocalGetMetrics {
    get_duration: LabelGuardedLocalHistogram<1>,
    get_key_size: LabelGuardedLocalHistogram<1>,
    get_value_size: LabelGuardedLocalHistogram<1>,
    report_count: usize,
}

impl LocalGetMetrics {
    fn may_flush(&mut self) {
        self.report_count += 1;
        if self.report_count > MAX_FLUSH_TIMES {
            self.get_duration.flush();
            self.get_key_size.flush();
            self.get_value_size.flush();
            self.report_count = 0;
        }
    }
}

struct LocalIterLogMetrics {
    iter_metrics: LocalIterMetricsInner,
    insert_count: LabelGuardedLocalIntCounter<2>,
    update_count: LabelGuardedLocalIntCounter<2>,
    delete_count: LabelGuardedLocalIntCounter<2>,
    report_count: usize,
}

impl LocalIterLogMetrics {
    fn may_flush(&mut self) {
        self.report_count += 1;
        if self.report_count > MAX_FLUSH_TIMES {
            self.iter_metrics.flush();
            self.insert_count.flush();
            self.update_count.flush();
            self.delete_count.flush();
            self.report_count = 0;
        }
    }
}

pub(crate) trait StateStoreIterStatsTrait: Send {
    type Item: IterItem;
    fn new(table_id: u32, metrics: &MonitoredStorageMetrics, iter_init_duration: Duration) -> Self;
    fn observe(&mut self, item: <Self::Item as IterItem>::ItemRef<'_>);
    fn report(&mut self, table_id: u32, metrics: &MonitoredStorageMetrics);
}

const MAX_FLUSH_TIMES: usize = 64;

struct StateStoreIterStatsInner {
    pub iter_init_duration: Duration,
    pub iter_scan_time: Instant,
    pub total_items: usize,
    pub total_size: usize,
}

impl StateStoreIterStatsInner {
    fn new(iter_init_duration: Duration) -> Self {
        Self {
            iter_init_duration,
            iter_scan_time: Instant::now(),
            total_items: 0,
            total_size: 0,
        }
    }
}

pub(crate) struct MonitoredStateStoreIterStats<S: StateStoreIterStatsTrait> {
    pub inner: S,
    pub table_id: u32,
    pub metrics: Arc<MonitoredStorageMetrics>,
}

impl<S: StateStoreIterStatsTrait> Drop for MonitoredStateStoreIterStats<S> {
    fn drop(&mut self) {
        self.inner.report(self.table_id, &self.metrics)
    }
}

pub(crate) struct StateStoreIterStats {
    inner: StateStoreIterStatsInner,
}

impl StateStoreIterStats {
    fn for_table_metrics(
        table_id: u32,
        global_metrics: &MonitoredStorageMetrics,
        f: impl FnOnce(&mut LocalIterMetrics),
    ) {
        thread_local!(static LOCAL_ITER_METRICS: RefCell<HashMap<u32, LocalIterMetrics>> = RefCell::new(HashMap::default()));
        LOCAL_ITER_METRICS.with_borrow_mut(|local_metrics| {
            let table_metrics = local_metrics.entry(table_id).or_insert_with(|| {
                let table_label = table_id.to_string();
                global_metrics.local_iter_metrics(&table_label)
            });
            f(table_metrics)
        });
    }
}

impl StateStoreIterStatsTrait for StateStoreIterStats {
    type Item = StateStoreKeyedRow;

    fn new(table_id: u32, metrics: &MonitoredStorageMetrics, iter_init_duration: Duration) -> Self {
        Self::for_table_metrics(table_id, metrics, |metrics| {
            metrics.inner.iter_in_progress_counts.inc();
        });
        Self {
            inner: StateStoreIterStatsInner::new(iter_init_duration),
        }
    }

    fn observe(&mut self, (key, value): StateStoreKeyedRowRef<'_>) {
        self.inner.total_items += 1;
        self.inner.total_size += key.encoded_len() + value.len();
    }

    fn report(&mut self, table_id: u32, metrics: &MonitoredStorageMetrics) {
        Self::for_table_metrics(table_id, metrics, |table_metrics| {
            self.inner.apply_to_local(&mut table_metrics.inner);
            table_metrics.may_flush();
        });
    }
}

impl StateStoreIterStatsInner {
    fn apply_to_local(&self, table_metrics: &mut LocalIterMetricsInner) {
        {
            let iter_scan_duration = self.iter_scan_time.elapsed();
            table_metrics
                .iter_scan_duration
                .observe(iter_scan_duration.as_secs_f64());
            table_metrics
                .iter_init_duration
                .observe(self.iter_init_duration.as_secs_f64());
            table_metrics.iter_counts.inc();
            table_metrics.iter_item.observe(self.total_items as f64);
            table_metrics.iter_size.observe(self.total_size as f64);
            table_metrics.iter_in_progress_counts.dec();
        }
    }
}

pub(crate) struct StateStoreIterLogStats {
    inner: StateStoreIterStatsInner,
    insert_count: u64,
    update_count: u64,
    delete_count: u64,
}

impl StateStoreIterLogStats {
    fn for_table_metrics(
        table_id: u32,
        global_metrics: &MonitoredStorageMetrics,
        f: impl FnOnce(&mut LocalIterLogMetrics),
    ) {
        thread_local!(static LOCAL_ITER_LOG_METRICS: RefCell<HashMap<u32, LocalIterLogMetrics>> = RefCell::new(HashMap::default()));
        LOCAL_ITER_LOG_METRICS.with_borrow_mut(|local_metrics| {
            let table_metrics = local_metrics.entry(table_id).or_insert_with(|| {
                let table_label = table_id.to_string();
                global_metrics.local_iter_log_metrics(&table_label)
            });
            f(table_metrics)
        });
    }
}

impl StateStoreIterStatsTrait for StateStoreIterLogStats {
    type Item = StateStoreReadLogItem;

    fn new(table_id: u32, metrics: &MonitoredStorageMetrics, iter_init_duration: Duration) -> Self {
        Self::for_table_metrics(table_id, metrics, |metrics| {
            metrics.iter_metrics.iter_in_progress_counts.inc();
        });
        Self {
            inner: StateStoreIterStatsInner::new(iter_init_duration),
            insert_count: 0,
            update_count: 0,
            delete_count: 0,
        }
    }

    fn observe(&mut self, (key, change_log): StateStoreReadLogItemRef<'_>) {
        self.inner.total_items += 1;
        let value_len = match change_log {
            ChangeLogValue::Insert(value) => {
                self.insert_count += 1;
                value.len()
            }
            ChangeLogValue::Update {
                old_value,
                new_value,
            } => {
                self.update_count += 1;
                old_value.len() + new_value.len()
            }
            ChangeLogValue::Delete(value) => {
                self.delete_count += 1;
                value.len()
            }
        };
        self.inner.total_size += key.len() + value_len;
    }

    fn report(&mut self, table_id: u32, metrics: &MonitoredStorageMetrics) {
        Self::for_table_metrics(table_id, metrics, |table_metrics| {
            self.inner.apply_to_local(&mut table_metrics.iter_metrics);
            table_metrics.insert_count.inc_by(self.insert_count);
            table_metrics.update_count.inc_by(self.update_count);
            table_metrics.delete_count.inc_by(self.delete_count);
            table_metrics.may_flush();
        });
    }
}

pub(crate) struct MonitoredStateStoreGetStats {
    pub get_duration: Instant,
    pub get_key_size: usize,
    pub get_value_size: usize,
    pub table_id: u32,
    pub metrics: Arc<MonitoredStorageMetrics>,
}

impl MonitoredStateStoreGetStats {
    pub(crate) fn new(table_id: u32, metrics: Arc<MonitoredStorageMetrics>) -> Self {
        Self {
            get_duration: Instant::now(),
            get_key_size: 0,
            get_value_size: 0,
            table_id,
            metrics,
        }
    }

    pub(crate) fn report(&self) {
        thread_local!(static LOCAL_GET_METRICS: RefCell<HashMap<u32, LocalGetMetrics>> = RefCell::new(HashMap::default()));
        LOCAL_GET_METRICS.with_borrow_mut(|local_metrics| {
            let table_metrics = local_metrics.entry(self.table_id).or_insert_with(|| {
                let table_label = self.table_id.to_string();
                self.metrics.local_get_metrics(&table_label)
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
