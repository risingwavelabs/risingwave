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

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use parking_lot::Mutex;
use prometheus::core::{
    AtomicF64, AtomicU64, Collector, Desc, GenericCounterVec, GenericGaugeVec, MetricVec,
    MetricVecBuilder,
};
use prometheus::{
    exponential_buckets, opts, proto, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec,
    IntGauge, Registry,
};

use crate::task::TaskId;

macro_rules! for_all_task_metrics {
    ($macro:ident) => {
        $macro! {
            { task_first_poll_delay, GenericGaugeVec<AtomicF64> },
            { task_fast_poll_duration, GenericGaugeVec<AtomicF64> },
            { task_idle_duration, GenericGaugeVec<AtomicF64> },
            { task_poll_duration, GenericGaugeVec<AtomicF64> },
            { task_scheduled_duration, GenericGaugeVec<AtomicF64> },
            { task_slow_poll_duration, GenericGaugeVec<AtomicF64> },
            { task_exchange_recv_row_number, GenericCounterVec<AtomicU64> },
            { task_row_seq_scan_next_duration, HistogramVec },
        }
    };
}

macro_rules! def_task_metrics {
    ($( { $metric:ident, $type:ty }, )*) => {
        #[derive(Clone)]
        pub struct BatchTaskMetrics {
            descs: Vec<Desc>,
            $( pub $metric: $type, )*
            /// Labels to remove after each `collect`.
            labels_to_remove: Arc<Mutex<HashMap<TaskId, Vec<Box<dyn Fn() + Send>>>>>,
            finished_tasks: Arc<Mutex<Vec<TaskId>>>,
        }
    };
}

for_all_task_metrics!(def_task_metrics);

impl BatchTaskMetrics {
    /// The created [`BatchTaskMetrics`] is already registered to the `registry`.
    pub fn new(registry: Registry) -> Self {
        let task_labels = vec!["query_id", "stage_id", "task_id"];
        let mut descs = Vec::with_capacity(8);

        let task_first_poll_delay = GaugeVec::new(opts!(
            "batch_task_first_poll_delay",
            "The total duration (s) elapsed between the instant tasks are instrumented, and the instant they are first polled.",
        ), &task_labels[..]).unwrap();
        descs.extend(task_first_poll_delay.desc().into_iter().cloned());

        let task_fast_poll_duration = GaugeVec::new(
            opts!(
                "batch_task_fast_poll_duration",
                "The total duration (s) of fast polls.",
            ),
            &task_labels[..],
        )
        .unwrap();
        descs.extend(task_fast_poll_duration.desc().into_iter().cloned());

        let task_idle_duration = GaugeVec::new(
            opts!(
                "batch_task_idle_duration",
                "The total duration (s) that tasks idled.",
            ),
            &task_labels[..],
        )
        .unwrap();
        descs.extend(task_idle_duration.desc().into_iter().cloned());

        let task_poll_duration = GaugeVec::new(
            opts!(
                "batch_task_poll_duration",
                "The total duration (s) elapsed during polls.",
            ),
            &task_labels[..],
        )
        .unwrap();
        descs.extend(task_poll_duration.desc().into_iter().cloned());

        let task_scheduled_duration = GaugeVec::new(
            opts!(
                "batch_task_scheduled_duration",
                "The total duration (s) that tasks spent waiting to be polled after awakening.",
            ),
            &task_labels[..],
        )
        .unwrap();
        descs.extend(task_scheduled_duration.desc().into_iter().cloned());

        let task_slow_poll_duration = GaugeVec::new(
            opts!(
                "batch_task_slow_poll_duration",
                "The total duration (s) of slow polls.",
            ),
            &task_labels[..],
        )
        .unwrap();
        descs.extend(task_slow_poll_duration.desc().into_iter().cloned());

        let mut custom_labels = task_labels.clone();
        custom_labels.extend_from_slice(&[
            "executor_id",
            "source_query_id",
            "source_stage_id",
            "source_task_id",
        ]);
        let task_exchange_recv_row_number = IntCounterVec::new(
            opts!(
                "batch_task_exchange_recv_row_number",
                "Total number of row that have been received from upstream source",
            ),
            &custom_labels,
        )
        .unwrap();
        descs.extend(task_exchange_recv_row_number.desc().into_iter().cloned());

        let mut custom_labels = task_labels.clone();
        custom_labels.extend_from_slice(&["executor_id"]);
        let task_row_seq_scan_next_duration = HistogramVec::new(
            HistogramOpts::new(
                "batch_row_seq_scan_next_duration",
                "Time spent deserializing into a row in cell based table.",
            )
            .buckets(exponential_buckets(0.0001, 2.0, 20).unwrap()),
            &custom_labels,
        )
        .unwrap();
        descs.extend(task_row_seq_scan_next_duration.desc().into_iter().cloned());

        let metrics = Self {
            descs,
            task_first_poll_delay,
            task_fast_poll_duration,
            task_idle_duration,
            task_poll_duration,
            task_scheduled_duration,
            task_slow_poll_duration,
            task_exchange_recv_row_number,
            task_row_seq_scan_next_duration,
            labels_to_remove: Arc::new(Mutex::new(HashMap::new())),
            finished_tasks: Arc::new(Mutex::new(Vec::new())),
        };
        registry.register(Box::new(metrics.clone())).unwrap();
        metrics
    }

    /// Create a new `BatchTaskMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        Self::new(prometheus::Registry::new())
    }

    pub fn local_for_task(self: Arc<Self>, task_id: TaskId) -> BatchTaskLocalMetrics {
        Arc::new(BatchTaskLocalMetricsInner {
            metrics: self,
            task_labels: vec![
                task_id.query_id.clone(),
                task_id.stage_id.to_string(),
                task_id.task_id.to_string(),
            ],
            task_id,
        })
    }
}

impl Collector for BatchTaskMetrics {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::with_capacity(8);

        macro_rules! collect_metric {
            ($({ $metric:ident, $type:ty },)*) => {
                $(
                    mfs.extend(self.$metric.collect());
                )*
            };
        }
        for_all_task_metrics!(collect_metric);

        // Clean up metrics for finished tasks.
        let mut guard = self.labels_to_remove.lock();
        for task_id in self.finished_tasks.lock().drain(..) {
            if let Some(callbacks) = guard.remove(&task_id) {
                for callback in callbacks {
                    callback();
                }
            }
        }

        mfs
    }
}

/// A wrapper of `BatchTaskMetrics` that is local to a task. It serves to:
///
/// - Store the labels derived from a `TaskId` to avoid generating it repeatedly.
/// - Remove labels values after the corresponding task finishes.
pub type BatchTaskLocalMetrics = Arc<BatchTaskLocalMetricsInner>;

pub struct BatchTaskLocalMetricsInner {
    metrics: Arc<BatchTaskMetrics>,
    task_id: TaskId,
    task_labels: Vec<String>,
}

impl BatchTaskLocalMetricsInner {
    pub fn task_labels(&self) -> Vec<&str> {
        self.task_labels.iter().map(AsRef::as_ref).collect()
    }

    /// Schedule values associated with `labels` on `metric` to be removed after the task owning
    /// these metrics finishes.
    pub fn remove_labels_after_finish<T: MetricVecBuilder + 'static>(
        &self,
        metric: MetricVec<T>,
        labels: &[&str],
    ) {
        let owned_labels = labels.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        self.metrics
            .labels_to_remove
            .lock()
            .entry(self.task_id.clone())
            .or_default()
            .push(Box::new(move || {
                let _ = metric.remove_label_values(
                    &owned_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                );
            }));
    }
}

impl Deref for BatchTaskLocalMetricsInner {
    type Target = Arc<BatchTaskMetrics>;

    fn deref(&self) -> &Self::Target {
        &self.metrics
    }
}

impl Drop for BatchTaskLocalMetricsInner {
    fn drop(&mut self) {
        self.metrics
            .finished_tasks
            .lock()
            .push(self.task_id.clone());
    }
}

#[derive(Clone)]
pub struct BatchManagerMetrics {
    pub task_num: IntGauge,
}

impl BatchManagerMetrics {
    pub fn new(registry: Registry) -> Self {
        let task_num = IntGauge::new("batch_task_num", "Number of batch task in memory").unwrap();

        registry.register(Box::new(task_num.clone())).unwrap();
        Self { task_num }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::new(Registry::new())
    }
}
