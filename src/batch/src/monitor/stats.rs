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
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::Mutex;
use paste::paste;
use prometheus::core::{
    AtomicF64, AtomicU64, Collector, Desc, GenericCounter, GenericCounterVec, GenericGaugeVec,
};
use prometheus::{
    exponential_buckets, opts, proto, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    IntCounterVec, IntGauge, IntGaugeVec, Registry,
};
use risingwave_common::metrics::TrAdderGauge;

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
            { task_mem_usage, IntGaugeVec },
        }
    };
}

macro_rules! def_task_metrics {
    ($( { $metric:ident, $type:ty }, )*) => {
        #[derive(Clone)]
        pub struct BatchTaskMetrics {
            descs: Vec<Desc>,
            delete_task: Arc<Mutex<Vec<TaskId>>>,
            $( pub $metric: $type, )*
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

        let task_mem_usage = IntGaugeVec::new(
            opts!(
                "batch_task_mem_usage",
                "Memory usage of batch tasks in bytes."
            ),
            &task_labels[..],
        )
        .unwrap();
        descs.extend(task_mem_usage.desc().into_iter().cloned());

        let metrics = Self {
            descs,
            task_first_poll_delay,
            task_fast_poll_duration,
            task_idle_duration,
            task_poll_duration,
            task_scheduled_duration,
            task_slow_poll_duration,
            task_mem_usage,
            delete_task: Arc::new(Mutex::new(Vec::new())),
        };
        registry.register(Box::new(metrics.clone())).unwrap();
        metrics
    }

    /// Create a new `BatchTaskMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        Self::new(prometheus::Registry::new())
    }

    fn clean_metrics(&self) {
        let delete_task: Vec<TaskId> = {
            let mut delete_task = self.delete_task.lock();
            if delete_task.is_empty() {
                return;
            }
            std::mem::take(delete_task.as_mut())
        };
        for id in &delete_task {
            let stage_id = id.stage_id.to_string();
            let task_id = id.task_id.to_string();
            let labels = vec![id.query_id.as_str(), stage_id.as_str(), task_id.as_str()];

            macro_rules! remove {
                ($({ $metric:ident, $type:ty},)*) => {
                    $(
                        self.$metric.remove_label_values(&labels).expect("It should not have duplicate task label.");
                    )*
                };
            }
            for_all_task_metrics!(remove);
        }
    }

    pub fn add_delete_task(&self, id: TaskId) {
        self.delete_task.lock().push(id);
    }
}

impl Collector for BatchTaskMetrics {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::with_capacity(8);

        macro_rules! collect {
            ($({ $metric:ident, $type:ty },)*) => {
                $(
                    mfs.extend(self.$metric.collect());
                )*
            };
        }
        for_all_task_metrics!(collect);

        // TODO: Every time we execute it involving get the lock, here maybe a bottleneck.
        self.clean_metrics();

        mfs
    }
}

macro_rules! for_all_executor_metrics {
    ($macro:ident) => {
        $macro! {
            { exchange_recv_row_number,  GenericCounterVec<AtomicU64>, GenericCounter<AtomicU64>},
            { row_seq_scan_next_duration, HistogramVec , Histogram},
            { mem_usage, IntGaugeVec, IntGauge },
        }
    };
}

macro_rules! def_executor_metrics {
    ($( { $metric:ident, $type:ty, $_t:ty }, )*) => {
        #[derive(Clone)]
        pub struct BatchExecutorMetrics {
            descs: Vec<Desc>,
            delete_task: Arc<Mutex<Vec<TaskId>>>,
            register_labels: Arc<Mutex<HashMap<TaskId, Vec<Box<dyn Fn() + Send>>>>>,
            $( pub $metric: $type, )*
        }
    };
}

for_all_executor_metrics!(def_executor_metrics);

impl BatchExecutorMetrics {
    pub fn new(register: Registry) -> Self {
        let executor_labels = vec!["query_id", "stage_id", "task_id", "executor_id"];
        let mut descs = Vec::with_capacity(2);

        let mut custom_labels = executor_labels.clone();
        custom_labels.extend_from_slice(&["source_query_id", "source_stage_id", "source_task_id"]);
        let exchange_recv_row_number = IntCounterVec::new(
            opts!(
                "batch_exchange_recv_row_number",
                "Total number of row that have been received from upstream source",
            ),
            &custom_labels,
        )
        .unwrap();
        descs.extend(exchange_recv_row_number.desc().into_iter().cloned());

        let row_seq_scan_next_duration = HistogramVec::new(
            HistogramOpts::new(
                "batch_row_seq_scan_next_duration",
                "Time spent deserializing into a row in cell based table.",
            )
            .buckets(exponential_buckets(0.0001, 2.0, 20).unwrap()),
            &executor_labels,
        )
        .unwrap();
        descs.extend(row_seq_scan_next_duration.desc().into_iter().cloned());

        let mem_usage = IntGaugeVec::new(
            opts!(
                "batch_executor_mem_usage",
                "Batch executor memory usage in bytes."
            ),
            &executor_labels,
        )
        .unwrap();
        descs.extend(mem_usage.desc().into_iter().cloned());

        let metrics = Self {
            descs,
            delete_task: Arc::new(Mutex::new(Vec::new())),
            exchange_recv_row_number,
            row_seq_scan_next_duration,
            mem_usage,
            register_labels: Arc::new(Mutex::new(HashMap::new())),
        };
        register.register(Box::new(metrics.clone())).unwrap();
        metrics
    }

    /// Create a new `BatchTaskMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        Self::new(prometheus::Registry::new())
    }

    fn clean_metrics(&self) {
        let delete_task: Vec<TaskId> = {
            let mut delete_task = self.delete_task.lock();
            if delete_task.is_empty() {
                return;
            }
            std::mem::take(delete_task.as_mut())
        };
        let delete_labels = {
            let mut register_labels = self.register_labels.lock();
            let mut delete_labels = Vec::with_capacity(delete_task.len());
            for id in delete_task {
                if let Some(callback) = register_labels.remove(&id) {
                    delete_labels.push(callback);
                }
            }
            delete_labels
        };
        delete_labels
            .into_iter()
            .for_each(|delete_labels| delete_labels.into_iter().for_each(|callback| callback()));
    }

    pub fn add_delete_task(&self, task_id: TaskId) {
        self.delete_task.lock().push(task_id);
    }
}

impl Collector for BatchExecutorMetrics {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::with_capacity(2);

        mfs.extend(self.exchange_recv_row_number.collect());
        mfs.extend(self.row_seq_scan_next_duration.collect());

        self.clean_metrics();

        mfs
    }
}

pub type BatchMetricsWithTaskLabels = Arc<BatchMetricsWithTaskLabelsInner>;

/// A wrapper of `BatchTaskMetrics` and `BatchExecutorMetrics` that contains the labels derived from
/// a `TaskId` so that we don't have to pass `task_id` around and repeatedly generate the same
/// labels.
pub struct BatchMetricsWithTaskLabelsInner {
    task_metrics: Arc<BatchTaskMetrics>,
    executor_metrics: Arc<BatchExecutorMetrics>,
    task_id: TaskId,
    task_labels: Vec<String>,
}

macro_rules! def_create_executor_collector {
    ($( { $metric:ident, $type:ty, $collector_type:ty }, )*) => {
        paste! {
            $(
                pub fn [<create_collector_for_ $metric>](&self,executor_label: Vec<String>) -> $collector_type {
                    let mut owned_task_labels = self.task_labels.clone();
                    owned_task_labels.extend(executor_label);
                    let task_labels = owned_task_labels.iter().map(|s| s.as_str()).collect_vec();

                    let collecter = self
                        .executor_metrics
                        .$metric
                        .with_label_values(&task_labels);

                    let metrics = self.executor_metrics.$metric.clone();

                    self.executor_metrics
                        .register_labels
                        .lock()
                        .entry(self.task_id.clone())
                        .or_default()
                        .push(Box::new(move || {
                            metrics.remove_label_values(
                                &owned_task_labels.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                            ).expect("Collector with same label only can be created once. It should never have case of duplicate remove");
                        }));

                    collecter
                }
            )*
        }
    };
}

impl BatchMetricsWithTaskLabelsInner {
    for_all_executor_metrics! {def_create_executor_collector}

    pub fn new(
        task_metrics: Arc<BatchTaskMetrics>,
        executor_metrics: Arc<BatchExecutorMetrics>,
        id: TaskId,
    ) -> Self {
        Self {
            task_metrics,
            executor_metrics,
            task_id: id.clone(),
            task_labels: vec![id.query_id, id.stage_id.to_string(), id.task_id.to_string()],
        }
    }

    pub fn task_labels(&self) -> Vec<&str> {
        self.task_labels.iter().map(AsRef::as_ref).collect()
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id.clone()
    }

    pub fn get_task_metrics(&self) -> &Arc<BatchTaskMetrics> {
        &self.task_metrics
    }
}

impl Drop for BatchMetricsWithTaskLabelsInner {
    fn drop(&mut self) {
        self.task_metrics.delete_task.lock().push(self.task_id());
        self.executor_metrics
            .delete_task
            .lock()
            .push(self.task_id());
    }
}

#[derive(Clone)]
pub struct BatchManagerMetrics {
    pub task_num: IntGauge,
    pub batch_total_mem: TrAdderGauge,
}

impl BatchManagerMetrics {
    pub fn new(registry: Registry) -> Self {
        let task_num = IntGauge::new("batch_task_num", "Number of batch task in memory").unwrap();
        let batch_total_mem = TrAdderGauge::new(
            "batch_total_mem",
            "Total number of memory usage for batch tasks.",
        )
        .unwrap();

        registry.register(Box::new(task_num.clone())).unwrap();
        Self {
            task_num,
            batch_total_mem,
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::new(Registry::new())
    }
}
