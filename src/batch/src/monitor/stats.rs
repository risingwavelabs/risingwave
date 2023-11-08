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

use std::sync::{Arc, LazyLock};

use prometheus::{IntGauge, Registry};
use risingwave_common::metrics::{
    LabelGuardedGaugeVec, LabelGuardedHistogramVec, LabelGuardedIntCounterVec,
    LabelGuardedIntGaugeVec, TrAdderGauge,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

use crate::task::TaskId;

#[derive(Clone)]
pub struct BatchTaskMetrics {
    pub task_first_poll_delay: LabelGuardedGaugeVec<3>,
    pub task_fast_poll_duration: LabelGuardedGaugeVec<3>,
    pub task_idle_duration: LabelGuardedGaugeVec<3>,
    pub task_poll_duration: LabelGuardedGaugeVec<3>,
    pub task_scheduled_duration: LabelGuardedGaugeVec<3>,
    pub task_slow_poll_duration: LabelGuardedGaugeVec<3>,
    pub task_mem_usage: LabelGuardedIntGaugeVec<3>,
}

pub static GLOBAL_BATCH_TASK_METRICS: LazyLock<BatchTaskMetrics> =
    LazyLock::new(|| BatchTaskMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl BatchTaskMetrics {
    /// The created [`BatchTaskMetrics`] is already registered to the `registry`.
    fn new(registry: &Registry) -> Self {
        let task_labels = ["query_id", "stage_id", "task_id"];

        let task_first_poll_delay = register_guarded_gauge_vec_with_registry!(
            "batch_task_first_poll_delay",
            "The total duration (s) elapsed between the instant tasks are instrumented, and the instant they are first polled.",
            &task_labels,
         registry).unwrap();

        let task_fast_poll_duration = register_guarded_gauge_vec_with_registry!(
            "batch_task_fast_poll_duration",
            "The total duration (s) of fast polls.",
            &task_labels,
            registry
        )
        .unwrap();

        let task_idle_duration = register_guarded_gauge_vec_with_registry!(
            "batch_task_idle_duration",
            "The total duration (s) that tasks idled.",
            &task_labels,
            registry
        )
        .unwrap();

        let task_poll_duration = register_guarded_gauge_vec_with_registry!(
            "batch_task_poll_duration",
            "The total duration (s) elapsed during polls.",
            &task_labels,
            registry,
        )
        .unwrap();

        let task_scheduled_duration = register_guarded_gauge_vec_with_registry!(
            "batch_task_scheduled_duration",
            "The total duration (s) that tasks spent waiting to be polled after awakening.",
            &task_labels,
            registry,
        )
        .unwrap();

        let task_slow_poll_duration = register_guarded_gauge_vec_with_registry!(
            "batch_task_slow_poll_duration",
            "The total duration (s) of slow polls.",
            &task_labels,
            registry,
        )
        .unwrap();

        let task_mem_usage = register_guarded_int_gauge_vec_with_registry!(
            "batch_task_mem_usage",
            "Memory usage of batch tasks in bytes.",
            &task_labels,
            registry,
        )
        .unwrap();

        Self {
            task_first_poll_delay,
            task_fast_poll_duration,
            task_idle_duration,
            task_poll_duration,
            task_scheduled_duration,
            task_slow_poll_duration,
            task_mem_usage,
        }
    }

    /// Create a new `BatchTaskMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        GLOBAL_BATCH_TASK_METRICS.clone()
    }
}

#[derive(Clone)]
pub struct BatchExecutorMetrics {
    pub exchange_recv_row_number: LabelGuardedIntCounterVec<4>,
    pub row_seq_scan_next_duration: LabelGuardedHistogramVec<4>,
    pub mem_usage: LabelGuardedIntGaugeVec<4>,
}

pub static GLOBAL_BATCH_EXECUTOR_METRICS: LazyLock<BatchExecutorMetrics> =
    LazyLock::new(|| BatchExecutorMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl BatchExecutorMetrics {
    fn new(register: &Registry) -> Self {
        let executor_labels = ["query_id", "stage_id", "task_id", "executor_id"];

        let exchange_recv_row_number = register_guarded_int_counter_vec_with_registry!(
            "batch_exchange_recv_row_number",
            "Total number of row that have been received from upstream source",
            &executor_labels,
            register,
        )
        .unwrap();

        let row_seq_scan_next_duration = register_guarded_histogram_vec_with_registry!(
            "batch_row_seq_scan_next_duration",
            "Time spent deserializing into a row in cell based table.",
            &executor_labels,
            register,
        )
        .unwrap();

        let mem_usage = register_guarded_int_gauge_vec_with_registry!(
            "batch_executor_mem_usage",
            "Batch executor memory usage in bytes.",
            &executor_labels,
            register,
        )
        .unwrap();

        Self {
            exchange_recv_row_number,
            row_seq_scan_next_duration,
            mem_usage,
        }
    }

    /// Create a new `BatchTaskMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        GLOBAL_BATCH_EXECUTOR_METRICS.clone()
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
    task_labels: [String; 3],
}

impl BatchMetricsWithTaskLabelsInner {
    pub fn new(
        task_metrics: Arc<BatchTaskMetrics>,
        executor_metrics: Arc<BatchExecutorMetrics>,
        id: TaskId,
    ) -> Self {
        Self {
            task_metrics,
            executor_metrics,
            task_id: id.clone(),
            task_labels: [id.query_id, id.stage_id.to_string(), id.task_id.to_string()],
        }
    }

    pub fn task_labels(&self) -> [&str; 3] {
        self.task_labels.each_ref().map(String::as_str)
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id.clone()
    }

    pub fn get_task_metrics(&self) -> &Arc<BatchTaskMetrics> {
        &self.task_metrics
    }

    pub fn executor_metrics(&self) -> &BatchExecutorMetrics {
        &self.executor_metrics
    }

    pub fn executor_labels<'a>(
        &'a self,
        executor_id: &'a (impl AsRef<str> + ?Sized),
    ) -> [&'a str; 4] {
        [
            self.task_labels[0].as_str(),
            self.task_labels[1].as_str(),
            self.task_labels[2].as_str(),
            executor_id.as_ref(),
        ]
    }
}

#[derive(Clone)]
pub struct BatchManagerMetrics {
    pub task_num: IntGauge,
    pub batch_total_mem: TrAdderGauge,
    pub batch_heartbeat_worker_num: IntGauge,
}

pub static GLOBAL_BATCH_MANAGER_METRICS: LazyLock<BatchManagerMetrics> =
    LazyLock::new(|| BatchManagerMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl BatchManagerMetrics {
    fn new(registry: &Registry) -> Self {
        let task_num = IntGauge::new("batch_task_num", "Number of batch task in memory").unwrap();
        let batch_total_mem = TrAdderGauge::new(
            "batch_total_mem",
            "Total number of memory usage for batch tasks.",
        )
        .unwrap();
        let batch_heartbeat_worker_num = IntGauge::new(
            "batch_heartbeat_worker_num",
            "Total number of heartbeat worker for batch tasks.",
        )
        .unwrap();

        registry.register(Box::new(task_num.clone())).unwrap();
        registry
            .register(Box::new(batch_total_mem.clone()))
            .unwrap();
        registry
            .register(Box::new(batch_heartbeat_worker_num.clone()))
            .unwrap();
        Self {
            task_num,
            batch_total_mem,
            batch_heartbeat_worker_num,
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        GLOBAL_BATCH_MANAGER_METRICS.clone()
    }
}
