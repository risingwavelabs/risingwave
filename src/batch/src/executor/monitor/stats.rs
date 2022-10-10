use std::sync::Arc;

// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use prometheus::core::{AtomicF64, AtomicU64, Collector, Desc, GenericCounterVec, GenericGaugeVec};
use prometheus::{
    exponential_buckets, opts, proto, register_gauge_vec_with_registry,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry, HistogramOpts,
    HistogramVec, Registry,
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
        }
    };
}

for_all_task_metrics!(def_task_metrics);

impl BatchTaskMetrics {
    pub fn new(registry: Registry) -> Self {
        let labels = vec!["query_id", "stage_id", "task_id"];
        let mut descs = Vec::with_capacity(8);

        let task_first_poll_delay = register_gauge_vec_with_registry!(
            opts!(
                "batch_task_first_poll_delay",
                "The total duration (s) elapsed between the instant tasks are instrumented, and the instant they are first polled.",
            ),
            &labels[..],
            registry,
        ).unwrap();
        descs.extend(task_first_poll_delay.desc().into_iter().cloned());

        let task_fast_poll_duration = register_gauge_vec_with_registry!(
            opts!(
                "batch_task_fast_poll_duration",
                "The total duration (s) of fast polls.",
            ),
            &labels[..],
            registry,
        )
        .unwrap();
        descs.extend(task_fast_poll_duration.desc().into_iter().cloned());

        let task_idle_duration = register_gauge_vec_with_registry!(
            opts!(
                "batch_task_idle_duration",
                "The total duration (s) that tasks idled.",
            ),
            &labels[..],
            registry,
        )
        .unwrap();
        descs.extend(task_idle_duration.desc().into_iter().cloned());

        let task_poll_duration = register_gauge_vec_with_registry!(
            opts!(
                "batch_task_poll_duration",
                "The total duration (s) elapsed during polls.",
            ),
            &labels[..],
            registry,
        )
        .unwrap();
        descs.extend(task_poll_duration.desc().into_iter().cloned());

        let task_scheduled_duration = register_gauge_vec_with_registry!(
            opts!(
                "batch_task_scheduled_duration",
                "The total duration (s) that tasks spent waiting to be polled after awakening.",
            ),
            &labels[..],
            registry,
        )
        .unwrap();
        descs.extend(task_scheduled_duration.desc().into_iter().cloned());

        let task_slow_poll_duration = register_gauge_vec_with_registry!(
            opts!(
                "batch_task_slow_poll_duration",
                "The total duration (s) of slow polls.",
            ),
            &labels[..],
            registry,
        )
        .unwrap();
        descs.extend(task_slow_poll_duration.desc().into_iter().cloned());

        let mut task_slow_poll_duration_labels = labels.clone();
        task_slow_poll_duration_labels.extend_from_slice(&[
            "executor_id",
            "source_query_id",
            "source_stage_id",
            "source_task_id",
        ]);
        let task_exchange_recv_row_number = register_int_counter_vec_with_registry!(
            opts!(
                "batch_task_exchange_recv_row_number",
                "Total number of row that have been received from upstream source",
            ),
            &task_slow_poll_duration_labels,
            registry,
        )
        .unwrap();
        descs.extend(task_slow_poll_duration.desc().into_iter().cloned());

        let mut task_row_seq_scan_next_duration_labels = labels.clone();
        task_row_seq_scan_next_duration_labels.extend_from_slice(&["executor_id"]);
        let task_row_seq_scan_next_duration = register_histogram_vec_with_registry!(
            HistogramOpts::new(
                "batch_row_seq_scan_next_duration",
                "Time spent deserializing into a row in cell based table.",
            )
            .buckets(exponential_buckets(0.0001, 2.0, 20).unwrap()),
            &task_row_seq_scan_next_duration_labels,
            registry,
        )
        .unwrap();
        descs.extend(task_slow_poll_duration.desc().into_iter().cloned());

        Self {
            descs,
            task_first_poll_delay,
            task_fast_poll_duration,
            task_idle_duration,
            task_poll_duration,
            task_scheduled_duration,
            task_slow_poll_duration,
            task_exchange_recv_row_number,
            task_row_seq_scan_next_duration,
        }
    }

    /// Create a new `BatchTaskMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        Self::new(prometheus::Registry::new())
    }
}

impl Collector for BatchTaskMetrics {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut mfs = Vec::with_capacity(8);

        macro_rules! collect_and_clear {
            ($({ $metric:ident, $type:ty },)*) => {
                $(
                    mfs.extend(self.$metric.collect());
                    self.$metric.reset();
                )*
            };
        }
        for_all_task_metrics!(collect_and_clear);
        mfs
    }
}

/// A wrapper with `BatchTaskMetrics` and label values derived from the task id.
#[derive(Clone)]
pub struct BatchTaskMetricsWithLabels {
    pub metrics: Arc<BatchTaskMetrics>,
    labels: Vec<String>,
}

impl BatchTaskMetricsWithLabels {
    pub fn new(metrics: Arc<BatchTaskMetrics>, id: TaskId) -> Self {
        Self {
            metrics,
            labels: vec![id.query_id, id.stage_id.to_string(), id.task_id.to_string()],
        }
    }

    pub fn task_labels(&self) -> Vec<&str> {
        self.labels.iter().map(AsRef::as_ref).collect()
    }
}
