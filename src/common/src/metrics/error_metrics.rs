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
use std::sync::{Arc, LazyLock};

use itertools::Itertools;
use parking_lot::Mutex;
use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge, LabelPair, Metric, MetricFamily};
use prometheus::Registry;

use crate::monitor::GLOBAL_METRICS_REGISTRY;
use crate::util::iter_util::ZipEqFast;

pub struct ErrorMetric<const N: usize> {
    payload: Arc<Mutex<HashMap<[String; N], u32>>>,
    desc: Desc,
}

impl<const N: usize> ErrorMetric<N> {
    pub fn new(name: &str, help: &str, label_names: &[&str; N]) -> Self {
        Self {
            payload: Default::default(),
            desc: Desc::new(
                name.to_owned(),
                help.to_owned(),
                label_names.iter().map(|l| l.to_string()).collect_vec(),
                Default::default(),
            )
            .unwrap(),
        }
    }

    pub fn report(&self, labels: [String; N]) {
        let mut m = self.payload.lock();
        let v = m.entry(labels).or_default();
        *v += 1;
    }

    fn collect(&self) -> MetricFamily {
        let mut m = MetricFamily::default();
        m.set_name(self.desc.fq_name.clone());
        m.set_help(self.desc.help.clone());
        m.set_field_type(prometheus::proto::MetricType::GAUGE);

        let payload = self.payload.lock().drain().collect_vec();
        let mut metrics = Vec::with_capacity(payload.len());
        for (labels, count) in payload {
            let mut label_pairs = Vec::with_capacity(self.desc.variable_labels.len());
            for (name, label) in self.desc.variable_labels.iter().zip_eq_fast(labels) {
                let mut label_pair = LabelPair::default();
                label_pair.set_name(name.clone());
                label_pair.set_value(label);
                label_pairs.push(label_pair);
            }

            let mut metric = Metric::new();
            metric.set_label(label_pairs.into());
            let mut gauge = Gauge::default();
            gauge.set_value(count as f64);
            metric.set_gauge(gauge);
            metrics.push(metric);
        }
        m.set_metric(metrics.into());
        m
    }
}

pub type ErrorMetricRef<const N: usize> = Arc<ErrorMetric<N>>;

#[derive(Clone)]
pub struct ErrorMetrics {
    pub user_sink_error: ErrorMetricRef<3>,
    pub user_compute_error: ErrorMetricRef<4>,
    pub user_source_reader_error: ErrorMetricRef<5>,
    pub user_source_error: ErrorMetricRef<5>,
}

impl ErrorMetrics {
    pub fn new() -> Self {
        Self {
            user_sink_error: Arc::new(ErrorMetric::new(
                "user_sink_error",
                "Sink errors in the system, queryable by tags",
                &["connector_name", "executor_id", "error_msg"],
            )),
            user_compute_error: Arc::new(ErrorMetric::new(
                "user_compute_error",
                "Compute errors in the system, queryable by tags",
                &["error_type", "error_msg", "executor_name", "fragment_id"],
            )),
            user_source_reader_error: Arc::new(ErrorMetric::new(
                "user_source_reader_error",
                "Source reader error count",
                &[
                    "error_type",
                    "error_msg",
                    "executor_name",
                    "actor_id",
                    "source_id",
                ],
            )),
            user_source_error: Arc::new(ErrorMetric::new(
                "user_source_error_count",
                "Source errors in the system, queryable by tags",
                &[
                    "error_type",
                    "error_msg",
                    "executor_name",
                    "fragment_id",
                    "table_id",
                ],
            )),
        }
    }

    fn desc(&self) -> Vec<&Desc> {
        vec![
            &self.user_sink_error.desc,
            &self.user_compute_error.desc,
            &self.user_source_reader_error.desc,
            &self.user_source_error.desc,
        ]
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        vec![
            self.user_sink_error.collect(),
            self.user_compute_error.collect(),
            self.user_source_reader_error.collect(),
            self.user_source_error.collect(),
        ]
    }
}

impl Default for ErrorMetrics {
    fn default() -> Self {
        ErrorMetrics::new()
    }
}

pub struct ErrorMetricsCollector {
    metrics: ErrorMetrics,
}

impl Collector for ErrorMetricsCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.metrics.desc()
    }

    fn collect(&self) -> Vec<prometheus::proto::MetricFamily> {
        self.metrics.collect()
    }
}

pub fn monitor_errors(registry: &Registry, metrics: ErrorMetrics) {
    let ec = ErrorMetricsCollector { metrics };
    registry.register(Box::new(ec)).unwrap()
}

pub static GLOBAL_ERROR_METRICS: LazyLock<ErrorMetrics> = LazyLock::new(|| {
    let e = ErrorMetrics::new();
    monitor_errors(&GLOBAL_METRICS_REGISTRY, e.clone());
    e
});
