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

use std::sync::LazyLock;

use prometheus::{IntCounterVec, Registry, register_int_counter_vec_with_registry};

use crate::monitor::GLOBAL_METRICS_REGISTRY;

#[derive(Clone)]
pub struct ErrorMetric<const N: usize> {
    inner: IntCounterVec,
}

impl<const N: usize> ErrorMetric<N> {
    pub fn new(name: &str, help: &str, label_names: &[&str; N], registry: &Registry) -> Self {
        Self {
            inner: register_int_counter_vec_with_registry!(name, help, label_names, registry)
                .unwrap(),
        }
    }

    pub fn report(&self, labels: [String; N]) {
        self.inner.with_label_values(&labels).inc();
    }
}

/// Metrics for counting errors in the system.
///
/// Please avoid adding new error metrics here. Instead, introduce new `error_type` for new errors.
#[derive(Clone)]
pub struct ErrorMetrics {
    pub user_sink_error: ErrorMetric<4>,
    pub user_compute_error: ErrorMetric<3>,
    pub user_source_error: ErrorMetric<4>,
}

impl ErrorMetrics {
    pub fn new(registry: &Registry) -> Self {
        Self {
            user_sink_error: ErrorMetric::new(
                "user_sink_error_cnt",
                "Sink errors in the system, queryable by tags",
                &["error_type", "sink_id", "sink_name", "fragment_id"],
                registry,
            ),
            user_compute_error: ErrorMetric::new(
                "user_compute_error_cnt",
                "Compute errors in the system, queryable by tags",
                &["error_type", "executor_name", "fragment_id"],
                registry,
            ),
            user_source_error: ErrorMetric::new(
                "user_source_error_cnt",
                "Source errors in the system, queryable by tags",
                &["error_type", "source_id", "source_name", "fragment_id"],
                registry,
            ),
        }
    }
}

pub static GLOBAL_ERROR_METRICS: LazyLock<ErrorMetrics> =
    LazyLock::new(|| ErrorMetrics::new(&GLOBAL_METRICS_REGISTRY));
