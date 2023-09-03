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

use std::sync::LazyLock;

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_with_registry,
    register_int_counter_with_registry, register_int_gauge_with_registry, Histogram, IntGauge,
    Registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

#[derive(Clone)]
pub struct FrontendMetrics {
    pub query_counter_local_execution: GenericCounter<AtomicU64>,
    pub latency_local_execution: Histogram,
    pub active_sessions: IntGauge,
}

pub static GLOBAL_FRONTEND_METRICS: LazyLock<FrontendMetrics> =
    LazyLock::new(|| FrontendMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl FrontendMetrics {
    fn new(registry: &Registry) -> Self {
        let query_counter_local_execution = register_int_counter_with_registry!(
            "frontend_query_counter_local_execution",
            "Total query number of local execution mode",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "frontend_latency_local_execution",
            "latency of local execution mode",
            exponential_buckets(0.01, 2.0, 23).unwrap()
        );
        let latency_local_execution = register_histogram_with_registry!(opts, registry).unwrap();

        let active_sessions = register_int_gauge_with_registry!(
            "frontend_active_sessions",
            "Total number of active sessions in frontend",
            registry
        )
        .unwrap();

        Self {
            query_counter_local_execution,
            latency_local_execution,
            active_sessions,
        }
    }

    /// Create a new `FrontendMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        GLOBAL_FRONTEND_METRICS.clone()
    }
}
