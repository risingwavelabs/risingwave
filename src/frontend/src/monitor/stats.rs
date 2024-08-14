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

use std::sync::{Arc, LazyLock};

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry, Histogram, HistogramVec, IntGauge, Registry,
};
use risingwave_common::metrics::TrAdderGauge;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

#[derive(Clone)]
pub struct FrontendMetrics {
    pub query_counter_local_execution: GenericCounter<AtomicU64>,
    pub latency_local_execution: Histogram,
    pub active_sessions: IntGauge,
    pub batch_total_mem: TrAdderGauge,
    pub cursor_metrics: Arc<CursorMetrics>,
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

        let batch_total_mem = TrAdderGauge::new(
            "frontend_batch_total_mem",
            "All memory usage of batch executors in bytes",
        )
        .unwrap();

        registry
            .register(Box::new(batch_total_mem.clone()))
            .unwrap();

        let cursor_metrics = Arc::new(CursorMetrics::new(registry));
        Self {
            query_counter_local_execution,
            latency_local_execution,
            active_sessions,
            batch_total_mem,
            cursor_metrics,
        }
    }

    /// Create a new `FrontendMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        GLOBAL_FRONTEND_METRICS.clone()
    }
}

#[derive(Clone)]
pub struct CursorMetrics {
    pub valid_subsription_cursor_nums: IntGauge,
    pub invalid_subsription_cursor_nums: IntGauge,
    pub subscription_cursor_error_count: GenericCounter<AtomicU64>,
    pub subscription_cursor_query_duration: HistogramVec,
    pub subscription_cursor_declare_duration: HistogramVec,
    pub subscription_cursor_fetch_duration: HistogramVec,
    pub subscription_cursor_last_fetch_duration: HistogramVec,
}

impl CursorMetrics {
    fn new(registry: &Registry) -> Self {
        let valid_subsription_cursor_nums = register_int_gauge_with_registry!(
            "valid_subsription_cursor_nums",
            "The number of valid subscription cursor",
            registry
        )
        .unwrap();
        let invalid_subsription_cursor_nums = register_int_gauge_with_registry!(
            "invalid_subsription_cursor_nums",
            "The number of invalid subscription cursor",
            registry
        )
        .unwrap();
        let subscription_cursor_error_count = register_int_counter_with_registry!(
            "subscription_cursor_error_count",
            "The subscription error num of cursor",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "subscription_cursor_query_duration",
            "The amount of time a query exists inside the cursor",
            exponential_buckets(0.1, 2.0, 22).unwrap(),
        );
        let subscription_cursor_query_duration =
            register_histogram_vec_with_registry!(opts, &["cursor_name"], registry).unwrap();

        let opts = histogram_opts!(
            "subscription_cursor_declare_duration",
            "Subscription cursor duration of declare",
            exponential_buckets(0.01, 2.0, 22).unwrap(),
        );
        let subscription_cursor_declare_duration =
            register_histogram_vec_with_registry!(opts, &["cursor_name"], registry).unwrap();

        let opts = histogram_opts!(
            "subscription_cursor_fetch_duration",
            "Subscription cursor duration of fetch",
            exponential_buckets(0.1, 2.0, 22).unwrap(),
        );
        let subscription_cursor_fetch_duration =
            register_histogram_vec_with_registry!(opts, &["cursor_name"], registry).unwrap();

        let opts = histogram_opts!(
            "subscription_cursor_last_fetch_duration",
            "Since the last fetch, the time up to now",
            exponential_buckets(0.1, 2.0, 22).unwrap(),
        );
        let subscription_cursor_last_fetch_duration =
            register_histogram_vec_with_registry!(opts, &["cursor_name"], registry).unwrap();
        Self {
            valid_subsription_cursor_nums,
            invalid_subsription_cursor_nums,
            subscription_cursor_error_count,
            subscription_cursor_query_duration,
            subscription_cursor_declare_duration,
            subscription_cursor_fetch_duration,
            subscription_cursor_last_fetch_duration,
        }
    }
}
