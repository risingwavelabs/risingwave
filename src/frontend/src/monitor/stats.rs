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

use core::mem;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_vec_with_registry,
    register_histogram_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry, Histogram, HistogramVec, IntGauge, Registry,
};
use risingwave_common::metrics::TrAdderGauge;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use tokio::task::JoinHandle;

use crate::session::SessionMapRef;

#[derive(Clone)]
pub struct FrontendMetrics {
    pub query_counter_local_execution: GenericCounter<AtomicU64>,
    pub latency_local_execution: Histogram,
    pub active_sessions: IntGauge,
    pub batch_total_mem: TrAdderGauge,
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

        Self {
            query_counter_local_execution,
            latency_local_execution,
            active_sessions,
            batch_total_mem,
        }
    }

    /// Create a new `FrontendMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        GLOBAL_FRONTEND_METRICS.clone()
    }
}

#[derive(Clone)]
pub struct CursorMetrics {
    pub subscription_cursor_error_count: GenericCounter<AtomicU64>,
    pub subscription_cursor_query_duration: HistogramVec,
    pub subscription_cursor_declare_duration: HistogramVec,
    pub subscription_cursor_fetch_duration: HistogramVec,
    _cursor_metrics_collector: Arc<CursorMetricsCollector>,
}

impl CursorMetrics {
    pub fn new(registry: &Registry, session_map: SessionMapRef) -> Self {
        let subscription_cursor_error_count = register_int_counter_with_registry!(
            "subscription_cursor_error_count",
            "The subscription error num of cursor",
            registry
        )
        .unwrap();
        let opts = histogram_opts!(
            "subscription_cursor_query_duration",
            "The amount of time a query exists inside the cursor",
            exponential_buckets(1.0, 5.0, 11).unwrap(),
        );
        let subscription_cursor_query_duration =
            register_histogram_vec_with_registry!(opts, &["subscription_name"], registry).unwrap();

        let opts = histogram_opts!(
            "subscription_cursor_declare_duration",
            "Subscription cursor duration of declare",
            exponential_buckets(1.0, 5.0, 11).unwrap(),
        );
        let subscription_cursor_declare_duration =
            register_histogram_vec_with_registry!(opts, &["subscription_name"], registry).unwrap();

        let opts = histogram_opts!(
            "subscription_cursor_fetch_duration",
            "Subscription cursor duration of fetch",
            exponential_buckets(1.0, 5.0, 11).unwrap(),
        );
        let subscription_cursor_fetch_duration =
            register_histogram_vec_with_registry!(opts, &["subscription_name"], registry).unwrap();
        Self {
            _cursor_metrics_collector: Arc::new(CursorMetricsCollector::new(session_map, registry)),
            subscription_cursor_error_count,
            subscription_cursor_query_duration,
            subscription_cursor_declare_duration,
            subscription_cursor_fetch_duration,
        }
    }
}

pub struct PeriodicCursorMetrics {
    pub subsription_cursor_nums: i64,
    pub invalid_subsription_cursor_nums: i64,
    pub subscription_cursor_last_fetch_duration: HashMap<String, f64>,
}

struct CursorMetricsCollector {
    _join_handle: JoinHandle<()>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}
impl CursorMetricsCollector {
    fn new(session_map: SessionMapRef, registry: &Registry) -> Self {
        const COLLECT_INTERVAL_SECONDS: u64 = 60;

        let subsription_cursor_nums = register_int_gauge_with_registry!(
            "subsription_cursor_nums",
            "The number of subscription cursor",
            registry
        )
        .unwrap();
        let invalid_subsription_cursor_nums = register_int_gauge_with_registry!(
            "invalid_subsription_cursor_nums",
            "The number of invalid subscription cursor",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "subscription_cursor_last_fetch_duration",
            "Since the last fetch, the time up to now",
            exponential_buckets(1.0, 5.0, 11).unwrap(),
        );
        let subscription_cursor_last_fetch_duration =
            register_histogram_vec_with_registry!(opts, &["subscription_name"], registry).unwrap();

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut monitor_interval =
                tokio::time::interval(Duration::from_secs(COLLECT_INTERVAL_SECONDS));
            monitor_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = monitor_interval.tick() => {},
                    // Shutdown monitor
                    _ = &mut shutdown_rx => {
                        tracing::info!("Fragment info monitor is stopped");
                        return;
                    }
                }

                let session_vec = { session_map.read().values().cloned().collect::<Vec<_>>() };
                let mut subsription_cursor_nums_value = 0;
                let mut invalid_subsription_cursor_nums_value = 0;
                for session in &session_vec {
                    let periodic_cursor_metrics = session
                        .get_cursor_manager()
                        .get_periodic_cursor_metrics()
                        .await;
                    subsription_cursor_nums_value +=
                        periodic_cursor_metrics.subsription_cursor_nums;
                    invalid_subsription_cursor_nums_value +=
                        periodic_cursor_metrics.invalid_subsription_cursor_nums;
                    for (subscription_name, duration) in
                        &periodic_cursor_metrics.subscription_cursor_last_fetch_duration
                    {
                        subscription_cursor_last_fetch_duration
                            .with_label_values(&[subscription_name])
                            .observe(*duration);
                    }
                }
                subsription_cursor_nums.set(subsription_cursor_nums_value);
                invalid_subsription_cursor_nums.set(invalid_subsription_cursor_nums_value);
            }
        });
        Self {
            _join_handle: join_handle,
            shutdown_tx: Some(shutdown_tx),
        }
    }
}
impl Drop for CursorMetricsCollector {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = mem::take(&mut self.shutdown_tx) {
            shutdown_tx.send(()).unwrap();
        }
    }
}
