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

use std::sync::{Arc, LazyLock};

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    Histogram, IntGauge, Registry, histogram_opts, register_histogram_with_registry,
    register_int_counter_with_registry,
};
use risingwave_common::metrics::TrAdderGauge;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_connector::source::iceberg::IcebergScanMetrics;

/// Metrics for batch executor.
/// Currently, it contains:
/// - `exchange_recv_row_number`: Total number of row that have been received for exchange.
/// - `row_seq_scan_next_duration`: Time spent iterating next rows.
#[derive(Clone)]
pub struct BatchExecutorMetrics {
    pub exchange_recv_row_number: GenericCounter<AtomicU64>,
    pub row_seq_scan_next_duration: Histogram,
}

pub static GLOBAL_BATCH_EXECUTOR_METRICS: LazyLock<BatchExecutorMetrics> =
    LazyLock::new(|| BatchExecutorMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl BatchExecutorMetrics {
    fn new(register: &Registry) -> Self {
        let exchange_recv_row_number = register_int_counter_with_registry!(
            "batch_exchange_recv_row_number",
            "Total number of row that have been received from upstream source",
            register,
        )
        .unwrap();

        let opts = histogram_opts!(
            "batch_row_seq_scan_next_duration",
            "Time spent deserializing into a row in cell based table.",
        );

        let row_seq_scan_next_duration = register_histogram_with_registry!(opts, register).unwrap();

        Self {
            exchange_recv_row_number,
            row_seq_scan_next_duration,
        }
    }

    /// Create a new `BatchTaskMetrics` instance used in tests or other places.
    pub fn for_test() -> Arc<Self> {
        Arc::new(GLOBAL_BATCH_EXECUTOR_METRICS.clone())
    }
}

pub type BatchMetrics = Arc<BatchMetricsInner>;

/// A wrapper of `BatchManagerMetrics` and `BatchExecutorMetrics` that contains all metrics for batch.
pub struct BatchMetricsInner {
    batch_manager_metrics: Arc<BatchManagerMetrics>,
    executor_metrics: Arc<BatchExecutorMetrics>,
    iceberg_scan_metrics: Arc<IcebergScanMetrics>,
}

impl BatchMetricsInner {
    pub fn new(
        batch_manager_metrics: Arc<BatchManagerMetrics>,
        executor_metrics: Arc<BatchExecutorMetrics>,
        iceberg_scan_metrics: Arc<IcebergScanMetrics>,
    ) -> Self {
        Self {
            batch_manager_metrics,
            executor_metrics,
            iceberg_scan_metrics,
        }
    }

    pub fn executor_metrics(&self) -> &BatchExecutorMetrics {
        &self.executor_metrics
    }

    pub fn batch_manager_metrics(&self) -> &BatchManagerMetrics {
        &self.batch_manager_metrics
    }

    pub fn iceberg_scan_metrics(&self) -> Arc<IcebergScanMetrics> {
        self.iceberg_scan_metrics.clone()
    }

    pub fn for_test() -> BatchMetrics {
        Arc::new(Self {
            batch_manager_metrics: BatchManagerMetrics::for_test(),
            executor_metrics: BatchExecutorMetrics::for_test(),
            iceberg_scan_metrics: IcebergScanMetrics::for_test(),
        })
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
            "compute_batch_total_mem",
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

    pub fn for_test() -> Arc<Self> {
        Arc::new(GLOBAL_BATCH_MANAGER_METRICS.clone())
    }
}

#[derive(Clone)]
pub struct BatchSpillMetrics {
    pub batch_spill_read_bytes: GenericCounter<AtomicU64>,
    pub batch_spill_write_bytes: GenericCounter<AtomicU64>,
}

pub static GLOBAL_BATCH_SPILL_METRICS: LazyLock<BatchSpillMetrics> =
    LazyLock::new(|| BatchSpillMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl BatchSpillMetrics {
    fn new(registry: &Registry) -> Self {
        let batch_spill_read_bytes = register_int_counter_with_registry!(
            "batch_spill_read_bytes",
            "Total bytes of requests read from spill files",
            registry,
        )
        .unwrap();
        let batch_spill_write_bytes = register_int_counter_with_registry!(
            "batch_spill_write_bytes",
            "Total bytes of requests write to spill files",
            registry,
        )
        .unwrap();
        Self {
            batch_spill_read_bytes,
            batch_spill_write_bytes,
        }
    }

    pub fn for_test() -> Arc<Self> {
        Arc::new(GLOBAL_BATCH_SPILL_METRICS.clone())
    }
}
