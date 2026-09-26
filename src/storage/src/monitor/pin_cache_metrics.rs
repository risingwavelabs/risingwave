// Copyright 2026 RisingWave Labs
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

use prometheus::{
    IntCounter, IntCounterVec, IntGauge, Registry, register_int_counter_vec_with_registry,
    register_int_counter_with_registry, register_int_gauge_vec_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

pub(crate) static GLOBAL_PIN_CACHE_METRICS: LazyLock<PinCacheMetrics> =
    LazyLock::new(|| PinCacheMetrics::new(&GLOBAL_METRICS_REGISTRY));

/// Metrics for the Hummock-local Pin Cache lifecycle and physical reclamation.
pub(crate) struct PinCacheMetrics {
    pub io_failures: IntCounterVec,
    pub capacity_bytes: IntGauge,
    pub accounted_bytes: IntGauge,
    pub uncertain_bytes: IntGauge,
    pub published_objects: IntGauge,
    pub published_bytes: IntGauge,
    pub recovery_ready: IntGauge,
    pub recovery_failures: IntCounter,
    pub gc_failures: IntCounter,
}

impl PinCacheMetrics {
    fn new(registry: &Registry) -> Self {
        let io_failures = register_int_counter_vec_with_registry!(
            "pin_cache_io_failure_total",
            "Pin Cache refill I/O failures by phase",
            &["phase"],
            registry
        )
        .unwrap();
        let capacity_bytes = register_int_gauge_vec_with_registry!(
            "pin_cache_capacity_bytes",
            "Pin Cache capacity accounting by state",
            &["state"],
            registry
        )
        .unwrap();
        let published_objects = register_int_gauge_with_registry!(
            "pin_cache_published_objects",
            "Complete SST objects currently routed to the local Pin Cache",
            registry
        )
        .unwrap();
        let published_bytes = register_int_gauge_with_registry!(
            "pin_cache_published_bytes",
            "Complete SST bytes currently routed to the local Pin Cache",
            registry
        )
        .unwrap();
        let recovery_ready = register_int_gauge_with_registry!(
            "pin_cache_recovery_ready",
            "Whether the local Pin Cache inventory scan completed successfully",
            registry
        )
        .unwrap();
        let recovery_failures = register_int_counter_with_registry!(
            "pin_cache_recovery_failure_total",
            "Pin Cache inventory scan failures",
            registry
        )
        .unwrap();
        let gc_failures = register_int_counter_with_registry!(
            "pin_cache_gc_failure_total",
            "Failed Pin Cache reclaim batches",
            registry
        )
        .unwrap();

        for phase in [
            "remote_read_init",
            "remote_read",
            "local_upload_init",
            "local_upload_write",
            "local_upload_finish",
            "local_metadata",
            "size_validation",
        ] {
            let _ = io_failures.with_label_values(&[phase]);
        }

        Self {
            io_failures,
            accounted_bytes: capacity_bytes.with_label_values(&["accounted"]),
            uncertain_bytes: capacity_bytes.with_label_values(&["uncertain"]),
            capacity_bytes: capacity_bytes.with_label_values(&["capacity"]),
            published_objects,
            published_bytes,
            recovery_ready,
            recovery_failures,
            gc_failures,
        }
    }
}
