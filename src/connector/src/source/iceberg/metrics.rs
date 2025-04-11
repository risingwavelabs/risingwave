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

use prometheus::Registry;
use risingwave_common::metrics::LabelGuardedIntCounterVec;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::register_guarded_int_counter_vec_with_registry;

#[derive(Clone)]
pub struct IcebergScanMetrics {
    pub iceberg_read_bytes: LabelGuardedIntCounterVec,
}

impl IcebergScanMetrics {
    fn new(registry: &Registry) -> Self {
        let iceberg_read_bytes = register_guarded_int_counter_vec_with_registry!(
            "iceberg_read_bytes",
            "Total size of iceberg read requests",
            &["table_name"],
            registry
        )
        .unwrap();

        Self { iceberg_read_bytes }
    }

    pub fn for_test() -> Arc<Self> {
        Arc::new(GLOBAL_ICEBERG_SCAN_METRICS.clone())
    }
}

pub static GLOBAL_ICEBERG_SCAN_METRICS: LazyLock<IcebergScanMetrics> =
    LazyLock::new(|| IcebergScanMetrics::new(&GLOBAL_METRICS_REGISTRY));
