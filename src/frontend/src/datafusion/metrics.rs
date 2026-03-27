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

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    Histogram, Registry, exponential_buckets, histogram_opts, register_histogram_with_registry,
    register_int_counter_with_registry,
};

#[derive(Clone)]
pub struct DataFusionMetrics {
    pub completed_query_counter: GenericCounter<AtomicU64>,
    pub failed_query_counter: GenericCounter<AtomicU64>,
    pub latency: Histogram,
}

impl DataFusionMetrics {
    pub fn new(registry: &Registry) -> Self {
        let completed_query_counter = register_int_counter_with_registry!(
            "datafusion_completed_query_counter",
            "Total completed query number for DataFusion batch engine",
            registry
        )
        .unwrap();

        let failed_query_counter = register_int_counter_with_registry!(
            "datafusion_failed_query_counter",
            "Total failed query number for DataFusion batch engine",
            registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "datafusion_latency",
            "latency for DataFusion batch engine",
            exponential_buckets(0.01, 2.0, 23).unwrap()
        );
        let latency = register_histogram_with_registry!(opts, registry).unwrap();

        Self {
            completed_query_counter,
            failed_query_counter,
            latency,
        }
    }
}
