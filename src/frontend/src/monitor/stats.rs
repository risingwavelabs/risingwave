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

use prometheus::core::{AtomicU64, GenericCounter};
use prometheus::{
    register_histogram_with_registry, register_int_counter_with_registry, Histogram, Registry,
};

pub struct FrontendMetrics {
    pub registry: Registry,
    pub query_counter_local_execution: GenericCounter<AtomicU64>,
    pub latency_local_execution: Histogram,
}

impl FrontendMetrics {
    pub fn new(registry: Registry) -> Self {
        let query_counter_local_execution = register_int_counter_with_registry!(
            "frontend_query_counter_local_execution",
            "Total query number of local execution mode",
            &registry
        )
        .unwrap();

        let latency_local_execution = register_histogram_with_registry!(
            "frontend_latency_local_execution",
            "latency of local execution mode",
            &registry,
        )
        .unwrap();

        Self {
            registry,
            query_counter_local_execution,
            latency_local_execution,
        }
    }

    /// Create a new `FrontendMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        Self::new(prometheus::Registry::new())
    }
}
