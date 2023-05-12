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

use prometheus::core::{AtomicI64, AtomicU64, GenericCounter, GenericGauge};
use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_with_registry,
    register_int_counter_with_registry, register_int_gauge_with_registry, Histogram, Registry,
};

pub struct DistributedQueryMetrics {
    pub registry: Registry,
    pub running_query_num: GenericGauge<AtomicI64>,
    pub rejected_query_counter: GenericCounter<AtomicU64>,
    pub completed_query_counter: GenericCounter<AtomicU64>,
    pub query_latency: Histogram,
}

impl DistributedQueryMetrics {
    pub fn new(registry: Registry) -> Self {
        let running_query_num = register_int_gauge_with_registry!(
            "distributed_running_query_num",
            "The number of running query of distributed execution mode",
            &registry
        )
        .unwrap();

        let rejected_query_counter = register_int_counter_with_registry!(
            "distributed_rejected_query_counter",
            "The number of rejected query in distributed execution mode. ",
            &registry
        )
        .unwrap();

        let completed_query_counter = register_int_counter_with_registry!(
            "distributed_completed_query_counter",
            "The number of query ended sccessfully in distributed execution mode",
            &registry
        )
        .unwrap();

        let opts = histogram_opts!(
            "distributed_query_latency",
            "latency of query executed successfully in distributed execution mode",
            exponential_buckets(0.01, 2.0, 23).unwrap()
        );

        let query_latency = register_histogram_with_registry!(opts, &registry).unwrap();

        Self {
            registry,
            running_query_num,
            rejected_query_counter,
            completed_query_counter,
            query_latency,
        }
    }

    /// Create a new `DistributedQueryMetrics` instance used in tests or other places.
    pub fn for_test() -> Self {
        Self::new(prometheus::Registry::new())
    }
}
