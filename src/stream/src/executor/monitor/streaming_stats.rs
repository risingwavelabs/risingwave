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

use prometheus::core::{AtomicF64, AtomicU64, GenericCounterVec, GenericGaugeVec};
use prometheus::{
    register_gauge_vec_with_registry, register_int_counter_vec_with_registry, Registry,
};

pub struct StreamingMetrics {
    pub registry: Registry,
    pub actor_row_count: GenericCounterVec<AtomicU64>,
    pub actor_processing_time: GenericGaugeVec<AtomicF64>,
    pub actor_barrier_time: GenericGaugeVec<AtomicF64>,
    pub source_output_row_count: GenericCounterVec<AtomicU64>,
    pub exchange_recv_size: GenericCounterVec<AtomicU64>,
}

impl StreamingMetrics {
    pub fn new(registry: Registry) -> Self {
        let actor_row_count = register_int_counter_vec_with_registry!(
            "stream_actor_row_count",
            "Total number of rows that have been output from each actor",
            &["actor_id"],
            registry
        )
        .unwrap();

        let source_output_row_count = register_int_counter_vec_with_registry!(
            "stream_source_output_rows_counts",
            "Total number of rows that have been output from source",
            &["source_id"],
            registry
        )
        .unwrap();

        let actor_processing_time = register_gauge_vec_with_registry!(
            "stream_actor_processing_time",
            "Time between merge node produces its first chunk in one epoch and barrier gets dispatched from actor_id",
            &["actor_id", "merge_node_id"],
            registry
        )
        .unwrap();

        let actor_barrier_time = register_gauge_vec_with_registry!(
            "stream_actor_barrier_time",
            "Time between merge node produces a barrier and barrier gets dispatched from actor_id",
            &["actor_id", "merge_node_id"],
            registry
        )
        .unwrap();

        let exchange_recv_size = register_int_counter_vec_with_registry!(
            "stream_exchange_recv_size",
            "Total size of messages that have been received from upstream Actor",
            &["up_actor_id", "down_actor_id"],
            registry
        )
        .unwrap();

        Self {
            registry,
            actor_row_count,
            actor_processing_time,
            actor_barrier_time,
            source_output_row_count,
            exchange_recv_size,
        }
    }

    /// Create a new `StreamingMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(prometheus::Registry::new())
    }
}
