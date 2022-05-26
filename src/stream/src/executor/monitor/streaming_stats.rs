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

use prometheus::core::{AtomicU64, GenericCounterVec};
use prometheus::{register_int_counter_vec_with_registry, Registry};

pub struct StreamingMetrics {
    pub registry: Registry,
    pub actor_row_count: GenericCounterVec<AtomicU64>,

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

        let exchange_recv_size = register_int_counter_vec_with_registry!(
            "stream_exchange_recv_size",
            "Total size of stream chunks that have been received from upstream Actor",
            &["up_actor_id", "down_actor_id"],
            registry
        )
        .unwrap();

        Self {
            registry,
            actor_row_count,
            source_output_row_count,
            exchange_recv_size,
        }
    }

    /// Create a new `StreamingMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(prometheus::Registry::new())
    }
}
