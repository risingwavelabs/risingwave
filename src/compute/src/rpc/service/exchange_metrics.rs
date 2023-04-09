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

use prometheus::core::{AtomicU64, GenericCounterVec};
use prometheus::{register_int_counter_vec_with_registry, Registry};

pub struct ExchangeServiceMetrics {
    pub registry: Registry,
    pub stream_fragment_exchange_bytes: GenericCounterVec<AtomicU64>,
    pub actor_sampled_serialize_duration_ns: GenericCounterVec<AtomicU64>,
}

impl ExchangeServiceMetrics {
    pub fn new(registry: Registry) -> Self {
        let stream_fragment_exchange_bytes = register_int_counter_vec_with_registry!(
            "stream_exchange_frag_send_size",
            "Total size of messages that have been send to downstream Fragment",
            &["up_fragment_id", "down_fragment_id"],
            registry
        )
        .unwrap();

        let actor_sampled_serialize_duration_ns = register_int_counter_vec_with_registry!(
            "actor_sampled_serialize_duration_ns",
            "Duration (ns) of sampled chunk serialization",
            &["actor_id"],
            registry
        )
        .unwrap();

        Self {
            registry,
            stream_fragment_exchange_bytes,
            actor_sampled_serialize_duration_ns,
        }
    }

    /// Create a new `ExchangeServiceMetrics` instance used in tests or other places.
    pub fn unused() -> Self {
        Self::new(prometheus::Registry::new())
    }
}
