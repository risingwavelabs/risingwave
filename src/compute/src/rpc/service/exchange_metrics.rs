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

use std::sync::LazyLock;

use prometheus::core::{AtomicU64, GenericCounterVec};
use prometheus::{Registry, register_int_counter_vec_with_registry};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

#[derive(Clone)]
pub struct ExchangeServiceMetrics {
    pub stream_fragment_exchange_bytes: GenericCounterVec<AtomicU64>,
}

pub static GLOBAL_EXCHANGE_SERVICE_METRICS: LazyLock<ExchangeServiceMetrics> =
    LazyLock::new(|| ExchangeServiceMetrics::new(&GLOBAL_METRICS_REGISTRY));

impl ExchangeServiceMetrics {
    fn new(registry: &Registry) -> Self {
        let stream_fragment_exchange_bytes = register_int_counter_vec_with_registry!(
            "stream_exchange_frag_send_size",
            "Total size of messages that have been send to downstream Fragment",
            &["up_fragment_id", "down_fragment_id"],
            registry
        )
        .unwrap();

        Self {
            stream_fragment_exchange_bytes,
        }
    }
}
