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

use prometheus::core::AtomicU64;
use prometheus::{register_int_gauge_with_registry, IntGauge, Registry};

pub struct RdKafkaStats {
    pub registry: Registry,

    pub top_ts: IntGauge,
    pub top_time: IntGauge,
}

impl RdKafkaStats {
    pub fn new(registry: Registry) -> Self {
        let top_ts = register_int_gauge_with_registry!(
            "rdkafka.top.ts",
            "librdkafka's internal monotonic clock (microseconds)",
            &["source_name", "client_id", "type"],
            registry
        )
        .unwrap();
        let top_time = register_int_gauge_with_registry!(
            "rdkafka.top.time",
            "Wall clock time in seconds since the epoch",
            &["source_name", "client_id", "type"],
            registry
        )
        .unwrap();
        RdKafkaStats {
            registry,
            top_ts,
            top_time,
        }
    }
}
