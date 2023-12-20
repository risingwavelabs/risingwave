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

use std::sync::LazyLock;

use prometheus::{
    exponential_buckets, register_histogram_with_registry, register_int_counter_with_registry,
    Histogram, IntCounter, Registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

/// Monitor metrics for expressions.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Number of successful UDF calls.
    pub udf_success_count: IntCounter,
    /// Number of failed UDF calls.
    pub udf_failure_count: IntCounter,
    /// Input chunk rows of UDF calls.
    pub udf_input_chunk_rows: Histogram,
    /// The latency of UDF calls in seconds.
    pub udf_latency: Histogram,
}

pub static GLOBAL_EXPR_METRICS: LazyLock<Metrics> =
    LazyLock::new(|| Metrics::new(&GLOBAL_METRICS_REGISTRY));

impl Metrics {
    fn new(registry: &Registry) -> Self {
        let udf_success_count = register_int_counter_with_registry!(
            "expr_udf_success_count",
            "Total number of successful UDF calls",
            registry
        )
        .unwrap();
        let udf_failure_count = register_int_counter_with_registry!(
            "expr_udf_failure_count",
            "Total number of failed UDF calls",
            registry
        )
        .unwrap();
        let udf_input_chunk_rows = register_histogram_with_registry!(
            "expr_udf_input_chunk_rows",
            "Input chunk rows of UDF calls",
            exponential_buckets(1.0, 2.0, 10).unwrap(), // 1 to 1024
            registry
        )
        .unwrap();
        let udf_latency = register_histogram_with_registry!(
            "expr_udf_latency",
            "The latency(s) of UDF calls",
            exponential_buckets(0.000001, 2.0, 30).unwrap(), // 1us to 1000s
            registry
        )
        .unwrap();

        Metrics {
            udf_success_count,
            udf_failure_count,
            udf_input_chunk_rows,
            udf_latency,
        }
    }
}
