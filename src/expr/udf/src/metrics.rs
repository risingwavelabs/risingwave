// Copyright 2024 RisingWave Labs
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
    exponential_buckets, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, HistogramVec, IntCounterVec, Registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

/// Monitor metrics for UDF.
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Number of successful UDF calls.
    pub udf_success_count: IntCounterVec,
    /// Number of failed UDF calls.
    pub udf_failure_count: IntCounterVec,
    /// Input chunk rows of UDF calls.
    pub udf_input_chunk_rows: HistogramVec,
    /// The latency of UDF calls in seconds.
    pub udf_latency: HistogramVec,
    /// Total number of input rows of UDF calls.
    pub udf_input_rows: IntCounterVec,
    /// Total number of input bytes of UDF calls.
    pub udf_input_bytes: IntCounterVec,
}

/// Global UDF metrics.
pub static GLOBAL_METRICS: LazyLock<Metrics> =
    LazyLock::new(|| Metrics::new(&GLOBAL_METRICS_REGISTRY));

impl Metrics {
    fn new(registry: &Registry) -> Self {
        let labels = &["link", "name", "actor_id", "fragment_id"];
        let udf_success_count = register_int_counter_vec_with_registry!(
            "udf_success_count",
            "Total number of successful UDF calls",
            labels,
            registry
        )
        .unwrap();
        let udf_failure_count = register_int_counter_vec_with_registry!(
            "udf_failure_count",
            "Total number of failed UDF calls",
            labels,
            registry
        )
        .unwrap();
        let udf_input_chunk_rows = register_histogram_vec_with_registry!(
            "udf_input_chunk_rows",
            "Input chunk rows of UDF calls",
            labels,
            exponential_buckets(1.0, 2.0, 10).unwrap(), // 1 to 1024
            registry
        )
        .unwrap();
        let udf_latency = register_histogram_vec_with_registry!(
            "udf_latency",
            "The latency(s) of UDF calls",
            labels,
            exponential_buckets(0.000001, 2.0, 30).unwrap(), // 1us to 1000s
            registry
        )
        .unwrap();
        let udf_input_rows = register_int_counter_vec_with_registry!(
            "udf_input_rows",
            "Total number of input rows of UDF calls",
            labels,
            registry
        )
        .unwrap();
        let udf_input_bytes = register_int_counter_vec_with_registry!(
            "udf_input_bytes",
            "Total number of input bytes of UDF calls",
            labels,
            registry
        )
        .unwrap();

        Metrics {
            udf_success_count,
            udf_failure_count,
            udf_input_chunk_rows,
            udf_latency,
            udf_input_rows,
            udf_input_bytes,
        }
    }
}
