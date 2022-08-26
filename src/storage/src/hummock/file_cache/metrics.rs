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

use std::sync::Arc;

use prometheus::{
    register_counter_with_registry, register_histogram_with_registry,
    register_int_counter_with_registry, Counter, Histogram, IntCounter, Registry,
};

pub struct FileCacheMetrics {
    pub disk_read_throughput: Counter,
    pub disk_read_latency: Histogram,
    pub disk_write_throughput: Counter,
    pub disk_write_latency: Histogram,

    pub insert_latency: Histogram,
    pub erase_latency: Histogram,
    pub get_latency: Histogram,

    pub cache_miss_counter: IntCounter,
}

impl FileCacheMetrics {
    pub fn new(registry: Registry) -> Self {
        Self {
            disk_read_throughput: register_counter_with_registry!(
                "file_cache_disk_read_throughput",
                "file cache disk read throughput",
                registry,
            )
            .unwrap(),
            disk_read_latency: register_histogram_with_registry!(
                "file_cache_disk_read_latency",
                "file cache disk read latency",
                vec![0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0],
                registry,
            )
            .unwrap(),
            disk_write_throughput: register_counter_with_registry!(
                "file_cache_disk_write_throughput",
                "file cache disk write throughput",
                registry,
            )
            .unwrap(),
            disk_write_latency: register_histogram_with_registry!(
                "file_cache_disk_write_latency",
                "file cache disk write latency",
                vec![0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0],
                registry,
            )
            .unwrap(),
            insert_latency: register_histogram_with_registry!(
                "file_cache_insert_latency",
                "file cache insert latency",
                vec![0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0],
                registry,
            )
            .unwrap(),
            erase_latency: register_histogram_with_registry!(
                "file_cache_erase_latency",
                "file cache erase latency",
                vec![0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0],
                registry,
            )
            .unwrap(),
            get_latency: register_histogram_with_registry!(
                "file_cache_get_latency",
                "file cache get latency",
                vec![0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0],
                registry,
            )
            .unwrap(),
            cache_miss_counter: register_int_counter_with_registry!(
                "file_cache_miss",
                "file cache miss",
                registry,
            )
            .unwrap(),
        }
    }
}

pub type FileCacheMetricsRef = Arc<FileCacheMetrics>;
