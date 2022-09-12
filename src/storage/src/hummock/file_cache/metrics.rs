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
    register_counter_vec_with_registry, register_histogram_vec_with_registry,
    register_int_counter_with_registry, Counter, Histogram, IntCounter, Registry,
};

pub struct FileCacheMetrics {
    pub cache_miss: IntCounter,

    pub disk_read_bytes: Counter,
    pub disk_read_latency: Histogram,
    pub disk_write_bytes: Counter,
    pub disk_write_latency: Histogram,
    pub disk_read_io_size: Histogram,
    pub disk_write_io_size: Histogram,

    pub insert_latency: Histogram,
    pub erase_latency: Histogram,
    pub get_latency: Histogram,
}

impl FileCacheMetrics {
    pub fn new(registry: Registry) -> Self {
        let latency = register_histogram_vec_with_registry!(
            "file_cache_latency",
            "file cache latency",
            &["op"],
            vec![
                0.0001, 0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
                1.0
            ],
            registry,
        )
        .unwrap();
        let disk_throughput = register_counter_vec_with_registry!(
            "file_cache_disk_bytes",
            "file cache disk bytes",
            &["op"],
            registry,
        )
        .unwrap();
        let disk_latency = register_histogram_vec_with_registry!(
            "file_cache_disk_latency",
            "file cache disk latency",
            &["op"],
            vec![
                0.0001, 0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75,
                1.0
            ],
            registry,
        )
        .unwrap();
        let disk_io_size = register_histogram_vec_with_registry!(
            "file_cache_disk_io_size",
            "file cache disk io size",
            &["op"],
            vec![
                0.1 * 1024.0 * 1024.0,
                0.5 * 1024.0 * 1024.0,
                1.0 * 1024.0 * 1024.0,
                2.0 * 1024.0 * 1024.0,
                4.0 * 1024.0 * 1024.0,
                8.0 * 1024.0 * 1024.0,
                16.0 * 1024.0 * 1024.0,
                64.0 * 1024.0 * 1024.0,
            ],
            registry,
        )
        .unwrap();
        let cache_miss =
            register_int_counter_with_registry!("file_cache_miss", "file cache miss", registry)
                .unwrap();
        let disk_read_throughput = disk_throughput
            .get_metric_with_label_values(&["read"])
            .unwrap();
        let disk_read_latency = disk_latency
            .get_metric_with_label_values(&["read"])
            .unwrap();
        let disk_write_throughput = disk_throughput
            .get_metric_with_label_values(&["write"])
            .unwrap();
        let disk_write_latency = disk_latency
            .get_metric_with_label_values(&["write"])
            .unwrap();
        let disk_read_io_size = disk_io_size
            .get_metric_with_label_values(&["read"])
            .unwrap();
        let disk_write_io_size = disk_io_size
            .get_metric_with_label_values(&["write"])
            .unwrap();
        let insert_latency = latency.get_metric_with_label_values(&["insert"]).unwrap();
        let erase_latency = latency.get_metric_with_label_values(&["erase"]).unwrap();
        let get_latency = latency.get_metric_with_label_values(&["get"]).unwrap();

        Self {
            cache_miss,
            disk_read_bytes: disk_read_throughput,
            disk_read_latency,
            disk_write_bytes: disk_write_throughput,
            disk_write_latency,
            disk_read_io_size,
            disk_write_io_size,
            insert_latency,
            erase_latency,
            get_latency,
        }
    }
}

pub type FileCacheMetricsRef = Arc<FileCacheMetrics>;
