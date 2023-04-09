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

use prometheus::{
    exponential_buckets, histogram_opts, register_histogram_vec_with_registry,
    register_int_counter_with_registry, Histogram, IntCounter, Registry,
};

pub struct BackupManagerMetrics {
    pub job_count: IntCounter,
    pub job_latency_success: Histogram,
    pub job_latency_failure: Histogram,
}

impl BackupManagerMetrics {
    pub fn new(registry: Registry) -> Self {
        let job_count = register_int_counter_with_registry!(
            "backup_job_count",
            "total backup job count since meta node is started",
            registry,
        )
        .unwrap();
        let opts = histogram_opts!(
            "backup_job_latency",
            "latency of backup jobs since meta node is started",
            exponential_buckets(1.0, 1.5, 20).unwrap(),
        );
        let job_latency =
            register_histogram_vec_with_registry!(opts, &["state"], registry,).unwrap();
        let job_latency_success = job_latency
            .get_metric_with_label_values(&["success"])
            .unwrap();
        let job_latency_failure = job_latency
            .get_metric_with_label_values(&["failure"])
            .unwrap();
        Self {
            job_count,
            job_latency_success,
            job_latency_failure,
        }
    }
}
