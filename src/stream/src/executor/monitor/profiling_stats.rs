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

use std::sync::atomic::Ordering;

use risingwave_common::monitor::in_mem::GuardedCount;

use crate::executor::monitor::StreamingMetrics;

pub enum ProfileMetricsImpl {
    NoopProfileMetrics,
    ProfileMetrics(ProfileMetrics),
}

impl ProfileMetricsImpl {
    pub fn new(
        executor_id: u64,
        stats: &StreamingMetrics,
        enable_profiling: bool,
    ) -> ProfileMetricsImpl {
        if enable_profiling {
            ProfileMetricsImpl::ProfileMetrics(ProfileMetrics {
                stream_node_output_row_count: stats
                    .mem_stream_node_output_row_count
                    .new_count(executor_id),
                stream_node_output_blocking_duration_ms: stats
                    .mem_stream_node_output_blocking_duration_ms
                    .new_count(executor_id),
            })
        } else {
            ProfileMetricsImpl::NoopProfileMetrics
        }
    }
}

pub struct ProfileMetrics {
    pub stream_node_output_row_count: GuardedCount,
    pub stream_node_output_blocking_duration_ms: GuardedCount,
}

pub trait ProfileMetricsExt {
    fn inc_row_count(&self, count: u64);
    fn inc_blocking_duration_ms(&self, duration: u64);
}

impl ProfileMetricsExt for ProfileMetrics {
    fn inc_row_count(&self, count: u64) {
        self.stream_node_output_row_count
            .count
            .fetch_add(count, Ordering::Relaxed);
    }

    fn inc_blocking_duration_ms(&self, duration_ms: u64) {
        self.stream_node_output_blocking_duration_ms
            .count
            .fetch_add(duration_ms, Ordering::Relaxed);
    }
}

impl ProfileMetricsExt for ProfileMetricsImpl {
    fn inc_row_count(&self, count: u64) {
        match self {
            ProfileMetricsImpl::NoopProfileMetrics => {}
            ProfileMetricsImpl::ProfileMetrics(metrics) => metrics.inc_row_count(count),
        }
    }

    fn inc_blocking_duration_ms(&self, duration: u64) {
        match self {
            ProfileMetricsImpl::NoopProfileMetrics => {}
            ProfileMetricsImpl::ProfileMetrics(metrics) => {
                metrics.inc_blocking_duration_ms(duration)
            }
        }
    }
}
