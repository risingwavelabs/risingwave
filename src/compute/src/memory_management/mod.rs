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

pub mod memory_manager;
#[cfg(target_os = "linux")]
pub mod policy;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use risingwave_batch::task::BatchManager;
use risingwave_common::error::Result;
use risingwave_stream::task::LocalStreamManager;

use crate::ComputeNodeOpts;

/// `MemoryControlStats` contains the necessary information for memory control, including both batch
/// and streaming.
#[derive(Default)]
pub struct MemoryControlStats {
    pub batch_memory_usage: usize,
    pub streaming_memory_usage: usize,
    pub jemalloc_allocated_mib: usize,
    pub lru_watermark_step: u64,
    pub lru_watermark_time_ms: u64,
    pub lru_physical_now_ms: u64,
}

pub type MemoryControlPolicy = Box<dyn MemoryControl>;

pub trait MemoryControl: Send + Sync {
    fn apply(
        &self,
        total_compute_memory_bytes: usize,
        barrier_interval_ms: u32,
        prev_memory_stats: MemoryControlStats,
        batch_manager: Arc<BatchManager>,
        stream_manager: Arc<LocalStreamManager>,
        watermark_epoch: Arc<AtomicU64>,
    ) -> MemoryControlStats;

    fn describe(&self, total_compute_memory_bytes: usize) -> String;
}

#[cfg(target_os = "linux")]
pub fn memory_control_policy_from_config(opts: &ComputeNodeOpts) -> Result<MemoryControlPolicy> {
    use anyhow::anyhow;

    use self::policy::{FixedProportionPolicy, StreamingOnlyPolicy};

    let input_policy = &opts.memory_control_policy;
    if input_policy == FixedProportionPolicy::CONFIG_STR {
        Ok(Box::new(FixedProportionPolicy::new(
            opts.streaming_memory_proportion,
        )?))
    } else if input_policy == StreamingOnlyPolicy::CONFIG_STR {
        Ok(Box::new(StreamingOnlyPolicy))
    } else {
        let valid_values = [
            FixedProportionPolicy::CONFIG_STR,
            StreamingOnlyPolicy::CONFIG_STR,
        ];
        Err(anyhow!(format!(
            "invalid memory control policy in configuration: {}, valid values: {:?}",
            input_policy, valid_values,
        ))
        .into())
    }
}

#[cfg(not(target_os = "linux"))]
pub fn memory_control_policy_from_config(_opts: &ComputeNodeOpts) -> Result<MemoryControlPolicy> {
    // We disable memory control on operating systems other than Linux now because jemalloc
    // stats do not work well.
    tracing::warn!("memory control is only enabled on Linux now");
    Ok(Box::new(DummyPolicy))
}

/// `DummyPolicy` is used for operarting systems other than Linux. It does nothing as memory control
/// is disabled on non-Linux OS.
pub struct DummyPolicy;

impl MemoryControl for DummyPolicy {
    fn apply(
        &self,
        _total_compute_memory_bytes: usize,
        _barrier_interval_ms: u32,
        _prev_memory_stats: MemoryControlStats,
        _batch_manager: Arc<BatchManager>,
        _stream_manager: Arc<LocalStreamManager>,
        _watermark_epoch: Arc<AtomicU64>,
    ) -> MemoryControlStats {
        MemoryControlStats::default()
    }

    fn describe(&self, _total_compute_memory_bytes: usize) -> String {
        "DummyPolicy".to_string()
    }
}
