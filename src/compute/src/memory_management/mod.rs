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

// Only enable the non-trivial policies on Linux as it relies on statistics from `jemalloc-ctl`
// which might be inaccurate on other platforms.
#[cfg(target_os = "linux")]
pub mod policy;

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use risingwave_batch::task::BatchManager;
use risingwave_common::config::{StorageConfig, StorageMemoryConfig};
use risingwave_common::error::Result;
use risingwave_stream::task::LocalStreamManager;

/// The minimal memory requirement of computing tasks in megabytes.
pub const MIN_COMPUTE_MEMORY_MB: usize = 512;
/// The memory reserved for system usage (stack and code segment of processes, allocation
/// overhead, network buffer, etc.) in megabytes.
pub const MIN_SYSTEM_RESERVED_MEMORY_MB: usize = 512;
pub const MAX_SYSTEM_RESERVED_MEMORY_MB: usize = 2048;
pub const SYSTEM_RESERVED_MEMORY_PROPORTION: f64 = 0.1;

pub const STORAGE_MEMORY_PROPORTION: f64 = 0.3;

pub const COMPACTOR_MEMORY_PROPORTION: f64 = 0.1;

pub const STORAGE_BLOCK_CACHE_MEMORY_PROPORTION: f64 = 0.3;

pub const STORAGE_META_CACHE_MAX_MEMORY_MB: usize = 4096;
pub const STORAGE_META_CACHE_MEMORY_PROPORTION: f64 = 0.35;
pub const STORAGE_SHARED_BUFFER_MEMORY_PROPORTION: f64 = 0.3;
pub const STORAGE_FILE_CACHE_MEMORY_PROPORTION: f64 = 0.05;
pub const STORAGE_DEFAULT_HIGH_PRIORITY_BLOCK_CACHE_RATIO: usize = 70;

/// `MemoryControlStats` contains the state from previous control loop
#[derive(Default)]
pub struct MemoryControlStats {
    pub jemalloc_allocated_mib: usize,
    pub lru_watermark_step: u64,
    pub lru_watermark_time_ms: u64,
    pub lru_physical_now_ms: u64,
}

pub type MemoryControlRef = Box<dyn MemoryControl>;

pub trait MemoryControl: Send + Sync + std::fmt::Debug {
    fn apply(
        &self,
        interval_ms: u32,
        prev_memory_stats: MemoryControlStats,
        batch_manager: Arc<BatchManager>,
        stream_manager: Arc<LocalStreamManager>,
        watermark_epoch: Arc<AtomicU64>,
    ) -> MemoryControlStats;
}

#[cfg(target_os = "linux")]
pub fn build_memory_control_policy(total_memory_bytes: usize) -> Result<MemoryControlRef> {
    use self::policy::JemallocMemoryControl;

    Ok(Box::new(JemallocMemoryControl::new(total_memory_bytes)))
}

#[cfg(not(target_os = "linux"))]
pub fn build_memory_control_policy(_total_memory_bytes: usize) -> Result<MemoryControlRef> {
    // We disable memory control on operating systems other than Linux now because jemalloc
    // stats do not work well.
    tracing::warn!("memory control is only enabled on Linux now");
    Ok(Box::new(DummyPolicy))
}

/// `DummyPolicy` is used for operarting systems other than Linux. It does nothing as memory control
/// is disabled on non-Linux OS.
#[derive(Debug)]
pub struct DummyPolicy;

impl MemoryControl for DummyPolicy {
    fn apply(
        &self,
        _interval_ms: u32,
        _prev_memory_stats: MemoryControlStats,
        _batch_manager: Arc<BatchManager>,
        _stream_manager: Arc<LocalStreamManager>,
        _watermark_epoch: Arc<AtomicU64>,
    ) -> MemoryControlStats {
        MemoryControlStats::default()
    }
}

/// Each compute node reserves some memory for stack and code segment of processes, allocation
/// overhead, network buffer, etc. based on `SYSTEM_RESERVED_MEMORY_PROPORTION`. The reserve memory
/// size belongs to [`MIN_SYSTEM_RESERVED_MEMORY_MB`, `MAX_SYSTEM_RESERVED_MEMORY_MB`]
pub fn reserve_memory_bytes(total_memory_bytes: usize) -> (usize, usize) {
    let reserved = std::cmp::min(
        std::cmp::max(
            (total_memory_bytes as f64 * SYSTEM_RESERVED_MEMORY_PROPORTION).ceil() as usize,
            MIN_SYSTEM_RESERVED_MEMORY_MB << 20,
        ),
        MAX_SYSTEM_RESERVED_MEMORY_MB << 20,
    );
    (reserved, total_memory_bytes - reserved)
}

/// Decide the memory limit for each storage cache. If not specified in `StorageConfig`, memory
/// limits are calculated based on the proportions to total `non_reserved_memory_bytes`.
pub fn storage_memory_config(
    non_reserved_memory_bytes: usize,
    embedded_compactor_enabled: bool,
    storage_config: &StorageConfig,
) -> StorageMemoryConfig {
    let (storage_memory_proportion, compactor_memory_proportion) = if embedded_compactor_enabled {
        (STORAGE_MEMORY_PROPORTION, COMPACTOR_MEMORY_PROPORTION)
    } else {
        (STORAGE_MEMORY_PROPORTION + COMPACTOR_MEMORY_PROPORTION, 0.0)
    };
    let mut block_cache_capacity_mb = storage_config.block_cache_capacity_mb.unwrap_or(
        ((non_reserved_memory_bytes as f64
            * storage_memory_proportion
            * STORAGE_BLOCK_CACHE_MEMORY_PROPORTION)
            .ceil() as usize)
            >> 20,
    );
    let high_priority_ratio_in_percent = storage_config
        .high_priority_ratio_in_percent
        .unwrap_or(STORAGE_DEFAULT_HIGH_PRIORITY_BLOCK_CACHE_RATIO);
    let default_meta_cache_capacity = (non_reserved_memory_bytes as f64
        * storage_memory_proportion
        * STORAGE_META_CACHE_MEMORY_PROPORTION)
        .ceil() as usize;
    let meta_cache_capacity_mb = storage_config
        .meta_cache_capacity_mb
        .unwrap_or(std::cmp::min(
            default_meta_cache_capacity >> 20,
            STORAGE_META_CACHE_MAX_MEMORY_MB,
        ));
    if meta_cache_capacity_mb == STORAGE_META_CACHE_MAX_MEMORY_MB {
        block_cache_capacity_mb += (default_meta_cache_capacity >> 20) - meta_cache_capacity_mb;
    }
    let shared_buffer_capacity_mb = storage_config.shared_buffer_capacity_mb.unwrap_or(
        ((non_reserved_memory_bytes as f64
            * storage_memory_proportion
            * STORAGE_SHARED_BUFFER_MEMORY_PROPORTION)
            .ceil() as usize)
            >> 20,
    );
    let file_cache_total_buffer_capacity_mb = storage_config
        .file_cache
        .total_buffer_capacity_mb
        .unwrap_or(
            ((non_reserved_memory_bytes as f64
                * storage_memory_proportion
                * STORAGE_FILE_CACHE_MEMORY_PROPORTION)
                .ceil() as usize)
                >> 20,
        );
    let compactor_memory_limit_mb = storage_config.compactor_memory_limit_mb.unwrap_or(
        ((non_reserved_memory_bytes as f64 * compactor_memory_proportion).ceil() as usize) >> 20,
    );

    StorageMemoryConfig {
        block_cache_capacity_mb,
        meta_cache_capacity_mb,
        shared_buffer_capacity_mb,
        file_cache_total_buffer_capacity_mb,
        compactor_memory_limit_mb,
        high_priority_ratio_in_percent,
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::config::StorageConfig;

    use super::{reserve_memory_bytes, storage_memory_config};

    #[test]
    fn test_reserve_memory_bytes() {
        // at least 512 MB
        let (reserved, non_reserved) = reserve_memory_bytes(2 << 30);
        assert_eq!(reserved, 512 << 20);
        assert_eq!(non_reserved, 1536 << 20);

        // reserve based on proportion
        let (reserved, non_reserved) = reserve_memory_bytes(10 << 30);
        assert_eq!(reserved, 1 << 30);
        assert_eq!(non_reserved, 9 << 30);

        // at most 2 GB
        let (reserved, non_reserved) = reserve_memory_bytes(100 << 30);
        assert_eq!(reserved, 2 << 30);
        assert_eq!(non_reserved, 98 << 30);
    }

    #[test]
    fn test_storage_memory_config() {
        let mut storage_config = StorageConfig::default();
        let total_non_reserved_memory_bytes = 8 << 30;

        let memory_config =
            storage_memory_config(total_non_reserved_memory_bytes, true, &storage_config);
        assert_eq!(memory_config.block_cache_capacity_mb, 737);
        assert_eq!(memory_config.meta_cache_capacity_mb, 860);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 737);
        assert_eq!(memory_config.file_cache_total_buffer_capacity_mb, 122);
        assert_eq!(memory_config.compactor_memory_limit_mb, 819);

        storage_config.block_cache_capacity_mb = Some(512);
        storage_config.meta_cache_capacity_mb = Some(128);
        storage_config.shared_buffer_capacity_mb = Some(1024);
        storage_config.file_cache.total_buffer_capacity_mb = Some(128);
        storage_config.compactor_memory_limit_mb = Some(512);
        let memory_config = storage_memory_config(0, true, &storage_config);
        assert_eq!(memory_config.block_cache_capacity_mb, 512);
        assert_eq!(memory_config.meta_cache_capacity_mb, 128);
        assert_eq!(memory_config.shared_buffer_capacity_mb, 1024);
        assert_eq!(memory_config.file_cache_total_buffer_capacity_mb, 128);
        assert_eq!(memory_config.compactor_memory_limit_mb, 512);
    }
}
