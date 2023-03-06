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

use risingwave_common::config::RwConfig;
use risingwave_common::system_param::default_system_params;
use risingwave_common::system_param::reader::SystemParamsReader;

#[derive(Clone, Debug)]
pub struct StorageOpts {
    /// Target size of the Sstable.
    pub sstable_size_mb: u32,
    /// Size of each block in bytes in SST.
    pub block_size_kb: u32,
    /// False positive probability of bloom filter.
    pub bloom_false_positive: f64,
    /// parallelism while syncing share buffers into L0 SST. Should NOT be 0.
    pub share_buffers_sync_parallelism: u32,
    /// Worker threads number of dedicated tokio runtime for share buffer compaction. 0 means use
    /// tokio's default value (number of CPU core).
    pub share_buffer_compaction_worker_threads_number: u32,
    /// Maximum shared buffer size, writes attempting to exceed the capacity will stall until there
    /// is enough space.
    pub shared_buffer_capacity_mb: usize,
    /// Remote directory for storing data and metadata objects.
    pub data_directory: String,
    /// Whether to enable write conflict detection
    pub write_conflict_detection_enabled: bool,
    /// Capacity of sstable block cache.
    pub block_cache_capacity_mb: usize,
    /// Capacity of sstable meta cache.
    pub meta_cache_capacity_mb: usize,
    pub disable_remote_compactor: bool,
    pub enable_local_spill: bool,
    /// Local object store root. We should call `get_local_object_store` to get the object store.
    pub local_object_store: String,
    /// Number of tasks shared buffer can upload in parallel.
    pub share_buffer_upload_concurrency: usize,
    /// Capacity of sstable meta cache.
    pub compactor_memory_limit_mb: usize,
    /// Number of SST ids fetched from meta per RPC
    pub sstable_id_remote_fetch_number: u32,
    /// Whether to enable streaming upload for sstable.
    pub min_sst_size_for_streaming_upload: u64,
    /// Max sub compaction task numbers
    pub max_sub_compaction: u32,
    pub max_concurrent_compaction_task_number: u64,

    pub file_cache_dir: String,
    pub file_cache_capacity_mb: usize,
    pub file_cache_total_buffer_capacity_mb: usize,
    pub file_cache_file_fallocate_unit_mb: usize,
    pub file_cache_meta_fallocate_unit_mb: usize,
    pub file_cache_file_max_write_size_mb: usize,

    /// The storage url for storing backups.
    pub backup_storage_url: String,
    /// The storage directory for storing backups.
    pub backup_storage_directory: String,
}

impl Default for StorageOpts {
    fn default() -> Self {
        let c = RwConfig::default();
        let p = default_system_params();
        Self::from((&c, &p.into()))
    }
}

impl From<(&RwConfig, &SystemParamsReader)> for StorageOpts {
    fn from((c, p): (&RwConfig, &SystemParamsReader)) -> Self {
        Self {
            sstable_size_mb: p.sstable_size_mb(),
            block_size_kb: p.block_size_kb(),
            bloom_false_positive: p.bloom_false_positive(),
            share_buffers_sync_parallelism: c.storage.share_buffers_sync_parallelism,
            share_buffer_compaction_worker_threads_number: c
                .storage
                .share_buffer_compaction_worker_threads_number,
            shared_buffer_capacity_mb: c.storage.shared_buffer_capacity_mb,
            data_directory: p.data_directory().to_string(),
            write_conflict_detection_enabled: c.storage.write_conflict_detection_enabled,
            block_cache_capacity_mb: c.storage.block_cache_capacity_mb,
            meta_cache_capacity_mb: c.storage.meta_cache_capacity_mb,
            disable_remote_compactor: c.storage.disable_remote_compactor,
            enable_local_spill: c.storage.enable_local_spill,
            local_object_store: c.storage.local_object_store.to_string(),
            share_buffer_upload_concurrency: c.storage.share_buffer_upload_concurrency,
            compactor_memory_limit_mb: c.storage.compactor_memory_limit_mb,
            sstable_id_remote_fetch_number: c.storage.sstable_id_remote_fetch_number,
            min_sst_size_for_streaming_upload: c.storage.min_sst_size_for_streaming_upload,
            max_sub_compaction: c.storage.max_sub_compaction,
            max_concurrent_compaction_task_number: c.storage.max_concurrent_compaction_task_number,
            file_cache_dir: c.storage.file_cache.dir.clone(),
            file_cache_capacity_mb: c.storage.file_cache.capacity_mb,
            file_cache_total_buffer_capacity_mb: c.storage.file_cache.total_buffer_capacity_mb,
            file_cache_file_fallocate_unit_mb: c.storage.file_cache.cache_file_fallocate_unit_mb,
            file_cache_meta_fallocate_unit_mb: c.storage.file_cache.cache_meta_fallocate_unit_mb,
            file_cache_file_max_write_size_mb: c.storage.file_cache.cache_file_max_write_size_mb,
            backup_storage_url: p.backup_storage_url().to_string(),
            backup_storage_directory: p.backup_storage_directory().to_string(),
        }
    }
}
