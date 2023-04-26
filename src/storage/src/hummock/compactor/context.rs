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

use std::sync::Arc;

use risingwave_hummock_sdk::compact::CompactorRuntimeConfig;
use risingwave_rpc_client::HummockMetaClient;

use super::task_progress::TaskProgressManagerRef;
use crate::filter_key_extractor::FilterKeyExtractorManagerRef;
use crate::hummock::compactor::CompactionExecutor;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{MemoryLimiter, SstableObjectIdManagerRef};
use crate::monitor::CompactorMetrics;
use crate::opts::StorageOpts;

/// A `CompactorContext` describes the context of a compactor.
#[derive(Clone)]
pub struct CompactorContext {
    /// Storage options.
    pub storage_opts: Arc<StorageOpts>,

    /// The meta client.
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,

    /// Sstable store that manages the sstables.
    pub sstable_store: SstableStoreRef,

    /// Statistics.
    pub compactor_metrics: Arc<CompactorMetrics>,

    /// True if it is a memory compaction (from shared buffer).
    pub is_share_buffer_compact: bool,

    pub compaction_executor: Arc<CompactionExecutor>,

    pub filter_key_extractor_manager: FilterKeyExtractorManagerRef,

    pub output_memory_limiter: Arc<MemoryLimiter>,

    pub sstable_object_id_manager: SstableObjectIdManagerRef,

    pub task_progress_manager: TaskProgressManagerRef,

    pub compactor_runtime_config: Arc<tokio::sync::Mutex<CompactorRuntimeConfig>>,
}

impl CompactorContext {
    pub fn new_local_compact_context(
        storage_opts: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        compactor_metrics: Arc<CompactorMetrics>,
        sstable_object_id_manager: SstableObjectIdManagerRef,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
        compactor_runtime_config: CompactorRuntimeConfig,
    ) -> Self {
        let compaction_executor = if storage_opts.share_buffer_compaction_worker_threads_number == 0
        {
            Arc::new(CompactionExecutor::new(None))
        } else {
            Arc::new(CompactionExecutor::new(Some(
                storage_opts.share_buffer_compaction_worker_threads_number as usize,
            )))
        };
        // not limit memory for local compact
        let memory_limiter = MemoryLimiter::unlimit();
        Self {
            storage_opts,
            hummock_meta_client,
            sstable_store,
            compactor_metrics,
            is_share_buffer_compact: true,
            compaction_executor,
            filter_key_extractor_manager,
            output_memory_limiter: memory_limiter,
            sstable_object_id_manager,
            task_progress_manager: Default::default(),
            compactor_runtime_config: Arc::new(tokio::sync::Mutex::new(compactor_runtime_config)),
        }
    }

    pub async fn lock_config(&self) -> tokio::sync::MutexGuard<'_, CompactorRuntimeConfig> {
        self.compactor_runtime_config.lock().await
    }
}
