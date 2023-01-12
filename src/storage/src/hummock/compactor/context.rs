// Copyright 2023 Singularity Data
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

use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compact::CompactorRuntimeConfig;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_rpc_client::HummockMetaClient;

use super::task_progress::TaskProgressManagerRef;
use crate::hummock::compactor::{CompactionExecutor, CompactorSstableStoreRef};
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{MemoryLimiter, SstableIdManagerRef};
use crate::monitor::CompactorMetrics;

/// A `CompactorContext` describes the context of a compactor.
#[derive(Clone)]
pub struct Context {
    /// Storage configurations.
    pub options: Arc<StorageConfig>,

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

    pub read_memory_limiter: Arc<MemoryLimiter>,

    pub sstable_id_manager: SstableIdManagerRef,

    pub task_progress_manager: TaskProgressManagerRef,
}

impl Context {
    pub fn new_local_compact_context(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        compactor_metrics: Arc<CompactorMetrics>,
        sstable_id_manager: SstableIdManagerRef,
        filter_key_extractor_manager: FilterKeyExtractorManagerRef,
    ) -> Self {
        let compaction_executor = if options.share_buffer_compaction_worker_threads_number == 0 {
            Arc::new(CompactionExecutor::new(None))
        } else {
            Arc::new(CompactionExecutor::new(Some(
                options.share_buffer_compaction_worker_threads_number as usize,
            )))
        };
        // not limit memory for local compact
        let memory_limiter = MemoryLimiter::unlimit();
        Context {
            options,
            hummock_meta_client,
            sstable_store,
            compactor_metrics,
            is_share_buffer_compact: true,
            compaction_executor,
            filter_key_extractor_manager,
            read_memory_limiter: memory_limiter,
            sstable_id_manager,
            task_progress_manager: Default::default(),
        }
    }
}
#[derive(Clone)]
pub struct CompactorContext {
    pub context: Arc<Context>,
    pub sstable_store: CompactorSstableStoreRef,
    config: Arc<tokio::sync::Mutex<CompactorRuntimeConfig>>,
}

impl CompactorContext {
    pub fn new(context: Arc<Context>, sstable_store: CompactorSstableStoreRef) -> Self {
        Self::with_config(
            context,
            sstable_store,
            CompactorRuntimeConfig {
                max_concurrent_task_number: u64::MAX,
            },
        )
    }

    pub fn with_config(
        context: Arc<Context>,
        sstable_store: CompactorSstableStoreRef,
        config: CompactorRuntimeConfig,
    ) -> Self {
        Self {
            context,
            sstable_store,
            config: Arc::new(tokio::sync::Mutex::new(config)),
        }
    }

    pub async fn lock_config(&self) -> tokio::sync::MutexGuard<'_, CompactorRuntimeConfig> {
        self.config.lock().await
    }
}
