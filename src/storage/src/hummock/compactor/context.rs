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

use std::sync::Arc;

use super::task_progress::TaskProgressManagerRef;
use crate::hummock::MemoryLimiter;
use crate::hummock::compactor::CompactionExecutor;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::monitor::CompactorMetrics;
use crate::opts::StorageOpts;

pub type CompactionAwaitTreeRegRef = await_tree::Registry;

pub fn new_compaction_await_tree_reg_ref(config: await_tree::Config) -> CompactionAwaitTreeRegRef {
    await_tree::Registry::new(config)
}

pub mod await_tree_key {
    /// Await-tree key type for compaction tasks.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Compaction {
        CompactRunner { task_id: u64, split_index: usize },
        CompactSharedBuffer { id: usize },
        SpawnUploadTask { id: usize },
        MergingTask { id: usize },
    }

    pub use Compaction::*;
}

/// A `CompactorContext` describes the context of a compactor.
#[derive(Clone)]
pub struct CompactorContext {
    /// Storage options.
    pub storage_opts: Arc<StorageOpts>,

    /// Sstable store that manages the sstables.
    pub sstable_store: SstableStoreRef,

    /// Statistics.
    pub compactor_metrics: Arc<CompactorMetrics>,

    /// True if it is a memory compaction (from shared buffer).
    pub is_share_buffer_compact: bool,

    pub compaction_executor: Arc<CompactionExecutor>,

    pub memory_limiter: Arc<MemoryLimiter>,

    pub task_progress_manager: TaskProgressManagerRef,

    pub await_tree_reg: Option<CompactionAwaitTreeRegRef>,
}

impl CompactorContext {
    pub fn new_local_compact_context(
        storage_opts: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        compactor_metrics: Arc<CompactorMetrics>,
        await_tree_reg: Option<CompactionAwaitTreeRegRef>,
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
        Self {
            storage_opts,
            sstable_store,
            compactor_metrics,
            is_share_buffer_compact: true,
            compaction_executor,
            memory_limiter: MemoryLimiter::unlimit(),
            task_progress_manager: Default::default(),
            await_tree_reg,
        }
    }
}
