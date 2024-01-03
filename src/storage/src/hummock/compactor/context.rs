// Copyright 2024 RisingWave Labs
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

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use more_asserts::assert_ge;
use parking_lot::RwLock;

use super::task_progress::TaskProgressManagerRef;
use crate::hummock::compactor::CompactionExecutor;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::MemoryLimiter;
use crate::monitor::CompactorMetrics;
use crate::opts::StorageOpts;

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

    pub await_tree_reg: Option<Arc<RwLock<await_tree::Registry<String>>>>,

    pub running_task_parallelism: Arc<AtomicU32>,

    pub max_task_parallelism: Arc<AtomicU32>,
}

impl CompactorContext {
    pub fn new_local_compact_context(
        storage_opts: Arc<StorageOpts>,
        sstable_store: SstableStoreRef,
        compactor_metrics: Arc<CompactorMetrics>,
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
            await_tree_reg: None,
            running_task_parallelism: Arc::new(AtomicU32::new(0)),
            max_task_parallelism: Arc::new(AtomicU32::new(u32::MAX)),
        }
    }

    pub fn acquire_task_quota(&self, parallelism: u32) -> bool {
        let mut running_u32 = self.running_task_parallelism.load(Ordering::SeqCst);
        let max_u32 = self.max_task_parallelism.load(Ordering::SeqCst);

        while parallelism + running_u32 <= max_u32 {
            match self.running_task_parallelism.compare_exchange(
                running_u32,
                running_u32 + parallelism,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return true;
                }
                Err(old_running_u32) => {
                    running_u32 = old_running_u32;
                }
            }
        }

        false
    }

    pub fn release_task_quota(&self, parallelism: u32) {
        let prev = self
            .running_task_parallelism
            .fetch_sub(parallelism, Ordering::SeqCst);

        assert_ge!(
            prev,
            parallelism,
            "running {} parallelism {}",
            prev,
            parallelism
        );
    }

    pub fn get_free_quota(&self) -> u32 {
        let running_u32 = self.running_task_parallelism.load(Ordering::SeqCst);
        let max_u32 = self.max_task_parallelism.load(Ordering::SeqCst);

        if max_u32 > running_u32 {
            max_u32 - running_u32
        } else {
            0
        }
    }
}
