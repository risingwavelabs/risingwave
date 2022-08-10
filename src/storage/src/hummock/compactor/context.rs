use std::sync::Arc;

use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManagerRef;
use risingwave_rpc_client::HummockMetaClient;

use crate::hummock::compactor::CompactionExecutor;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{MemoryLimiter, SstableIdManagerRef};
use crate::monitor::StateStoreMetrics;

/// A `CompactorContext` describes the context of a compactor.
#[derive(Clone)]
pub struct CompactorContext {
    /// Storage configurations.
    pub options: Arc<StorageConfig>,

    /// The meta client.
    pub hummock_meta_client: Arc<dyn HummockMetaClient>,

    /// Sstable store that manages the sstables.
    pub sstable_store: SstableStoreRef,

    /// Statistics.
    pub stats: Arc<StateStoreMetrics>,

    /// True if it is a memory compaction (from shared buffer).
    pub is_share_buffer_compact: bool,

    pub compaction_executor: Option<Arc<CompactionExecutor>>,

    pub filter_key_extractor_manager: FilterKeyExtractorManagerRef,

    pub memory_limiter: Arc<MemoryLimiter>,

    pub sstable_id_manager: SstableIdManagerRef,
}
