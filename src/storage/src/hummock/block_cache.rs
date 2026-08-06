// Copyright 2022 RisingWave Labs
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

use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use arc_swap::ArcSwap;
use await_tree::{InstrumentAwait, SpanExt};
use foyer::{
    HybridCache, HybridCacheEntry, HybridGetOrFetch, Statistics, StorageFilterCondition,
    StorageFilterResult,
};
use risingwave_common::config::EvictionConfig;
use risingwave_hummock_sdk::version::LocalHummockVersion;
use risingwave_hummock_sdk::{HummockObjectId, HummockSstableObjectId};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh64::xxh64;

use super::{Block, HummockResult};
use crate::hummock::HummockError;

const BLOCK_INDEX_BITS: u32 = 16;
const BLOCK_INDEX_LIMIT: u64 = 1 << BLOCK_INDEX_BITS;
// The POC keeps the full SST object ID under the current 32-bit deployment bound. Bit 63 marks
// keys that cannot use the extractable layout, so the live-SST filter rejects them fail-closed.
const SST_ID_LIMIT: u64 = 1 << 32;
const INVALID_HASH_BIT: u64 = 1 << 63;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct SstableBlockIndex {
    pub sst_id: HummockSstableObjectId,
    pub block_idx: u64,
}

impl SstableBlockIndex {
    fn encoded_hash(&self) -> u64 {
        let sst_id = self.sst_id.as_raw_id();
        if sst_id < SST_ID_LIMIT && self.block_idx < BLOCK_INDEX_LIMIT {
            return (sst_id << BLOCK_INDEX_BITS) | self.block_idx;
        }

        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&sst_id.to_le_bytes());
        bytes[8..].copy_from_slice(&self.block_idx.to_le_bytes());
        INVALID_HASH_BIT | (xxh64(&bytes, 0) & !INVALID_HASH_BIT)
    }
}

impl Hash for SstableBlockIndex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.encoded_hash());
    }
}

#[derive(Clone, Debug, Default)]
pub struct SstableBlockHasher {
    hash: u64,
}

impl Hasher for SstableBlockHasher {
    fn finish(&self) -> u64 {
        self.hash
    }

    fn write(&mut self, bytes: &[u8]) {
        self.hash = match <&[u8; 8]>::try_from(bytes) {
            Ok(bytes) => u64::from_ne_bytes(*bytes),
            Err(_) => INVALID_HASH_BIT | (xxh64(bytes, 0) & !INVALID_HASH_BIT),
        };
    }

    fn write_u64(&mut self, value: u64) {
        self.hash = value;
    }
}

pub type SstableBlockHashBuilder = BuildHasherDefault<SstableBlockHasher>;
pub type SstableBlockCache = HybridCache<SstableBlockIndex, Box<Block>, SstableBlockHashBuilder>;
type HybridCachedBlockEntry =
    HybridCacheEntry<SstableBlockIndex, Box<Block>, SstableBlockHashBuilder>;
type HybridBlockGetOrFetch =
    HybridGetOrFetch<SstableBlockIndex, Box<Block>, SstableBlockHashBuilder>;

#[derive(Clone)]
pub struct LiveSsts {
    inner: Arc<ArcSwap<HashSet<HummockObjectId>>>,
}

impl Default for LiveSsts {
    fn default() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(HashSet::new())),
        }
    }
}

impl std::fmt::Debug for LiveSsts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveSsts")
            .field("len", &self.inner.load().len())
            .finish()
    }
}

impl LiveSsts {
    pub fn replace_from_version(&self, version: &LocalHummockVersion) {
        self.replace(version.get_object_ids());
    }

    fn replace(&self, object_ids: impl IntoIterator<Item = HummockObjectId>) {
        let next = HashSet::from_iter(object_ids);
        if self.inner.load().as_ref() != &next {
            self.inner.store(Arc::new(next));
        }
    }

    fn contains(&self, object_id: &HummockObjectId) -> bool {
        self.inner.load().contains(object_id)
    }
}

#[derive(Clone, Debug)]
pub struct LiveSstFilter {
    live_ssts: LiveSsts,
}

impl LiveSstFilter {
    pub fn new(live_ssts: LiveSsts) -> Self {
        Self { live_ssts }
    }

    fn is_admitted(&self, hash: u64) -> bool {
        if hash & INVALID_HASH_BIT != 0 {
            return false;
        }
        self.live_ssts
            .contains(&HummockObjectId::Sstable(HummockSstableObjectId::new(
                hash >> BLOCK_INDEX_BITS,
            )))
    }
}

impl StorageFilterCondition for LiveSstFilter {
    fn filter(
        &self,
        _stats: &Arc<Statistics>,
        hash: u64,
        _estimated_size: usize,
    ) -> StorageFilterResult {
        if self.is_admitted(hash) {
            StorageFilterResult::Admit
        } else {
            StorageFilterResult::Reject
        }
    }
}

pub enum BlockEntry {
    HybridCache(HybridCachedBlockEntry),
    Owned(Box<Block>),
    RefEntry(Arc<Block>),
}

pub struct BlockHolder {
    _handle: BlockEntry,
    pub block: *const Block,
}

impl BlockHolder {
    pub fn from_ref_block(block: Arc<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::RefEntry(block),
            block: ptr,
        }
    }

    pub fn from_owned_block(block: Box<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::Owned(block),
            block: ptr,
        }
    }

    pub fn from_hybrid_cache_entry(entry: HybridCachedBlockEntry) -> Self {
        let ptr = entry.value().as_ref() as *const _;
        Self {
            _handle: BlockEntry::HybridCache(entry),
            block: ptr,
        }
    }

    pub fn entry(&self) -> &BlockEntry {
        &self._handle
    }
}

impl Deref for BlockHolder {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.block) }
    }
}

unsafe impl Send for BlockHolder {}
unsafe impl Sync for BlockHolder {}

#[derive(Debug)]
pub struct BlockCacheConfig {
    pub capacity: usize,
    pub shard_num: usize,
    pub eviction: EvictionConfig,
}

pub enum BlockResponse {
    Block(BlockHolder),
    Fetch(HybridBlockGetOrFetch),
}

impl BlockResponse {
    pub async fn wait(self) -> HummockResult<BlockHolder> {
        let fetch = match self {
            BlockResponse::Block(block) => return Ok(block),
            BlockResponse::Fetch(fetch) => fetch,
        };
        let fetch = match fetch.try_unwrap() {
            Ok(entry) => return Ok(BlockHolder::from_hybrid_cache_entry(entry)),
            Err(fetch) => fetch,
        };
        fetch
            .instrument_await("wait_pending_fetch_block".verbose())
            .await
            .map(BlockHolder::from_hybrid_cache_entry)
            .map_err(HummockError::foyer_error)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::hash::BuildHasher;
    use std::sync::Arc;

    use risingwave_hummock_sdk::level::{LevelCommon, LevelsCommon};
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
    use risingwave_hummock_sdk::version::{HummockVersion, LocalHummockVersion};
    use risingwave_hummock_sdk::{HummockObjectId, HummockSstableObjectId, HummockVersionId};

    use super::{
        BLOCK_INDEX_LIMIT, INVALID_HASH_BIT, LiveSstFilter, LiveSsts, SST_ID_LIMIT,
        SstableBlockHashBuilder, SstableBlockIndex,
    };

    fn hash(key: SstableBlockIndex) -> u64 {
        SstableBlockHashBuilder::default().hash_one(key)
    }

    fn version_with_ssts(sst_ids: &[u64]) -> LocalHummockVersion {
        let table_infos: Vec<SstableInfo> = sst_ids
            .iter()
            .map(|sst_id| {
                SstableInfoInner {
                    object_id: (*sst_id).into(),
                    sst_id: (*sst_id + 1000).into(),
                    ..Default::default()
                }
                .into()
            })
            .collect();
        let level = LevelCommon::<SstableInfo> {
            level_idx: 0,
            level_type: Default::default(),
            table_infos,
            total_file_size: 0,
            sub_level_id: 0,
            uncompressed_file_size: 0,
            vnode_partition_count: 0,
        };
        let mut levels = LevelsCommon::<SstableInfo>::default();
        levels.l0.sub_levels.push(level);
        let mut version = HummockVersion::default();
        version.id = HummockVersionId::new(1);
        version.levels = HashMap::from([(1.into(), levels)]);
        version.into()
    }

    #[test]
    fn test_live_sst_filter() {
        let live_ssts = LiveSsts::default();
        let filter = LiveSstFilter::new(live_ssts.clone());
        let live = HummockSstableObjectId::new(7);
        let stale = HummockSstableObjectId::new(8);

        assert!(!filter.is_admitted(hash(SstableBlockIndex {
            sst_id: live,
            block_idx: 3,
        })));

        assert_eq!(
            hash(SstableBlockIndex {
                sst_id: live,
                block_idx: 3,
            }),
            (live.as_raw_id() << super::BLOCK_INDEX_BITS) | 3
        );

        live_ssts.replace([HummockObjectId::Sstable(live)]);
        assert!(filter.is_admitted(hash(SstableBlockIndex {
            sst_id: live,
            block_idx: 3,
        })));
        assert!(!filter.is_admitted(hash(SstableBlockIndex {
            sst_id: stale,
            block_idx: 3,
        })));
    }

    #[test]
    fn test_live_ssts_replace_from_version() {
        let live_ssts = LiveSsts::default();
        let filter = LiveSstFilter::new(live_ssts.clone());
        let sst_7 = SstableBlockIndex {
            sst_id: HummockSstableObjectId::new(7),
            block_idx: 0,
        };
        let sst_8 = SstableBlockIndex {
            sst_id: HummockSstableObjectId::new(8),
            block_idx: 0,
        };

        live_ssts.replace_from_version(&version_with_ssts(&[7, 8]));
        let first = live_ssts.inner.load_full();
        live_ssts.replace_from_version(&version_with_ssts(&[7, 8]));
        assert!(Arc::ptr_eq(&first, &live_ssts.inner.load_full()));
        assert!(filter.is_admitted(hash(sst_7)));
        assert!(filter.is_admitted(hash(sst_8)));

        live_ssts.replace_from_version(&version_with_ssts(&[8]));
        assert!(!filter.is_admitted(hash(sst_7)));
        assert!(filter.is_admitted(hash(sst_8)));
    }

    #[test]
    fn test_sstable_block_hash_bounds_fail_closed() {
        let block_overflow = hash(SstableBlockIndex {
            sst_id: HummockSstableObjectId::new(7),
            block_idx: BLOCK_INDEX_LIMIT,
        });
        let sst_overflow = hash(SstableBlockIndex {
            sst_id: HummockSstableObjectId::new(SST_ID_LIMIT),
            block_idx: 0,
        });

        assert_ne!(block_overflow & INVALID_HASH_BIT, 0);
        assert_ne!(sst_overflow & INVALID_HASH_BIT, 0);
    }
}
