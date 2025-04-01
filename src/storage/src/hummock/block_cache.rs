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

use std::ops::Deref;
use std::sync::Arc;

use await_tree::{InstrumentAwait, SpanExt};
use foyer::{FetchState, HybridCacheEntry, HybridFetch};
use risingwave_common::config::EvictionConfig;

use super::{Block, HummockResult, SstableBlockIndex};
use crate::hummock::HummockError;

type HybridCachedBlockEntry = HybridCacheEntry<SstableBlockIndex, Box<Block>>;

enum BlockEntry {
    HybridCache(#[allow(dead_code)] HybridCachedBlockEntry),
    Owned(#[allow(dead_code)] Box<Block>),
    RefEntry(#[allow(dead_code)] Arc<Block>),
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
    Entry(HybridFetch<SstableBlockIndex, Box<Block>>),
}

impl BlockResponse {
    pub async fn wait(self) -> HummockResult<BlockHolder> {
        let entry = match self {
            BlockResponse::Block(block) => return Ok(block),
            BlockResponse::Entry(entry) => entry,
        };
        match entry.state() {
            FetchState::Hit => entry
                .await
                .map(BlockHolder::from_hybrid_cache_entry)
                .map_err(HummockError::foyer_error),
            FetchState::Wait => entry
                .instrument_await("wait_pending_fetch_block".verbose())
                .await
                .map(BlockHolder::from_hybrid_cache_entry)
                .map_err(HummockError::foyer_error),
            FetchState::Miss => entry
                .instrument_await("fetch_block".verbose())
                .await
                .map(BlockHolder::from_hybrid_cache_entry)
                .map_err(HummockError::foyer_error),
        }
    }
}
