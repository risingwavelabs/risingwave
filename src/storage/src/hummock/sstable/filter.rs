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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use risingwave_pb::hummock::PbSstableFilterType;

use crate::hummock::MemoryLimiter;

pub const DEFAULT_FILTER_HASH_PREALLOC_KEY_COUNT_CAP: usize = 256 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct FilterBuilderOptions {
    /// Estimated key count for one output SST.
    pub estimated_key_count: usize,
    /// Estimated data block count for one output SST.
    pub estimated_block_count: usize,
    /// Maximum initial allocation for the key-hash buffer.
    ///
    /// Plain filters use this as the upper bound for `Vec<u64>::with_capacity`. Blocked filters use
    /// their own smaller per-block bound for the current block-local filter.
    pub hash_prealloc_key_count_cap: usize,
}

pub trait FilterBuilder: Send {
    /// add key which need to be filter for construct filter data.
    fn add_key(&mut self, dist_key: &[u8], table_id: u32);
    /// Builds serialized filter bytes from key hashes.
    fn finish(&mut self, memory_limiter: Option<Arc<MemoryLimiter>>) -> Vec<u8>;
    /// Approximate serialized filter bytes counted toward SST builder capacity.
    ///
    /// `SstableBuilder::reach_capacity` uses this value to decide when to seal the current
    /// SST. It should track the filter bytes that would be appended to the SST if the builder
    /// were finished now, including any pending block-local filter bytes. It is not a heap memory
    /// estimate for the builder or the decoded filter reader. Use `approximate_building_memory`
    /// for build-time temporary memory.
    fn approximate_len(&self) -> usize;

    fn create(options: FilterBuilderOptions) -> Self;
    fn switch_block(&mut self, _memory_limiter: Option<Arc<MemoryLimiter>>) {}
    /// Approximate temporary memory needed when finishing the filter.
    fn approximate_building_memory(&self) -> usize;

    /// Add raw data which build by keys directly. Please make sure that you have finished the last
    /// block by calling `switch_block`
    fn add_raw_data(&mut self, _raw: Vec<u8>) {}

    fn support_blocked_raw_data(&self) -> bool {
        false
    }

    fn filter_type(&self) -> PbSstableFilterType;
}
