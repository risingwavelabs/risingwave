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

// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use crate::hummock::MemoryLimiter;

pub trait FilterBuilder: Send {
    /// add key which need to be filter for construct filter data.
    fn add_key(&mut self, dist_key: &[u8], table_id: u32);
    /// Builds Bloom filter from key hashes
    fn finish(&mut self, memory_limiter: Option<Arc<MemoryLimiter>>) -> Vec<u8>;
    /// approximate memory of filter builder
    fn approximate_len(&self) -> usize;

    fn create(fpr: f64, capacity: usize) -> Self;
    fn switch_block(&mut self, _memory_limiter: Option<Arc<MemoryLimiter>>) {}
    /// approximate memory when finish filter
    fn approximate_building_memory(&self) -> usize;

    /// Add raw data which build by keys directly. Please make sure that you have finished the last
    /// block by calling `switch_block`
    fn add_raw_data(&mut self, _raw: Vec<u8>) {}

    fn support_blocked_raw_data(&self) -> bool {
        false
    }
}
