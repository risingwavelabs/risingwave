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

use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::DefaultOrdered;
use crate::common::cache::TopNStateCache;

/// INSERT
///    A. Cache evicted. Update cached value.
///    B. Cache uninitialized. Initialize cache, insert into TopNCache.
///    C. Cache not empty. Insert into TopNCache.
///
/// DELETE
///    A. Matches lowest value pk. Remove lowest value. Mark cache as Evicted.
///       Later on Barrier we will refresh the cache with table scan.
///       Since on barrier we will clean up all values before watermark,
///       We have less rows to scan.
///    B. Does not match. Do nothing.
///
/// UPDATE
///    A. Do delete then insert.
///
/// BARRIER
///    State table commit. See below.
///
/// STATE TABLE COMMIT
///    Update `prev_cleaned_watermark`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum WatermarkCacheState {
    /// All values deleted. Need refresh cache on barrier.
    Evicted,
    /// Either no lowest values, or some lowest values.
    Ready,
}

type WatermarkCacheKey = DefaultOrdered<OwnedRow>;

#[derive(Clone)]
pub(crate) struct StateTableWatermarkCache {
    inner: TopNStateCache<WatermarkCacheKey, ()>,
}

impl StateTableWatermarkCache {
    pub fn new() -> Self {
        Self {
            inner: TopNStateCache::new(100), // TODO: This number is arbitrary
        }
    }
    /// Handle inserts / updates.
    fn handle_insert(&mut self, row: impl Row, watermark_col_idx: &Option<usize>) {
        todo!()
    }
}

