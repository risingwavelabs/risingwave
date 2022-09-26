// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::btree_map::Range;
use std::collections::{BTreeMap, HashSet};
use std::ops::Bound::{self, *};
use std::ops::RangeBounds;

use anyhow::anyhow;
use risingwave_common::array::Row;
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::executor::error::StreamExecutorError;
use crate::executor::StreamExecutorResult;

/// The `RangeCache` caches a given range of `ScalarImpl` keys and corresponding rows.
/// It will evict keys from memory if it is above capacity and shrink its range.
/// Values not in range will have to be retrieved from storage.
pub struct RangeCache<S: StateStore> {
    // TODO: It could be potentially expensive memory-wise to store `HashSet`.
    //       The memory overhead per single row is potentially a backing Vec of size 4
    //       (See: https://github.com/rust-lang/hashbrown/pull/162)
    //       + some byte-per-entry metadata. Well, `Row` is on heap anyway...
    //
    //       It could be preferred to find a way to do prefix range scans on the left key and
    //       storing as `BTreeSet<(ScalarImpl, Row)>`.
    //       We could solve it if `ScalarImpl` had a successor/predecessor function.
    cache: BTreeMap<ScalarImpl, HashSet<Row>>,
    pub(crate) state_table: StateTable<S>,
    /// The current range stored in the cache.
    /// Any request for a set of values outside of this range will result in a scan
    /// from storage
    range: (Bound<ScalarImpl>, Bound<ScalarImpl>),

    #[allow(unused)]
    num_rows_stored: usize,
    #[allow(unused)]
    capacity: usize,
}

impl<S: StateStore> RangeCache<S> {
    /// Create a [`RangeCache`] with given capacity and epoch
    pub fn new(state_table: StateTable<S>, capacity: usize) -> Self {
        Self {
            cache: BTreeMap::new(),
            state_table,
            range: (Unbounded, Unbounded),
            num_rows_stored: 0,
            capacity,
        }
    }

    pub fn init(&mut self, epoch: EpochPair) {
        self.state_table.init_epoch(epoch);
    }

    /// Insert a row and corresponding scalar value key into cache (if within range) and
    /// `StateTable`.
    pub fn insert(&mut self, k: ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if self.range.contains(&k) {
            let entry = self.cache.entry(k).or_insert_with(HashSet::new);
            entry.insert(v.clone());
        }
        self.state_table.insert(v);
        Ok(())
    }

    /// Delete a row and corresponding scalar value key from cache (if within range) and
    /// `StateTable`.
    // FIXME: panic instead of returning Err
    pub fn delete(&mut self, k: &ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if self.range.contains(k) {
            let contains_element = self
                .cache
                .get_mut(k)
                .ok_or_else(|| StreamExecutorError::from(anyhow!("Deleting non-existent element")))?
                .remove(&v);

            if !contains_element {
                return Err(StreamExecutorError::from(anyhow!(
                    "Deleting non-existent element"
                )));
            };
        }
        self.state_table.delete(v);
        Ok(())
    }

    /// Return an iterator over sets of rows that satisfy the given range. Evicts entries if
    /// exceeding capacity based on whether the latest RHS value is the lower or upper bound of
    /// the range.
    pub fn range(
        &self,
        range: (Bound<ScalarImpl>, Bound<ScalarImpl>),
        _latest_is_lower: bool,
    ) -> Range<'_, ScalarImpl, HashSet<Row>> {
        // TODO (cache behaviour):
        // What we want: At the end of every epoch we will try to read
        // ranges based on the new value. The values in the range may not all be cached.
        //
        // If the new range is overlapping with the current range, we will keep the
        // current range. We will then evict to capacity after the cache has been populated
        // with the new range.
        //
        // If the new range is non-overlapping, we will delete the old range, and store
        // `self.capacity` elements from the new range.
        //
        // We will always prefer to cache values that are closer to the latest value.
        //
        // If this requested range is too large, it will cause OOM. The `StateStore`
        // layer already buffers the entire output of a range scan in `Vec`, so there is
        // currently no workarond for OOM due to large range scasns.
        //
        // --------------------------------------------------------------------
        //
        // For overlapping ranges, we will prevent double inserts,
        // preferring the fresher in-cache value
        //
        // let lower_fetch_range: Option<ScalarRange>) = match self.range.0 {
        //     Unbounded => None,
        //     Included(x) | Excluded(x) => match range.0 {
        //         Unbounded => (Unbounded, Included(x)),
        //         bound @ Included(y) | Excluded(y) => if y
        //         Included(y) | Excluded(y) => x <= y,
        //     },
        //     Excluded(x) =>
        // }

        self.cache.range(range)
    }

    /// Flush writes to the `StateTable` from the in-memory buffer.
    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // self.metrics.flush();
        self.state_table.commit(epoch).await?;
        Ok(())
    }
}
