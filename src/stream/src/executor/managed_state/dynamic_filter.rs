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

use std::sync::Arc;
use std::collections::btree_map::Range;
use std::collections::{BTreeMap, HashSet, HashMap};
use std::ops::Bound::{self, *};
use std::ops::RangeBounds;

use futures::{pin_mut, StreamExt};
use anyhow::anyhow;
use risingwave_common::buffer::Bitmap;
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
    cache: HashMap<u8, BTreeMap<ScalarImpl, HashSet<Row>>>,
    pub(crate) state_table: StateTable<S>,
    /// The current range stored in the cache.
    /// Any request for a set of values outside of this range will result in a scan
    /// from storage
    range: Option<(Bound<ScalarImpl>, Bound<ScalarImpl>)>,

    #[expect(dead_code)]
    num_rows_stored: usize,
    #[expect(dead_code)]
    capacity: usize,

    vnodes: Arc<Bitmap>,
}

impl<S: StateStore> RangeCache<S> {
    /// Create a [`RangeCache`] with given capacity and epoch
    pub fn new(state_table: StateTable<S>, capacity: usize, vnodes: Arc<Bitmap>) -> Self {
        Self {
            cache: HashMap::new(),
            state_table,
            range: None,
            num_rows_stored: 0,
            capacity,
            vnodes,
        }
    }

    pub fn init(&mut self, epoch: EpochPair) {
        self.state_table.init_epoch(epoch);
    }

    /// Insert a row and corresponding scalar value key into cache (if within range) and
    /// `StateTable`.
    pub fn insert(&mut self, k: ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if self.range.contains(&k) {
            let vnode = self.state_table.compute_vnode(&v);
            let vnode_entry = self.cache.entry(vnode).or_insert_with(BTreeMap::new);
            let entry = vnode_entry.entry(k).or_insert_with(HashSet::new);
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
            let vnode = self.state_table.compute_vnode(&v);
            let contains_element = self.cache.get_mut(&vnode)
                .ok_or_else(|| StreamExecutorError::from(anyhow!("Deleting non-existent element")))?
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
    pub async fn range(
        &self,
        range: (Bound<ScalarImpl>, Bound<ScalarImpl>),
        _latest_is_lower: bool,
    ) -> Range<'_, ScalarImpl, HashSet<Row>> {
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
        // currently no workarond for OOM due to large range scans.

        let missing_ranges = if let Some(existing_range) = &self.range {
            let (ranges_to_fetch, new_range, delete_old) =
                get_missing_ranges(existing_range, &range);
            self.range = Some(new_range);
            if delete_old {
                self.cache = HashMap::new();
            }
            ranges_to_fetch
        } else {
            self.range = Some(range);
            vec![range]
        };

        let to_row_bound = |bound: Bound<ScalarImpl>| -> Bound<Row> {
            match bound {
                Unbounded => Unbounded,
                Included(s) => Included(Row::new(vec![Some(s)])),
                Excluded(s) => Excluded(Row::new(vec![Some(s)])),
            }
        };

        let missing_ranges = missing_ranges.iter().map(|(r0, r1)| {
            (to_row_bound(r0), to_row_bound(r0))
        }).collect::<Vec<_>>();

        for pk_range in missing_ranges {
            for (vnode, b) in self.vnodes.iter().enumerate() {
                if b {
                    // TODO: error handle.
                    let row_stream = self.state_table.iter_key_and_val_with_pk_range(&pk_range, vnode.try_into().unwrap()).await.unwrap();
                    pin_mut!(row_stream);

                    let vnode = self.state_table.compute_vnode(&v);
                    let vnode_entry = self.cache.entry(vnode).or_insert_with(BTreeMap::new);
                    while let Some(row) = row_stream.next().await {
                        let entry = vnode_entry.entry(k).or_insert_with(HashSet::new);
                    }
                }
            }
        }
        self.cache.range(range)
    }

    /// Flush writes to the `StateTable` from the in-memory buffer.
    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // self.metrics.flush();
        self.state_table.commit(epoch).await?;
        Ok(())
    }
}

// This function returns three objects.
// 1. The ranges required to be fetched from cache.
// 2. The new range
// 3. Whether to delete the existing range.
fn get_missing_ranges(
    existing_range: &(Bound<ScalarImpl>, Bound<ScalarImpl>),
    required_range: &(Bound<ScalarImpl>, Bound<ScalarImpl>),
) -> Vec<(Bound<ScalarImpl>, Bound<ScalarImpl>)> {
    let (existing_contains_lower, existing_contains_upper) =
        range_contains_lower_upper(existing_range, &required_range.0, &required_range.1);

    if existing_contains_lower && existing_contains_upper {
        (vec![], existing_range, false)
    } else if existing_contains_lower {
        let lower = match existing_range.1 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        (
            vec![(lower, required_range.1)],
            (existing_range.0, required_range.1),
            false,
        )
    } else if existing_contains_upper {
        let upper = match existing_range.0 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        (
            vec![(required_range.0, upper)],
            (required_range.0, existing_range.1),
            false,
        )
    } else {
        if range_contains_lower_upper(required_range, &existing_range.0, &existing_range.1)
            == (true, true)
        {
            let lower = match existing_range.1 {
                Included(s) => Excluded(s),
                Excluded(s) => Included(s),
                Unbounded => unreachable!(),
            };
            let upper = match existing_range.0 {
                Included(s) => Excluded(s),
                Excluded(s) => Included(s),
                Unbounded => unreachable!(),
            };
            (
                vec![(required_range.0, lower), (upper, required_range.1)],
                required_range.clone(),
                false,
            )
        } else {
            // The ranges are non-overlapping. So we delete the old range.
            (vec![required_range.clone()], required_range.clone(), true)
        }
    }
}

fn range_contains_lower_upper(
    range: &(Bound<ScalarImpl>, Bound<ScalarImpl>),
    lower: &Bound<ScalarImpl>,
    upper: &Bound<ScalarImpl>,
) -> (bool, bool) {
    let contains_lower = match &lower {
        Excluded(s) => {
            let modified_lower = if let Excluded(x) = range.0 {
                Included(x)
            } else {
                range.0
            };
            (modified_lower, range.1).contains(s)
        }
        Included(s) => range.contains(s),
        Unbounded => matches!(range.0, Unbounded),
    };

    let contains_upper = match &upper {
        Excluded(s) => {
            let modified_upper = if let Excluded(x) = range.1 {
                Included(x)
            } else {
                range.1
            };
            (range.0, modified_lower).contains(s)
        }
        Included(s) => range.contains(s),
        Unbounded => matches!(range.1, Unbounded),
    };

    (contains_lower, contains_upper)
}
