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

use std::collections::btree_map::Range as BTreeMapRange;
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{self, *};
use std::ops::RangeBounds;
use std::sync::Arc;

use anyhow::anyhow;
use futures::{pin_mut, StreamExt};
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::CompactedRow;
use risingwave_common::types::{ScalarImpl, VIRTUAL_NODE_SIZE};
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::table::streaming_table::state_table::{
    prefix_range_to_memcomparable, StateTable,
};
use risingwave_storage::StateStore;

use crate::executor::error::StreamExecutorError;
use crate::executor::StreamExecutorResult;

type ScalarRange = (Bound<ScalarImpl>, Bound<ScalarImpl>);

/// The `RangeCache` caches a given range of `ScalarImpl` keys and corresponding rows.
/// It will evict keys from memory if it is above capacity and shrink its range.
/// Values not in range will have to be retrieved from storage.
pub struct RangeCache<S: StateStore> {
    /// {vnode -> {memcomparable_pk -> row}}
    cache: HashMap<u8, BTreeMap<Vec<u8>, CompactedRow>>,
    pub(crate) state_table: StateTable<S>,
    /// The current range stored in the cache.
    /// Any request for a set of values outside of this range will result in a scan
    /// from storage
    range: Option<ScalarRange>,

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
    pub fn insert(&mut self, k: &ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if let Some(r) = &self.range && r.contains(k) {
            let vnode = self.state_table.compute_vnode(&v);
            let vnode_entry = self.cache.entry(vnode).or_insert_with(BTreeMap::new);
            let pk = v.extract_memcomparable_by_indices(self.state_table.pk_serde(), self.state_table.pk_indices());
            vnode_entry.insert(pk, (&v).into());
        }
        self.state_table.insert(v);
        Ok(())
    }

    /// Delete a row and corresponding scalar value key from cache (if within range) and
    /// `StateTable`.
    // FIXME: panic instead of returning Err
    pub fn delete(&mut self, k: &ScalarImpl, v: Row) -> StreamExecutorResult<()> {
        if let Some(r) = &self.range && r.contains(k) {
            let vnode = self.state_table.compute_vnode(&v);
            let pk = v.extract_memcomparable_by_indices(self.state_table.pk_serde(), self.state_table.pk_indices());

            self.cache.get_mut(&vnode)
                .ok_or_else(|| StreamExecutorError::from(anyhow!("Deleting non-existent element")))?
                .remove(&pk)
                .ok_or_else(|| StreamExecutorError::from(anyhow!("Deleting non-existent element")))?;
        }
        self.state_table.delete(v);
        Ok(())
    }

    /// Return an iterator over sets of rows that satisfy the given range. Evicts entries if
    /// exceeding capacity based on whether the latest RHS value is the lower or upper bound of
    /// the range.
    pub async fn range(
        &mut self,
        range: ScalarRange,
        _latest_is_lower: bool,
    ) -> StreamExecutorResult<UnorderedRangeCacheIter<'_>> {
        // What we want: At the end of every epoch we will try to read
        // ranges based on the new value. The values in the range may not all be cached.
        //
        // If the new range is overlapping with the current range, we will keep the
        // current range. We will then evict to capacity after the cache has been populated
        // with the new range.
        //
        // If the new range is non-overlapping, we will delete the old range, and store
        // all elements from the new range.
        //
        // (TODO): We will always prefer to cache values that are closer to the latest value.
        //
        // If this requested range is too large, it will cause OOM. The `StateStore`
        // layer already buffers the entire output of a range scan in `Vec`, so there is
        // currently no workarond for OOM due to large range scans.

        let missing_ranges = if let Some(existing_range) = &self.range {
            let (ranges_to_fetch, new_range, delete_old) =
                get_missing_ranges(existing_range.clone(), range.clone());
            self.range = Some(new_range);
            if delete_old {
                self.cache = HashMap::new();
            }
            ranges_to_fetch
        } else {
            self.range = Some(range.clone());
            vec![range.clone()]
        };

        let to_row_bound = |bound: Bound<ScalarImpl>| -> Bound<Row> {
            match bound {
                Unbounded => Unbounded,
                Included(s) => Included(Row::new(vec![Some(s)])),
                Excluded(s) => Excluded(Row::new(vec![Some(s)])),
            }
        };

        let missing_ranges = missing_ranges
            .iter()
            .map(|(r0, r1)| (to_row_bound(r0.clone()), to_row_bound(r1.clone())));

        for pk_range in missing_ranges {
            for (vnode, b) in self.vnodes.iter().enumerate() {
                if b {
                    let vnode = vnode.try_into().unwrap();
                    // TODO: do this concurrently over each vnode.
                    let row_stream = self
                        .state_table
                        .iter_key_and_val_with_pk_range(&pk_range, vnode)
                        .await?;
                    pin_mut!(row_stream);

                    let map = self.cache.entry(vnode).or_insert_with(BTreeMap::new);
                    while let Some(res) = row_stream.next().await {
                        let (key_bytes, row) = res?;

                        map.insert(
                            key_bytes[VIRTUAL_NODE_SIZE..].to_vec(),
                            (row.as_ref()).into(),
                        );
                    }
                }
            }
        }

        let range = (to_row_bound(range.0), to_row_bound(range.1));
        let memcomparable_range =
            prefix_range_to_memcomparable(self.state_table.pk_serde(), &range);
        Ok(UnorderedRangeCacheIter::new(
            &self.cache,
            memcomparable_range,
            self.vnodes.clone(),
        ))
    }

    /// Updates the vnodes for `RangeCache`, purging the rows of the vnodes that are no longer
    /// owned.
    pub fn update_vnodes(&mut self, new_vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        let old_vnodes = self.state_table.update_vnode_bitmap(new_vnodes.clone());
        for (vnode, (old, new)) in old_vnodes.iter().zip_eq(new_vnodes.iter()).enumerate() {
            if old && !new {
                let vnode = vnode.try_into().unwrap();
                self.cache.remove(&vnode);
            }
        }
        self.vnodes = new_vnodes;
        old_vnodes
    }

    /// Flush writes to the `StateTable` from the in-memory buffer.
    pub async fn flush(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        // self.metrics.flush();
        self.state_table.commit(epoch).await?;
        Ok(())
    }
}

pub struct UnorderedRangeCacheIter<'a> {
    cache: &'a HashMap<u8, BTreeMap<Vec<u8>, CompactedRow>>,
    current_map: Option<&'a BTreeMap<Vec<u8>, CompactedRow>>,
    current_iter: Option<BTreeMapRange<'a, Vec<u8>, CompactedRow>>,
    vnodes: Arc<Bitmap>,
    next_vnode: u8,
    completed: bool,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
}

impl<'a> UnorderedRangeCacheIter<'a> {
    fn new(
        cache: &'a HashMap<u8, BTreeMap<Vec<u8>, CompactedRow>>,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        vnodes: Arc<Bitmap>,
    ) -> Self {
        let mut new = Self {
            cache,
            current_map: None,
            current_iter: None,
            next_vnode: 0,
            vnodes,
            range,
            completed: false,
        };
        new.refill_iterator();
        new
    }

    fn refill_iterator(&mut self) {
        loop {
            if self.vnodes.is_set(self.next_vnode as usize) && let Some(vnode_range) = self.cache.get(&self.next_vnode) {
                self.current_map = Some(vnode_range);
                self.current_iter = self.current_map.map(|m| m.range(self.range.clone()));
                return;
            } else if self.next_vnode == u8::MAX {
                // The iterator cannot be refilled further.
                self.completed = true;
                return;
            } else {
                self.next_vnode += 1;
            }
        }
    }
}

impl<'a> std::iter::Iterator for UnorderedRangeCacheIter<'a> {
    type Item = &'a CompactedRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.completed {
            None
        } else if let Some(iter) = &mut self.current_iter {
            let res = iter.next();
            if res.is_none() {
                if self.next_vnode == u8::MAX {
                    // The iterator cannot be refilled further.
                    self.completed = true;
                    None
                } else {
                    // Try to refill the iterator.
                    self.next_vnode += 1;
                    self.refill_iterator();
                    self.next()
                }
            } else {
                res.map(|r| r.1)
            }
        } else {
            panic!("Not completed but no iterator");
        }
    }
}

// This function returns three objects.
// 1. The ranges required to be fetched from cache.
// 2. The new range
// 3. Whether to delete the existing range.
fn get_missing_ranges(
    existing_range: ScalarRange,
    required_range: ScalarRange,
) -> (Vec<ScalarRange>, ScalarRange, bool) {
    let (existing_contains_lower, existing_contains_upper) =
        range_contains_lower_upper(&existing_range, &required_range.0, &required_range.1);

    if existing_contains_lower && existing_contains_upper {
        (vec![], existing_range.clone(), false)
    } else if existing_contains_lower {
        let lower = match existing_range.1 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        (
            vec![(lower, required_range.1.clone())],
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
            vec![(required_range.0.clone(), upper)],
            (required_range.0, existing_range.1),
            false,
        )
    } else if range_contains_lower_upper(&required_range, &existing_range.0, &existing_range.1)
        == (true, true)
    {
        let lower = match existing_range.0 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        let upper = match existing_range.1 {
            Included(s) => Excluded(s),
            Excluded(s) => Included(s),
            Unbounded => unreachable!(),
        };
        (
            vec![
                (required_range.0.clone(), lower),
                (upper, required_range.1.clone()),
            ],
            required_range,
            false,
        )
    } else {
        // The ranges are non-overlapping. So we delete the old range.
        (vec![required_range.clone()], required_range, true)
    }
}

// Returns whether the given range contains the lower and upper bounds respectively of another
// range.
fn range_contains_lower_upper(
    range: &ScalarRange,
    lower: &Bound<ScalarImpl>,
    upper: &Bound<ScalarImpl>,
) -> (bool, bool) {
    let contains_lower = match &lower {
        Excluded(s) => {
            let modified_lower = if let Excluded(x) = &range.0 {
                Included(x.clone())
            } else {
                range.0.clone()
            };
            let modified_upper = if let Included(x) = &range.1 {
                Excluded(x.clone())
            } else {
                range.1.clone()
            };
            (modified_lower, modified_upper).contains(s)
        }
        Included(s) => range.contains(s),
        Unbounded => matches!(range.0, Unbounded),
    };

    let contains_upper = match &upper {
        Excluded(s) => {
            let modified_lower = if let Included(x) = &range.0 {
                Excluded(x.clone())
            } else {
                range.0.clone()
            };
            let modified_upper = if let Excluded(x) = &range.1 {
                Included(x.clone())
            } else {
                range.1.clone()
            };
            (modified_lower, modified_upper).contains(s)
        }
        Included(s) => range.contains(s),
        Unbounded => matches!(range.1, Unbounded),
    };

    (contains_lower, contains_upper)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_missing_range() {
        // Overlapping ranges
        assert_eq!(
            get_missing_ranges(
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(100))
                ),
                (
                    Included(ScalarImpl::Int64(50)),
                    Included(ScalarImpl::Int64(150))
                ),
            ),
            (
                vec![(
                    Excluded(ScalarImpl::Int64(100)),
                    Included(ScalarImpl::Int64(150))
                )],
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
                false
            )
        );

        // Non-overlapping ranges
        assert_eq!(
            range_contains_lower_upper(
                &(
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(50))
                ),
                &Excluded(ScalarImpl::Int64(50)),
                &Included(ScalarImpl::Int64(150)),
            ),
            (false, false)
        );

        assert_eq!(
            get_missing_ranges(
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(50))
                ),
                (
                    Excluded(ScalarImpl::Int64(50)),
                    Included(ScalarImpl::Int64(150))
                ),
            ),
            (
                vec![(
                    Excluded(ScalarImpl::Int64(50)),
                    Included(ScalarImpl::Int64(150))
                )],
                (
                    Excluded(ScalarImpl::Int64(50)),
                    Included(ScalarImpl::Int64(150))
                ),
                true
            )
        );

        // Required contains existing
        assert_eq!(
            get_missing_ranges(
                (
                    Included(ScalarImpl::Int64(25)),
                    Excluded(ScalarImpl::Int64(50))
                ),
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
            ),
            (
                vec![
                    (
                        Included(ScalarImpl::Int64(0)),
                        Excluded(ScalarImpl::Int64(25))
                    ),
                    (
                        Included(ScalarImpl::Int64(50)),
                        Included(ScalarImpl::Int64(150))
                    )
                ],
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
                false,
            )
        );

        // Existing contains required
        assert_eq!(
            get_missing_ranges(
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
                (
                    Included(ScalarImpl::Int64(25)),
                    Excluded(ScalarImpl::Int64(50))
                ),
            ),
            (
                vec![],
                (
                    Included(ScalarImpl::Int64(0)),
                    Included(ScalarImpl::Int64(150))
                ),
                false,
            )
        );
    }

    #[test]
    fn test_dynamic_filter_range_cache_unordered_range_iter() {
        let cache = (0..=u8::MAX)
            .map(|x| {
                (
                    x,
                    vec![(vec![x], CompactedRow { row: vec![x] })]
                        .into_iter()
                        .collect::<BTreeMap<_, _>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        let range = (Unbounded, Unbounded);
        let vnodes = Arc::new(Bitmap::from_bytes(bytes::Bytes::from_static(
            &[u8::MAX; 32],
        ))); // set all the bits
        let mut iter = UnorderedRangeCacheIter::new(&cache, range, vnodes);
        for i in 0..=u8::MAX {
            assert_eq!(Some(&CompactedRow { row: vec![i] }), iter.next());
        }
        assert!(iter.next().is_none());
    }
}
