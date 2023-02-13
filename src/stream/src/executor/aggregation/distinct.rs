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

use std::collections::HashMap;
use std::marker::PhantomData;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayImpl, Op, Vis, VisRef};
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::ScalarImpl;
use risingwave_storage::StateStore;

use super::AggCall;
use crate::common::table::state_table::StateTable;
use crate::executor::StreamExecutorResult;

/// Deduplicater for one distinct column.
struct Deduplicater<S: StateStore> {
    agg_call_indices: Vec<usize>,
    _phantom: PhantomData<S>,
}

impl<S: StateStore> Deduplicater<S> {
    fn new(indices_and_calls: Vec<(usize, &AggCall)>) -> Self {
        let agg_call_indices = indices_and_calls
            .into_iter()
            .map(|(call_idx, _)| call_idx)
            .collect();
        Self {
            agg_call_indices,
            _phantom: PhantomData,
        }
    }

    /// Get the indices of agg calls that distinct on this column.
    /// The index is the position of the agg call in the original agg call list.
    fn agg_call_indices(&self) -> &[usize] {
        &self.agg_call_indices
    }

    /// Update the `visibilities` of distinct agg calls that distinct on the `column`,
    /// according to the counts of distinct keys for each call.
    ///
    /// * `ops`: Ops for each datum in `column`.
    /// * `column`: The column to distinct on.
    /// * `visibilities` - Visibilities for agg calls that distinct on the this column.
    /// * `dedup_table` - The deduplication table for this distinct column.
    async fn dedup(
        &mut self,
        ops: &[Op],
        column: &ArrayImpl,
        mut visibilities: Vec<&mut Vis>,
        dedup_table: &mut StateTable<S>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<()> {
        assert_eq!(visibilities.len(), self.agg_call_indices.len());
        println!("[rc] column: {:?}", column);

        // TODO(rc): move to field of `Deduplicater`
        let mut cache = HashMap::new();
        let mut old_rows = HashMap::new();

        // inverted masks for visibilities, 1 means hidden, 0 means visible
        let mut vis_masks_inv = (0..visibilities.len())
            .map(|_| BitmapBuilder::zeroed(column.len()))
            .collect_vec();
        for (datum_idx, (op, datum)) in ops.iter().zip(column.iter()).enumerate() {
            // get counts of the distinct key of all agg calls that distinct on this column
            let counts = if let Some(counts) = cache.get_mut(&datum) {
                counts
            } else {
                let counts_row: Option<OwnedRow> = dedup_table
                    .get_row(group_key.chain(row::once(datum)))
                    .await?;
                println!(
                    "[rc] load counts for distinct key {:?} from dedup_table: {:?}",
                    datum, counts_row
                );
                let counts = counts_row.map_or_else(
                    || vec![0; self.agg_call_indices.len()],
                    |r| {
                        old_rows.insert(datum, r.clone());
                        r.iter()
                            .map(|d| if let Some(d) = d { d.into_int64() } else { 0 })
                            .collect()
                    },
                );
                println!("[rc] initial counts: {:?}", counts);
                cache.insert(datum, counts);
                cache.get_mut(&datum).unwrap()
            };
            debug_assert_eq!(counts.len(), visibilities.len());

            match op {
                Op::Insert | Op::UpdateInsert => {
                    // iterate over vis of each distinct agg call, count up for visible datum
                    for (i, vis) in visibilities.iter().enumerate() {
                        if vis.is_set(datum_idx) {
                            counts[i] += 1;
                            println!("[rc] count up for datum {:?}, count: {}", datum, counts[i]);
                            if counts[i] > 1 {
                                // duplicate, hide this one
                                vis_masks_inv[i].set(datum_idx, true);
                            }
                        }
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    // iterate over vis of each distinct agg call, count down for visible datum
                    for (i, vis) in visibilities.iter().enumerate() {
                        if vis.is_set(datum_idx) {
                            counts[i] -= 1;
                            println!(
                                "[rc] count down for datum {:?}, count: {}",
                                datum, counts[i]
                            );
                            debug_assert!(counts[i] >= 0);
                            if counts[i] > 0 {
                                // still exists at least one duplicate, hide this one
                                vis_masks_inv[i].set(datum_idx, true);
                            }
                        }
                    }
                }
            }
        }

        println!("[rc] count cache: {:?}", cache);
        println!("[rc] change set: {:?}", old_rows);
        cache.into_iter().for_each(|(key, counts)| {
            let new_row = group_key.chain(row::once(key)).chain(OwnedRow::new(
                counts.into_iter().map(ScalarImpl::from).map(Some).collect(),
            ));
            if let Some(old_row) = old_rows.remove(&key) {
                dedup_table.update(group_key.chain(row::once(key)).chain(old_row), new_row)
            } else {
                dedup_table.insert(new_row)
            }
        });

        for (vis, vis_mask_inv) in visibilities.iter_mut().zip(vis_masks_inv.into_iter()) {
            let mask = !vis_mask_inv.finish();
            if !mask.all() {
                // update visibility is needed
                **vis = vis.as_ref() & VisRef::from(&mask);
            }
        }
        println!("[rc] new visibilities: {:?}", visibilities);

        Ok(())
    }

    /// Flush the deduplication table.
    fn flush(&self, _dedup_table: &mut StateTable<S>) {
        // TODO(rc): now we flush the table in `dedup` method.
    }
}

/// # Safety
///
/// There must not be duplicate items in `indices`.
unsafe fn get_many_mut_from_slice<'a, T>(slice: &'a mut [T], indices: &[usize]) -> Vec<&'a mut T> {
    let mut res = Vec::with_capacity(indices.len());
    let ptr = slice.as_mut_ptr();
    for &idx in indices {
        res.push(&mut *ptr.add(idx));
    }
    res
}

pub struct DistinctDeduplicater<S: StateStore> {
    /// Key: distinct column index, value: deduplicater for the column.
    deduplicaters: HashMap<usize, Deduplicater<S>>,

    _phantom: PhantomData<S>,
}

impl<S: StateStore> DistinctDeduplicater<S> {
    pub fn new(agg_calls: &[AggCall]) -> Self {
        let deduplicaters: HashMap<_, _> = agg_calls
            .iter()
            .enumerate()
            .filter(|(_, call)| call.distinct) // only distinct agg calls need dedup table
            .into_group_map_by(|(_, call)| call.args.val_indices()[0])
            .into_iter()
            .map(|(k, v)| (k, Deduplicater::new(v)))
            .collect();
        Self {
            deduplicaters,
            _phantom: PhantomData,
        }
    }

    pub async fn dedup_chunk(
        &mut self,
        ops: &[Op],
        columns: &[Column],
        visibilities: Vec<Option<Bitmap>>,
        dedup_tables: &mut HashMap<usize, StateTable<S>>,
        group_key: Option<&OwnedRow>,
    ) -> StreamExecutorResult<Vec<Option<Bitmap>>> {
        // convert `Option<Bitmap>` to `Vis` for convenience
        let mut visibilities = visibilities
            .into_iter()
            .map(|v| match v {
                Some(bitmap) => Vis::from(bitmap),
                None => Vis::from(ops.len()),
            })
            .collect_vec();
        for (distinct_col, deduplicater) in &mut self.deduplicaters {
            let column = columns[*distinct_col].array_ref();
            let dedup_table = dedup_tables.get_mut(distinct_col).unwrap();
            // Select visibilities (as mutable references) of distinct agg calls that distinct on
            // `distinct_col` so that `Deduplicater` doesn't need to care about index mapping.
            // Safety: all items in `agg_call_indices` are unique by nature.
            let visibilities = unsafe {
                get_many_mut_from_slice(&mut visibilities, deduplicater.agg_call_indices())
            };
            deduplicater
                .dedup(ops, column, visibilities, dedup_table, group_key)
                .await?;
        }
        Ok(visibilities
            .into_iter()
            .map(|v| v.into_visibility())
            .collect())
    }

    pub fn flush(
        &self,
        dedup_tables: &mut HashMap<usize, StateTable<S>>,
    ) -> StreamExecutorResult<()> {
        for (distinct_col, deduplicater) in &self.deduplicaters {
            let dedup_table = dedup_tables.get_mut(distinct_col).unwrap();
            deduplicater.flush(dedup_table);
        }
    }
}
