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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::marker::PhantomData;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayImpl, Op};
use risingwave_common::buffer::Bitmap;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{Datum, ScalarImpl};
use risingwave_storage::StateStore;

use super::AggCall;
use crate::common::table::state_table::StateTable;

/// Deduplicater for one distinct column.
struct Deduplicater<S: StateStore> {
    agg_call_indices: Vec<usize>,
    // TODO(rctmp): need a cache
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

    /// Update the `visibilities` of distinct agg calls that distinct on the `column`.
    ///
    /// * `ops`: Ops for each datum in `column`.
    /// * `column`: The column to distinct on.
    /// * `visibilities` - Visibilities for agg calls that distinct on the this column.
    /// * `dedup_table` - The deduplication table for this distinct column.
    fn dedup(
        &mut self,
        ops: &[Op],
        column: &ArrayImpl,
        visibilities: &[&mut Option<Bitmap>],
        dedup_table: &mut StateTable<S>,
    ) {
        todo!()
    }
}

/// # Safety
///
/// There must not be duplicate indices in `indices`.
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

    pub fn dedup_chunk(
        &mut self,
        ops: &[Op],
        columns: &[Column],
        mut visibilities: Vec<Option<Bitmap>>,
        dedup_tables: &mut HashMap<usize, StateTable<S>>,
    ) -> Vec<Option<Bitmap>> {
        for (distinct_col, deduplicater) in &mut self.deduplicaters {
            let column = columns[*distinct_col].array_ref();
            let agg_call_indices = deduplicater.agg_call_indices();
            let dedup_table = dedup_tables.get_mut(distinct_col).unwrap();
            // Safety: all items in `agg_call_indices` are unique by nature.
            let visibilities =
                unsafe { get_many_mut_from_slice(&mut visibilities, agg_call_indices) };
            deduplicater.dedup(ops, column, &visibilities, dedup_table);
        }
        visibilities
    }

    pub fn flush(&mut self, dedup_tables: &mut HashMap<usize, StateTable<S>>) {
        todo!()
    }
}
