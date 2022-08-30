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

use std::collections::{btree_map, BTreeMap};

use super::*;

#[expect(dead_code)]
type JoinEntryStateIter<'a> = btree_map::Iter<'a, PkType, StateValueType>;

#[expect(dead_code)]
type JoinEntryStateValues<'a> = btree_map::Values<'a, PkType, StateValueType>;

#[expect(dead_code)]
type JoinEntryStateValuesMut<'a> = btree_map::ValuesMut<'a, PkType, StateValueType>;

/// We manages a `BTreeMap` in memory for all entries belonging to a join key.
/// When evicted, `cached` does not hold any entries.
///
/// If a `JoinEntryState` exists for a join key, the all records under this
/// join key will be presented in the cache.
pub struct JoinEntryState {
    /// The full copy of the state. If evicted, it will be `None`.
    cached: BTreeMap<PkType, StateValueType, SharedStatsAlloc<Global>>,

    allocator: SharedStatsAlloc<Global>,

    content_estimate_size: usize,
}

impl Default for JoinEntryState {
    fn default() -> Self {
        let allocator = StatsAlloc::new(Global).shared();
        Self {
            cached: BTreeMap::new_in(allocator.clone()),
            allocator,
            content_estimate_size: 0,
        }
    }
}

impl JoinEntryState {
    // Insert into the cache and flush buffer.
    pub fn insert(&mut self, key: PkType, value: StateValueType) {
        self.content_estimate_size += key.estimate_size() + value.estimate_size();
        self.cached.insert(key, value);
    }

    pub fn remove(&mut self, pk: PkType) {
        if let Some(value) = self.cached.remove(&pk) {
            self.content_estimate_size = self
                .content_estimate_size
                .saturating_sub(pk.estimate_size() + value.estimate_size())
        }
    }

    #[expect(dead_code)]
    pub fn iter(&self) -> JoinEntryStateIter<'_> {
        self.cached.iter()
    }

    #[expect(dead_code)]
    pub fn values(&self) -> JoinEntryStateValues<'_> {
        self.cached.values()
    }

    /// Note: To make the estimcate size correct, the caller should ensure that it does not mutate
    /// the estimate size of the [`StateValueType`].
    pub fn values_mut<'a, 'b: 'a>(
        &'a mut self,
        data_types: &'b [DataType],
    ) -> impl Iterator<Item = (&'a mut StateValueType, StreamExecutorResult<JoinRow>)> + 'a {
        self.cached.values_mut().map(|encoded| {
            let decoded = encoded.decode(data_types);
            (encoded, decoded)
        })
    }

    pub fn len(&self) -> usize {
        self.cached.len()
    }

    pub fn estimate_size(&self) -> usize {
        self.content_estimate_size + self.allocator.bytes_in_use() + std::mem::size_of::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    #[tokio::test]
    async fn test_managed_all_or_none_state() {
        let mut managed_state = JoinEntryState::default();
        let pk_indices = [0];
        let col1 = [1, 2, 3];
        let col2 = [6, 5, 4];
        let col_types = vec![DataType::Int64, DataType::Int64];
        let data_chunk = DataChunk::from_pretty(
            "I I
             3 4
             2 5
             1 6",
        );

        for row_ref in data_chunk.rows() {
            let row: Row = row_ref.into();
            let pk = pk_indices.iter().map(|idx| row[*idx].clone()).collect_vec();
            let pk = Row(pk);
            let join_row = JoinRow { row, degree: 0 };
            managed_state.insert(pk, join_row.encode().unwrap());
        }

        for ((_, matched_row), (d1, d2)) in managed_state
            .values_mut(&col_types)
            .zip_eq(col1.iter().zip_eq(col2.iter()))
        {
            let matched_row = matched_row.unwrap();
            assert_eq!(matched_row.row[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(matched_row.row[1], Some(ScalarImpl::Int64(*d2)));
            assert_eq!(matched_row.degree, 0);
        }
    }
}
