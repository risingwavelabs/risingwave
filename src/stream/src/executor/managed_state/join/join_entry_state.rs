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


use madsim::collections::{btree_map, BTreeMap};

use super::*;

type JoinEntryStateIter<'a> = btree_map::Iter<'a, PkType, StateValueType>;

type JoinEntryStateValues<'a> = btree_map::Values<'a, PkType, StateValueType>;

type JoinEntryStateValuesMut<'a> = btree_map::ValuesMut<'a, PkType, StateValueType>;

/// We manages a `BTreeMap` in memory for all entries belonging to a join key,
/// since each `WriteBatch` is an ordered list of key-value pairs.
/// When evicted, `BTreeMap` does not hold any entries.
pub struct JoinEntryState {
    /// The full copy of the state. If evicted, it will be `None`.
    cached: BTreeMap<PkType, StateValueType>,
}

impl JoinEntryState {
    pub fn with_cached(
        cached: BTreeMap<PkType, StateValueType>
    ) -> Self {
        Self {
            cached,
        }
    }

    /// If the cache is empty
    pub fn is_empty(&self) -> bool {
        self.cached.is_empty()
    }

    // Insert into the cache and flush buffer.
    pub fn insert(&mut self, key: PkType, value: StateValueType) {
        self.cached.insert(key.clone(), value.clone());
    }

    pub fn remove(&mut self, pk: PkType) {
        self.cached.remove(&pk);
    }

    #[allow(dead_code)]
    pub fn iter(&mut self) -> JoinEntryStateIter<'_> {
        self.cached.iter()
    }

    #[allow(dead_code)]
    pub fn values(&mut self) -> JoinEntryStateValues<'_> {
        self.cached.values()
    }

    pub fn values_mut(&mut self) -> JoinEntryStateValuesMut<'_> {
        self.cached.values_mut()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    #[tokio::test]
    async fn test_managed_all_or_none_state() {
        let mut managed_state = JoinEntryState::with_cached(BTreeMap::new());
        let pk_indices = [0];
        let col1 = [1, 2, 3];
        let col2 = [6, 5, 4];
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
            managed_state.insert(pk, join_row);
        }

        for state in managed_state
            .iter()
            .zip_eq(col1.iter().zip_eq(col2.iter()))
        {
            let ((key, value), (d1, d2)) = state;
            assert_eq!(key.0[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(value.row[0], Some(ScalarImpl::Int64(*d1)));
            assert_eq!(value.row[1], Some(ScalarImpl::Int64(*d2)));
            assert_eq!(value.degree, 0);
        }
    }
}
