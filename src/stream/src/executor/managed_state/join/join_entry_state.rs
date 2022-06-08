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

use bytes::Bytes;
use madsim::collections::{btree_map, BTreeMap};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_storage::{Keyspace, StateStore};

use super::*;

type JoinEntryStateIter<'a> = btree_map::Iter<'a, PkType, StateValueType>;

type JoinEntryStateValues<'a> = btree_map::Values<'a, PkType, StateValueType>;

type JoinEntryStateValuesMut<'a> = btree_map::ValuesMut<'a, PkType, StateValueType>;

/// We manages a `BTreeMap` in memory for all entries belonging to a join key,
/// since each `WriteBatch` is an ordered list of key-value pairs.
/// When evicted, `BTreeMap` does not hold any entries.
pub struct JoinEntryState<S: StateStore> {
    /// The full copy of the state. If evicted, it will be `None`.
    cached: BTreeMap<PkType, StateValueType>,

    /// Data types of the sort column
    data_types: Arc<[DataType]>,

    /// Data types of primary keys
    pk_data_types: Arc<[DataType]>,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
}

impl<S: StateStore> JoinEntryState<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
    ) -> Self {
        Self {
            cached: BTreeMap::new(),
            data_types,
            pk_data_types,
            keyspace,
        }
    }

    pub async fn with_cached_state(
        keyspace: Keyspace<S>,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
        epoch: u64,
    ) -> Result<Option<Self>> {
        let all_data = keyspace.scan(None, epoch).await?;
        if !all_data.is_empty() {
            // Insert cached states.
            let cached = Self::fill_cached(all_data, data_types.clone(), pk_data_types.clone())?;
            Ok(Some(Self {
                cached,
                data_types,
                pk_data_types,
                keyspace,
            }))
        } else {
            Ok(None)
        }
    }

    fn fill_cached(
        data: Vec<(Bytes, Bytes)>,
        data_types: Arc<[DataType]>,
        pk_data_types: Arc<[DataType]>,
    ) -> Result<BTreeMap<PkType, StateValueType>> {
        let mut cached = BTreeMap::new();
        

        Ok(cached)
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
    pub async fn iter(&mut self, epoch: u64) -> JoinEntryStateIter<'_> {
        self.cached.iter()
    }

    #[allow(dead_code)]
    pub async fn values(&mut self, epoch: u64) -> JoinEntryStateValues<'_> {
        self.cached.values()
    }

    pub async fn values_mut(&mut self, epoch: u64) -> JoinEntryStateValuesMut<'_> {
        self.cached.values_mut()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::*;
    use risingwave_common::catalog::TableId;
    use risingwave_common::types::ScalarImpl;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_managed_all_or_none_state() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::table_root(store.clone(), &TableId::from(0x2333));
        let mut managed_state = JoinEntryState::new(
            keyspace,
            vec![DataType::Int64, DataType::Int64].into(),
            vec![DataType::Int64].into(),
        );
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

        let epoch = 0;
        for state in managed_state
            .iter(epoch)
            .await
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
