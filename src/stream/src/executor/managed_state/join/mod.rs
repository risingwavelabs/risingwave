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

mod join_entry_state;
use std::alloc::Global;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut, Index};

use futures::pin_mut;
use futures_async_stream::for_await;
use itertools::Itertools;
pub use join_entry_state::JoinEntryState;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::collection::evictable::EvictableHashMap;
use risingwave_common::error::{ErrorCode, Result as RwResult, RwError};
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::{Keyspace, StateStore};
use stats_alloc::{SharedStatsAlloc, StatsAlloc};

type DegreeType = i64;
/// This is a row with a match degree
#[derive(Clone, Debug)]
pub struct JoinRow {
    pub row: Row,
    degree: DegreeType,
}

impl Index<usize> for JoinRow {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.row[index]
    }
}

impl JoinRow {
    pub fn new(row: Row, degree: DegreeType) -> Self {
        Self { row, degree }
    }

    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.row.size()
    }

    pub fn is_zero_degree(&self) -> bool {
        self.degree == 0
    }

    pub fn inc_degree(&mut self) -> DegreeType {
        self.degree += 1;
        self.degree
    }

    pub fn dec_degree(&mut self) -> RwResult<DegreeType> {
        if self.degree == 0 {
            return Err(
                ErrorCode::InternalError("Tried to decrement zero join row degree".into()).into(),
            );
        }
        self.degree -= 1;
        Ok(self.degree)
    }

    pub fn row_by_indices(&self, indices: &[usize]) -> Row {
        Row(indices
            .iter()
            .map(|&idx| self.row.index(idx).to_owned())
            .collect_vec())
    }

    /// Make degree as the last datum of row
    pub fn into_row(mut self) -> Row {
        self.row.0.push(Some(ScalarImpl::Int64(self.degree)));
        self.row
    }

    /// Convert [`Row`] with last datum as degree to [`JoinRow`]
    pub fn from_row(row: Row) -> Self {
        let mut datums = row.0;
        let degree_datum = datums
            .pop()
            .expect("missing degree in JoinRow")
            .expect("degree should not be null");
        let degree = degree_datum.into_int64();
        JoinRow {
            row: Row(datums),
            degree,
        }
    }
}

type PkType = Row;

pub type StateValueType = JoinRow;
pub type HashValueType = JoinEntryState;

type JoinHashMapInner<K> =
    EvictableHashMap<K, HashValueType, PrecomputedBuildHasher, SharedStatsAlloc<Global>>;

pub struct JoinHashMap<K: HashKey, S: StateStore> {
    /// Allocator
    alloc: SharedStatsAlloc<Global>,
    /// Store the join states.
    // SAFETY: This is a self-referential data structure and the allocator is owned by the struct
    // itself. Use the field is safe iff the struct is constructed with [`moveit`](https://crates.io/crates/moveit)'s way.
    inner: JoinHashMapInner<K>,
    /// Data types of the columns
    join_key_data_types: Vec<DataType>,
    /// Indices of the primary keys
    pk_indices: Vec<usize>,
    /// Current epoch
    current_epoch: u64,
    /// State table
    state_table: StateTable<S>,
}

impl<K: HashKey, S: StateStore> JoinHashMap<K, S> {
    /// Create a [`JoinHashMap`] with the given LRU capacity.
    pub fn new(
        target_cap: usize,
        pk_indices: Vec<usize>,
        join_key_indices: Vec<usize>,
        mut data_types: Vec<DataType>,
        keyspace: Keyspace<S>,
        dist_key_indices: Option<Vec<usize>>,
    ) -> Self {
        let join_key_data_types = join_key_indices
            .iter()
            .map(|idx| data_types[*idx].clone())
            .collect_vec();

        // Put the degree to the last column of the table.
        data_types.push(DataType::Int64);

        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();

        let table_pk_indices = [join_key_indices, pk_indices.clone()].concat();

        // Order type doesn't matter here. Arbitrarily choose one.
        let order_types = vec![OrderType::Descending; table_pk_indices.len()];

        let state_table = StateTable::new(
            keyspace,
            column_descs,
            order_types,
            dist_key_indices,
            table_pk_indices,
        );
        let alloc = StatsAlloc::new(Global).shared();
        Self {
            inner: EvictableHashMap::with_hasher_in(
                target_cap,
                PrecomputedBuildHasher,
                alloc.clone(),
            ),
            join_key_data_types,
            pk_indices,
            current_epoch: 0,
            state_table,
            alloc,
        }
    }

    #[allow(dead_code)]
    /// Report the bytes used by the join map.
    // FIXME: Currently, only memory used in the hash map itself is counted.
    pub fn bytes_in_use(&self) -> usize {
        self.alloc.bytes_in_use()
    }

    pub fn update_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    /// Returns a mutable reference to the value of the key in the memory, if does not exist, look
    /// up in remote storage and return. If not exist in remote storage, a
    /// `JoinEntryState` with empty cache will be returned.
    // #[allow(dead_code)]
    // pub async fn get<'a>(&'a mut self, key: &K) -> Option<&'a HashValueType> {
    //     let state = self.inner.get(key);
    //     // TODO: we should probably implement a entry function for `LruCache`
    //     match state {
    //         Some(_) => self.inner.get(key),
    //         None => {
    //             let remote_state = self.fetch_cached_state(key).await.unwrap();
    //             self.inner.put(key.clone(), remote_state);
    //             self.inner.get(key)
    //         }
    //     }
    // }

    /// Returns a mutable reference to the value of the key in the memory, if does not exist, look
    /// up in remote storage and return. If not exist in remote storage, a
    /// `JoinEntryState` with empty cache will be returned.
    // #[allow(dead_code)]
    // pub async fn get_mut<'a>(&'a mut self, key: &'a K) -> Option<&'a mut HashValueType> {
    //     let state = self.inner.get(key);
    //     // TODO: we should probably implement a entry function for `LruCache`
    //     match state {
    //         Some(_) => self.inner.get_mut(key),
    //         None => {
    //             let remote_state = self.fetch_cached_state(key).await.unwrap();
    //             self.inner.put(key.clone(), remote_state);
    //             self.inner.get_mut(key)
    //         }
    //     }
    // }

    /// Remove the key in the memory, returning the value at the key if the
    /// key was previously in the map. If does not exist, look
    /// up in remote storage and return. If not exist in remote storage, a
    /// `JoinEntryState` with empty cache will be returned.
    /// WARNING: This will NOT remove anything from remote storage.
    pub async fn remove_state<'a>(&mut self, key: &K) -> Option<HashValueType> {
        let state = self.inner.pop(key);
        match state {
            Some(_) => state,
            None => Some(self.fetch_cached_state(key).await.unwrap()),
        }
    }

    /// Fetch cache from the state store. Should only be called if the key does not exist in memory.
    async fn fetch_cached_state(&self, key: &K) -> RwResult<JoinEntryState> {
        let key = key.clone().deserialize(self.join_key_data_types.iter())?;

        let table_iter = self
            .state_table
            .iter_with_pk_prefix(&key, self.current_epoch)
            .await?;
        pin_mut!(table_iter);

        let mut cached = BTreeMap::new();
        #[for_await]
        for row in table_iter {
            let row = row?.into_owned();
            let pk = row.by_indices(&self.pk_indices);

            cached.insert(pk, JoinRow::from_row(row));
        }
        Ok(JoinEntryState::with_cached(cached))
    }

    pub async fn flush(&mut self) -> RwResult<()> {
        self.state_table
            .commit_with_value_meta(self.current_epoch)
            .await
            .map_err(RwError::from)
    }

    /// Insert a key
    pub fn insert(&mut self, join_key: &K, pk: Row, value: JoinRow) -> RwResult<()> {
        if let Some(entry) = self.inner.get_mut(join_key) {
            entry.insert(pk, value.clone());
        }

        // If no cache maintained, only update the flush buffer.
        self.state_table.insert(value.into_row())?;
        Ok(())
    }

    /// Delete a key
    pub fn delete(&mut self, join_key: &K, pk: Row, value: JoinRow) -> RwResult<()> {
        if let Some(entry) = self.inner.get_mut(join_key) {
            entry.remove(pk);
        }

        // If no cache maintained, only update the flush buffer.
        self.state_table.delete(value.into_row())?;
        Ok(())
    }

    /// Insert a [`JoinEntryState`]
    pub fn insert_state(&mut self, key: &K, state: JoinEntryState) {
        self.inner.put(key.clone(), state);
    }

    pub fn inc_degree(&mut self, join_row: &mut JoinRow) -> RwResult<()> {
        let old_row = join_row.clone().into_row();
        join_row.inc_degree();
        let new_row = join_row.clone().into_row();

        self.state_table.update(old_row, new_row)?;
        Ok(())
    }

    pub fn dec_degree(&mut self, join_row: &mut JoinRow) -> RwResult<()> {
        let old_row = join_row.clone().into_row();
        join_row.dec_degree()?;
        let new_row = join_row.clone().into_row();

        self.state_table.update(old_row, new_row)?;
        Ok(())
    }
}

impl<K: HashKey, S: StateStore> Deref for JoinHashMap<K, S> {
    type Target = JoinHashMapInner<K>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K: HashKey, S: StateStore> DerefMut for JoinHashMap<K, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
