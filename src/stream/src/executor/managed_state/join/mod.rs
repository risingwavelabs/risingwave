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
use std::ops::{Deref, DerefMut, Index};
use std::sync::Arc;

use bytes::Buf;
use itertools::Itertools;
pub use join_entry_state::JoinEntryState;
use risingwave_common::array::{Row, RowDeserializer};
use risingwave_common::collection::evictable::EvictableHashMap;
use risingwave_common::error::{ErrorCode, Result as RwResult};
use risingwave_common::hash::{HashKey, PrecomputedBuildHasher};
use risingwave_common::types::{DataType, Datum};
use risingwave_storage::{Keyspace, StateStore};
use stats_alloc::{SharedStatsAlloc, StatsAlloc};

/// This is a row with a match degree
#[derive(Clone, Debug)]
pub struct JoinRow {
    pub row: Row,
    degree: u64,
}

impl Index<usize> for JoinRow {
    type Output = Datum;

    fn index(&self, index: usize) -> &Self::Output {
        &self.row[index]
    }
}

impl JoinRow {
    pub fn new(row: Row, degree: u64) -> Self {
        Self { row, degree }
    }

    #[allow(dead_code)]
    pub fn size(&self) -> usize {
        self.row.size()
    }

    pub fn is_zero_degree(&self) -> bool {
        self.degree == 0
    }

    pub fn inc_degree(&mut self) -> u64 {
        self.degree += 1;
        self.degree
    }

    pub fn dec_degree(&mut self) -> RwResult<u64> {
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

    /// Serialize the `JoinRow` into a value encoding bytes.
    pub fn serialize(&self) -> RwResult<Vec<u8>> {
        let mut vec = Vec::with_capacity(10);

        // Serialize row.
        vec.extend(self.row.value_encode()?);

        // Serialize degree.
        vec.extend(self.degree.to_le_bytes());

        Ok(vec)
    }
}

/// Deserializer of the `JoinRow`.
pub struct JoinRowDeserializer {
    data_types: Vec<DataType>,
}

impl JoinRowDeserializer {
    /// Creates a new `RowDeserializer` with row schema.
    pub fn new(schema: Vec<DataType>) -> Self {
        JoinRowDeserializer { data_types: schema }
    }

    /// Deserialize the [`JoinRow`] from a value encoding bytes.
    pub fn deserialize(&self, mut data: impl Buf) -> RwResult<JoinRow> {
        let deserializer = RowDeserializer::new(self.data_types.clone());
        let row = deserializer.value_decode(&mut data)?;
        let degree = data.get_u64_le();
        Ok(JoinRow { row, degree })
    }
}

type PkType = Row;

pub type StateValueType = JoinRow;
pub type HashValueType<S> = JoinEntryState<S>;

type JoinHashMapInner<K, S> =
    EvictableHashMap<K, HashValueType<S>, PrecomputedBuildHasher, SharedStatsAlloc<Global>>;

pub struct JoinHashMap<K: HashKey, S: StateStore> {
    /// Allocator
    alloc: SharedStatsAlloc<Global>,
    /// Store the join states.
    // SAFETY: This is a self-referential data structure and the allocator is owned by the struct
    // itself. Use the field is safe iff the struct is constructed with [`moveit`](https://crates.io/crates/moveit)'s way.
    inner: JoinHashMapInner<K, S>,
    /// Data types of the columns
    data_types: Arc<[DataType]>,
    /// Data types of the columns
    join_key_data_types: Arc<[DataType]>,
    /// Data types of primary keys
    pk_data_types: Arc<[DataType]>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
    /// Current epoch
    current_epoch: u64,
}

impl<K: HashKey, S: StateStore> JoinHashMap<K, S> {
    /// Create a [`JoinHashMap`] with the given LRU capacity.
    pub fn new(
        target_cap: usize,
        pk_indices: Vec<usize>,
        join_key_indices: Vec<usize>,
        data_types: Vec<DataType>,
        keyspace: Keyspace<S>,
    ) -> Self {
        let pk_data_types = pk_indices
            .iter()
            .map(|idx| data_types[*idx].clone())
            .collect_vec();
        let join_key_data_types = join_key_indices
            .iter()
            .map(|idx| data_types[*idx].clone())
            .collect_vec();
        let alloc = StatsAlloc::new(Global).shared();
        Self {
            inner: EvictableHashMap::with_hasher_in(
                target_cap,
                PrecomputedBuildHasher,
                alloc.clone(),
            ),
            data_types: data_types.into(),
            join_key_data_types: join_key_data_types.into(),
            pk_data_types: pk_data_types.into(),
            keyspace,
            current_epoch: 0,
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

    fn get_state_keyspace(&self, key: &K) -> RwResult<Keyspace<S>> {
        // TODO: in pure in-memory engine, we should not do this serialization.
        let key = key.clone().deserialize(self.join_key_data_types.iter())?;
        let key_encoded = key.serialize().unwrap();
        Ok(self.keyspace.append(key_encoded))
    }

    /// Returns a mutable reference to the value of the key in the memory, if does not exist, look
    /// up in remote storage and return, if still not exist, return None.
    #[allow(dead_code)]
    pub async fn get<'a>(&'a mut self, key: &K) -> Option<&'a HashValueType<S>> {
        let state = self.inner.get(key);
        // TODO: we should probably implement a entry function for `LruCache`
        match state {
            Some(_) => self.inner.get(key),
            None => {
                let remote_state = self.fetch_cached_state(key).await.unwrap();
                remote_state.map(|rv| {
                    self.inner.put(key.clone(), rv);
                    self.inner.get(key).unwrap()
                })
            }
        }
    }

    /// Returns a mutable reference to the value of the key in the memory, if does not exist, look
    /// up in remote storage and return, if still not exist, return None.
    pub async fn get_mut(&mut self, key: &K) -> Option<&mut HashValueType<S>> {
        let state = self.inner.get(key);
        // TODO: we should probably implement a entry function for `LruCache`
        match state {
            Some(_) => self.inner.get_mut(key),
            None => {
                let remote_state = self.fetch_cached_state(key).await.unwrap();
                remote_state.map(|rv| {
                    self.inner.put(key.clone(), rv);
                    self.inner.get_mut(key).unwrap()
                })
            }
        }
    }

    /// Returns a mutable reference to the value of the key in the memory, if does not exist, look
    /// up in remote storage and return the [`JoinEntryState`] without cached state, if still not
    /// exist, return None.
    pub async fn get_mut_without_cached<'a, 'b: 'a>(
        &'a mut self,
        key: &'b K,
    ) -> RwResult<Option<&'a mut HashValueType<S>>> {
        let state = self.inner.get(key);
        // TODO: we should probably implement a entry function for `LruCache`
        match state {
            Some(_) => Ok(self.inner.get_mut(key)),
            None => {
                let keyspace = self.get_state_keyspace(key)?;
                let all_data = keyspace.scan(None, self.current_epoch).await.unwrap();
                let total_count = all_data.len();
                if total_count > 0 {
                    let state = JoinEntryState::new(
                        keyspace,
                        self.data_types.clone(),
                        self.pk_data_types.clone(),
                    );
                    self.inner.put(key.clone(), state);
                    Ok(Some(self.inner.get_mut(key).unwrap()))
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// Returns true if the key in the memory or remote storage, otherwise false.
    #[allow(dead_code)]
    pub async fn contains(&mut self, key: &K) -> bool {
        let contains = self.inner.contains(key);
        if contains {
            true
        } else {
            let remote_state = self.fetch_cached_state(key).await.unwrap();
            match remote_state {
                Some(rv) => {
                    self.inner.put(key.clone(), rv);
                    true
                }
                None => false,
            }
        }
    }

    /// Fetch cache from the state store. Should only be called if the key does not exist in memory.
    async fn fetch_cached_state(&self, key: &K) -> RwResult<Option<JoinEntryState<S>>> {
        let keyspace = self.get_state_keyspace(key)?;
        JoinEntryState::with_cached_state(
            keyspace,
            self.data_types.clone(),
            self.pk_data_types.clone(),
            self.current_epoch,
        )
        .await
    }

    /// Create a [`JoinEntryState`] without cached state. Should only be called if the key
    /// does not exist in memory or remote storage.
    pub async fn init_without_cache(&mut self, key: &K) -> RwResult<()> {
        let keyspace = self.get_state_keyspace(key)?;
        let state = JoinEntryState::new(
            keyspace,
            self.data_types.clone(),
            self.pk_data_types.clone(),
        );
        self.inner.put(key.clone(), state);
        Ok(())
    }

    /// Get or create a [`JoinEntryState`] without cached state. Should only be called if the key
    /// does not exist in memory or remote storage.
    pub async fn get_or_init_without_cache<'a, 'b: 'a>(
        &'a mut self,
        key: &'b K,
    ) -> RwResult<&'a mut JoinEntryState<S>> {
        // TODO: we should probably implement a entry function for `LruCache`
        let contains = self.inner.contains(key);
        if contains {
            Ok(self.inner.get_mut(key).unwrap())
        } else {
            self.init_without_cache(key).await?;
            Ok(self.inner.get_mut(key).unwrap())
        }
    }
}

impl<K: HashKey, S: StateStore> Deref for JoinHashMap<K, S> {
    type Target = JoinHashMapInner<K, S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K: HashKey, S: StateStore> DerefMut for JoinHashMap<K, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
