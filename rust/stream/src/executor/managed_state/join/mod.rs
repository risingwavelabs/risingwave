mod join_entry_state;
use std::ops::{Deref, DerefMut, Index};
use std::sync::Arc;

use itertools::Itertools;
pub use join_entry_state::JoinEntryState;
use risingwave_common::array::Row;
use risingwave_common::collection::evictable::EvictableHashMap;
use risingwave_common::error::Result as RWResult;
use risingwave_common::types::{deserialize_datum_from, serialize_datum_into, DataType, Datum};
use risingwave_storage::keyspace::Segment;
use risingwave_storage::{Keyspace, StateStore};
use serde::{Deserialize, Serialize};

/// This is a row with a match degree
#[derive(Clone)]
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

    pub fn dec_degree(&mut self) -> u64 {
        self.degree -= 1;
        self.degree
    }

    /// Serialize the `JoinRow` into a memcomparable bytes. All values must not be null.
    pub fn serialize(&self) -> Result<Vec<u8>, memcomparable::Error> {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        for v in &self.row.0 {
            serialize_datum_into(v, &mut serializer)?;
        }
        self.degree.serialize(&mut serializer)?;
        Ok(serializer.into_inner())
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

    /// Deserialize the row from a memcomparable bytes.
    pub fn deserialize(&self, data: &[u8]) -> Result<JoinRow, memcomparable::Error> {
        let mut values = vec![];
        values.reserve(self.data_types.len());
        let mut deserializer = memcomparable::Deserializer::new(data);
        for &ty in &self.data_types {
            values.push(deserialize_datum_from(&ty, &mut deserializer)?);
        }
        let degree = u64::deserialize(&mut deserializer)?;
        Ok(JoinRow {
            row: Row(values),
            degree,
        })
    }
}

type PkType = Row;

pub type StateValueType = JoinRow;
pub type HashKeyType = Row;
pub type HashValueType<S> = JoinEntryState<S>;

pub struct JoinHashMap<S: StateStore> {
    /// Store the join states.
    inner: EvictableHashMap<HashKeyType, HashValueType<S>>,
    /// Data types of the columns
    data_types: Arc<[DataType]>,
    /// Data types of primary keys
    pk_data_types: Arc<[DataType]>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
    /// Current epoch
    current_epoch: u64,
}

impl<S: StateStore> JoinHashMap<S> {
    /// Create a [`JoinHashMap`] with the given LRU capacity.
    pub fn new(
        target_cap: usize,
        pk_indices: Vec<usize>,
        data_types: Vec<DataType>,
        keyspace: Keyspace<S>,
    ) -> Self {
        let pk_data_types = pk_indices.iter().map(|idx| data_types[*idx]).collect_vec();

        Self {
            inner: EvictableHashMap::new(target_cap),
            data_types: data_types.into(),
            pk_data_types: pk_data_types.into(),
            keyspace,
            current_epoch: 0,
        }
    }

    pub fn update_epoch(&mut self, epoch: u64) {
        self.current_epoch = epoch;
    }

    fn get_state_keyspace(&self, key: &HashKeyType) -> Keyspace<S> {
        // TODO: in pure in-memory engine, we should not do this serialization.
        let key_encoded = key.serialize().unwrap();
        self.keyspace
            .with_segment(Segment::VariantLength(key_encoded))
    }

    #[allow(dead_code)]
    /// Returns a mutable reference to the value of the key in the memory, if does not exist, look
    /// up in remote storage and return, if still not exist, return None.
    pub async fn get(&mut self, key: &HashKeyType) -> Option<&HashValueType<S>> {
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
    pub async fn get_mut(&mut self, key: &HashKeyType) -> Option<&mut HashValueType<S>> {
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
    pub async fn get_mut_without_cached(
        &mut self,
        key: &HashKeyType,
    ) -> Option<&mut HashValueType<S>> {
        let state = self.inner.get(key);
        // TODO: we should probably implement a entry function for `LruCache`
        match state {
            Some(_) => self.inner.get_mut(key),
            None => {
                let keyspace = self.get_state_keyspace(key);
                let all_data = keyspace
                    .scan_strip_prefix(None, self.current_epoch)
                    .await
                    .unwrap();
                let total_count = all_data.len();
                if total_count > 0 {
                    let state = JoinEntryState::new(
                        keyspace,
                        self.data_types.clone(),
                        self.pk_data_types.clone(),
                    );
                    self.inner.put(key.clone(), state);
                    Some(self.inner.get_mut(key).unwrap())
                } else {
                    None
                }
            }
        }
    }

    #[allow(dead_code)]
    /// Returns true if the key in the memory or remote storage, otherwise false.
    pub async fn contains(&mut self, key: &HashKeyType) -> bool {
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
    async fn fetch_cached_state(&self, key: &HashKeyType) -> RWResult<Option<JoinEntryState<S>>> {
        let keyspace = self.get_state_keyspace(key);
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
    pub async fn init_without_cache(&mut self, key: &HashKeyType) -> RWResult<()> {
        let keyspace = self.get_state_keyspace(key);
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
    pub async fn get_or_init_without_cache(
        &mut self,
        key: &HashKeyType,
    ) -> RWResult<&mut JoinEntryState<S>> {
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

impl<S: StateStore> Deref for JoinHashMap<S> {
    type Target = EvictableHashMap<HashKeyType, HashValueType<S>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: StateStore> DerefMut for JoinHashMap<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
