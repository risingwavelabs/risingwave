mod all_or_none;
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut, Index};
use std::sync::Arc;

pub use all_or_none::AllOrNoneState;
use itertools::Itertools;
use risingwave_common::array::data_chunk_iter::RowDeserializer;
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
pub type HashValueType<S> = AllOrNoneState<S>;

pub struct JoinHashMap<S: StateStore> {
    /// Store the join states.
    inner: EvictableHashMap<HashKeyType, HashValueType<S>>,
    /// Data types of the columns
    data_types: Arc<[DataType]>,
    /// Data types of primary keys
    pk_data_types: Arc<[DataType]>,
    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
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
        }
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

    /// Fetch cache from the state store.
    async fn fetch_cached_state(&self, key: &HashKeyType) -> RWResult<Option<AllOrNoneState<S>>> {
        let keyspace = self.get_state_keyspace(key);
        let all_data = keyspace.scan_strip_prefix(None).await?;
        if !all_data.is_empty() {
            // Insert cached states.
            let mut cached = BTreeMap::new();
            for (raw_key, raw_value) in all_data {
                let pk_deserializer = RowDeserializer::new(self.pk_data_types.to_vec());
                let key = pk_deserializer.deserialize_not_null(&raw_key)?;
                let deserializer = JoinRowDeserializer::new(self.data_types.to_vec());
                let value = deserializer.deserialize(&raw_value)?;
                cached.insert(key, value);
            }
            Ok(Some(AllOrNoneState::with_cached_state(
                keyspace,
                cached,
                self.data_types.clone(),
                self.pk_data_types.clone(),
            )))
        } else {
            Ok(None)
        }
    }

    /// Create a [`AllOrNoneState`] without cached state. Should only be called if the key
    /// does not exist in memory or remote storage.
    pub async fn init_without_cache(&mut self, key: &HashKeyType) -> RWResult<()> {
        let keyspace = self.get_state_keyspace(key);
        let all_data = keyspace.scan_strip_prefix(None).await?;
        let total_count = all_data.len();
        let state = AllOrNoneState::new(
            keyspace,
            total_count,
            self.data_types.clone(),
            self.pk_data_types.clone(),
        );
        self.inner.put(key.clone(), state);
        Ok(())
    }

    /// Get or create a [`AllOrNoneState`] without cached state. Should only be called if the key
    /// does not exist in memory or remote storage.
    pub async fn get_or_init_without_cache(
        &mut self,
        key: &HashKeyType,
    ) -> RWResult<&mut AllOrNoneState<S>> {
        // TODO: we should probably implement a entry function for `LruCache`
        let contains = self.inner.contains(key);
        if contains {
            Ok(self.inner.get_mut(key).unwrap())
        } else {
            let keyspace = self.get_state_keyspace(key);
            let all_data = keyspace.scan_strip_prefix(None).await?;
            let total_count = all_data.len();
            let state = AllOrNoneState::new(
                keyspace,
                total_count,
                self.data_types.clone(),
                self.pk_data_types.clone(),
            );
            self.inner.put(key.clone(), state);

            Ok(self.inner.get_mut(key).unwrap())
        }
    }

    // /// Returns a mutable reference to the value of the key, or put with `construct` if it is not
    // /// present.
    // pub fn get_or_put<'a, I>(
    //     &'a mut self,
    //     key: &HashKeyType,
    //     construct: I,
    // ) -> &'a mut HashValueType<S> {
    //     if !self.inner.contains(key) {
    //         let value = construct();
    //         self.inner.put(key.to_owned(), value);
    //     }
    //     self.inner.get_mut(key).unwrap()
    // }
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

// pub fn create_hash_join_state<S: StateStore>(
//     key: PkType,
//     keyspace: &Keyspace<S>,
//     data_types: &[DataType],
//     pk_data_types: &[DataType],
// ) -> AllOrNoneState<S> {
//     // TODO: in pure in-memory engine, we should not do this serialization.
//     let key_encoded = key.serialize().unwrap();

//     let keyspace = keyspace.with_segment(Segment::VariantLength(key_encoded));

//     AllOrNoneState::new(keyspace, data_types.to_vec(), pk_data_types.to_vec())
// }
