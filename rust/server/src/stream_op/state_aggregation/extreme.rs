use std::collections::BTreeMap;

use bytes::Bytes;
use risingwave_common::array::stream_chunk::{Op, Ops};
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{
    deserialize_datum_not_null_from, serialize_datum_not_null_into, DataTypeRef, Datum, Scalar,
    ScalarImpl,
};

use super::super::keyspace::{Keyspace, StateStore};

/// Manages a `BTreeMap` in memory for top N entries, and the state store for remaining entries.
///
/// There are several prerequisites for using the `MinState`.
/// * Sort key must be unique. Users should always encode the sort key as value + row id. The
///   current interface doesn't support this, and we should add support in the next refactor. The
///   `K: Scalar` trait bound should be something like `SortKeySerializer`, and what actually stored
///   in the `BTreeMap` should be `(Datum, RowId)`.
/// * The order encoded key and the `Ord` implementation of the sort key must be the same. If they
///   are different, it is more likely a bug of the encoder, and should be fixed.
/// * Key can't be null.
///
/// In the state store, the datum is stored as `sort_key -> encoded_value`. Therefore, we should
/// have the following relationship:
/// * `sort_key = sort_key_encode(key)`
/// * `encoded_value = datum_encode(key)`
/// * `key = datum_decode(encoded_value)`
pub struct ManagedMinState<S: StateStore, K: Scalar + Ord> {
    /// Top N elements in the state, which stores the mapping of sort key -> (dirty, original
    /// value). This BTreeMap always maintain the elements that we are sure it's top n.
    top_n: BTreeMap<K, ScalarImpl>,

    /// The actions that will be taken on next flush
    flush_buffer: BTreeMap<K, Option<ScalarImpl>>,

    /// Number of items in the state including those not in top n cache but in state store.
    total_count: usize,

    /// Number of entries to retain in memory after each flush.
    top_n_count: Option<usize>,

    /// Data type of the sort column
    data_type: DataTypeRef,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
}

impl<S: StateStore, K: Scalar + Ord> ManagedMinState<S, K> {
    /// Create a managed min state based on `Keyspace`. When `top_n_count` is `None`, the cache will
    /// always be retained when flushing the managed state. Otherwise, we will only retain n entries
    /// after each flush.
    pub async fn new(
        keyspace: Keyspace<S>,
        data_type: DataTypeRef,
        top_n_count: Option<usize>,
    ) -> Result<Self> {
        // TODO: use the RowCount from HashAgg instead of fetching all data from the keyspace.
        // This is critical to performance.
        let total_count = keyspace.scan(None).await?.len();
        // Create the internal state based on the value we get.
        Ok(Self {
            top_n: BTreeMap::new(),
            flush_buffer: BTreeMap::new(),
            total_count,
            keyspace,
            top_n_count,
            data_type,
        })
    }

    /// Apply a batch of data to the state.
    pub async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()> {
        debug_assert!(super::verify_batch(ops, visibility, data));
        assert_eq!(data.len(), 1);

        if self.total_count == self.top_n.len() {
            // All data resides in memory, we only need to operate on the top n cache.
            for (id, (op, data)) in ops.iter().zip(data[0].iter()).enumerate() {
                let visible = visibility.map(|x| x.is_set(id).unwrap()).unwrap_or(true);
                if !visible {
                    continue;
                }
                let value: ScalarImpl = data.expect("sort key cannot be null").into_scalar_impl();
                let key: K = value.clone().try_into().unwrap();
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        self.top_n.insert(key.clone(), value.clone());
                        self.flush_buffer.insert(key, Some(value));
                        self.total_count += 1;
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.top_n.remove(&key);
                        self.flush_buffer.insert(key, None);
                        self.total_count -= 1;
                    }
                }
            }
        } else {
            // `self.top_n` only contains parts of the top keys. When applying batch, we only:
            // 1. Insert keys that is in the range of top n, so that we only maintain the top keys
            // we are sure the minimum.
            // 2. Delete keys if exists in top_n cache.
            // If the top n cache becomes empty after applying the batch, we will merge data from
            // flush_buffer and state store when getting the output.

            for (id, (op, data)) in ops.iter().zip(data[0].iter()).enumerate() {
                let visible = visibility.map(|x| x.is_set(id).unwrap()).unwrap_or(true);
                if !visible {
                    continue;
                }
                let value: ScalarImpl = data.expect("sort key cannot be null").into_scalar_impl();
                let key: K = value.clone().try_into().unwrap();
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        let mut do_insert = false;
                        if let Some((last_key, _)) = self.top_n.last_key_value() {
                            if &key < last_key {
                                do_insert = true;
                            }
                        }
                        if do_insert {
                            self.top_n.insert(key.clone(), value.clone());
                        }
                        self.flush_buffer.insert(key, Some(value));
                        self.total_count += 1;
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.top_n.remove(&key);
                        self.flush_buffer.insert(key, None);
                        self.total_count -= 1;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn get_output(&mut self) -> Result<Datum> {
        // To make things easier, we do not allow get_output before flushing. Otherwise we will need
        // to merge data from flush_buffer and state store, which is hard to implement.
        //
        // To make ExtremeState produce the correct result, the write batch must be flushed into the
        // state store before getting the output. Note that calling `.flush()` is not enough, as it
        // only generates a write batch without flushing to store.
        debug_assert!(!self.is_dirty());

        // Firstly, check if datum is available in cache.
        if let Some((_, v)) = self.top_n.first_key_value() {
            Ok(Some(v.clone()))
        } else {
            let all_data = self.keyspace.scan(self.top_n_count).await?;

            for (_, raw_value) in all_data {
                let mut deserializer = memcomparable::Deserializer::from_slice(&raw_value[..]);
                let value = deserialize_datum_not_null_from(
                    &self.data_type.data_type_kind(),
                    &mut deserializer,
                )
                .map_err(ErrorCode::MemComparableError)?
                .unwrap();
                let key = value.clone().try_into().unwrap();
                self.top_n.insert(key, value);
            }

            if let Some((_, v)) = self.top_n.first_key_value() {
                Ok(Some(v.clone()))
            } else {
                Ok(None)
            }
        }
    }

    /// Check if this state needs a flush.
    pub fn is_dirty(&self) -> bool {
        !self.flush_buffer.is_empty()
    }

    /// Flush the internal state to a write batch. TODO: add `WriteBatch` to Hummock.
    pub fn flush(&mut self, write_batch: &mut Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        debug_assert!(self.is_dirty());

        // TODO: we can populate the cache while flushing, but that's hard.

        for (k, v) in std::mem::take(&mut self.flush_buffer) {
            match v {
                Some(v) => {
                    let mut serializer = memcomparable::Serializer::default();
                    serialize_datum_not_null_into(&Some(k.into()), &mut serializer)
                        .map_err(ErrorCode::MemComparableError)?;
                    let key = serializer.into_inner();

                    let mut serializer = memcomparable::Serializer::default();
                    serialize_datum_not_null_into(&Some(v), &mut serializer)
                        .map_err(ErrorCode::MemComparableError)?;
                    let value = serializer.into_inner();

                    write_batch.push((
                        [self.keyspace.prefix(), &key[..]].concat().into(),
                        Some(value.into()),
                    ));
                }
                None => {
                    let mut serializer = memcomparable::Serializer::default();
                    serialize_datum_not_null_into(&Some(k.into()), &mut serializer)
                        .map_err(ErrorCode::MemComparableError)?;
                    let key = serializer.into_inner();

                    write_batch.push(([self.keyspace.prefix(), &key[..]].concat().into(), None));
                }
            }
        }

        if let Some(count) = self.top_n_count {
            let old_top_n = std::mem::take(&mut self.top_n);
            for (idx, (k, v)) in old_top_n.into_iter().enumerate() {
                if idx >= count {
                    break;
                }
                self.top_n.insert(k, v);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn iterate_store(&self) -> Result<Vec<K>> {
        let all_data = self.keyspace.scan(None).await?;
        let mut result = vec![];

        for (_, raw_value) in all_data {
            let mut deserializer = memcomparable::Deserializer::from_slice(&raw_value[..]);
            let value = deserialize_datum_not_null_from(
                &self.data_type.data_type_kind(),
                &mut deserializer,
            )
            .map_err(ErrorCode::MemComparableError)?
            .unwrap();
            let key = value.clone().try_into().unwrap();
            result.push(key);
        }
        Ok(result)
    }

    #[cfg(test)]
    pub async fn iterate_topn_cache(&self) -> Result<Vec<K>> {
        Ok(self.top_n.iter().map(|(k, _)| k.clone()).collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use risingwave_common::types::{Int64Type, ScalarImpl};

    use super::*;
    use crate::stream_op::keyspace::MemoryStateStore;
    use itertools::Itertools;
    use rand::prelude::*;
    use risingwave_common::array::{I64Array, Op};

    #[tokio::test]
    async fn test_managed_value_state() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state =
            ManagedMinState::<_, i64>::new(keyspace, Int64Type::create(false), Some(5))
                .await
                .unwrap();
        assert!(!managed_state.is_dirty());

        // insert 0, 10, 20
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(0), Some(10), Some(20)])
                    .unwrap()
                    .into()],
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());

        // flush to write batch and write to state store
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();

        // The minimum should be 0
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(0))
        );

        // insert 5, 15, 25, and do a flush. After that, `25`, `27, and `30` should be evicted from
        // the top n cache.
        managed_state
            .apply_batch(
                &[Op::Insert; 5],
                None,
                &[
                    &I64Array::from_slice(&[Some(5), Some(15), Some(25), Some(27), Some(30)])
                        .unwrap()
                        .into(),
                ],
            )
            .await
            .unwrap();

        // flush to write batch and write to state store
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();

        // The minimum should be 0
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(0))
        );

        // delete 0, 10, 5, 15, so that the top n cache now contains only 20.
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Delete, Op::Delete, Op::Delete],
                None,
                &[
                    &I64Array::from_slice(&[Some(0), Some(10), Some(15), Some(5)])
                        .unwrap()
                        .into(),
                ],
            )
            .await
            .unwrap();

        // flush to write batch and write to state store
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();

        // The minimum should be 20
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(20))
        );

        // delete 20, 27, so that the top n cache is empty, and we should get minimum 25.
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Delete],
                None,
                &[&I64Array::from_slice(&[Some(20), Some(27)]).unwrap().into()],
            )
            .await
            .unwrap();

        // flush to write batch and write to state store
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();

        // The minimum should be 25
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(25))
        );

        // delete 25, and we should get minimum 30.
        managed_state
            .apply_batch(
                &[Op::Delete],
                None,
                &[&I64Array::from_slice(&[Some(25)]).unwrap().into()],
            )
            .await
            .unwrap();

        // flush to write batch and write to state store
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();

        // The minimum should be 30
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(30))
        );
    }

    #[tokio::test]
    async fn test_same_value() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state =
            ManagedMinState::<_, i64>::new(keyspace, Int64Type::create(false), Some(3))
                .await
                .unwrap();
        assert!(!managed_state.is_dirty());

        let value_buffer =
            I64Array::from_slice(&[Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)])
                .unwrap()
                .into();

        managed_state
            .apply_batch(
                &[Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(6)]).unwrap().into()],
            )
            .await
            .unwrap();

        for i in 0..100 {
            managed_state
                .apply_batch(&[Op::Insert; 6], None, &[&value_buffer])
                .await
                .unwrap();

            if i % 2 == 0 {
                // only ingest after insert in some cases
                // flush to write batch and write to state store
                let mut write_batch = vec![];
                managed_state.flush(&mut write_batch).unwrap();
                store.ingest_batch(write_batch).await.unwrap();
            }

            managed_state
                .apply_batch(&[Op::Delete; 6], None, &[&value_buffer])
                .await
                .unwrap();

            // flush to write batch and write to state store
            let mut write_batch = vec![];
            managed_state.flush(&mut write_batch).unwrap();
            store.ingest_batch(write_batch).await.unwrap();

            // The minimum should be 0
            assert_eq!(
                managed_state.get_output().await.unwrap(),
                Some(ScalarImpl::Int64(6))
            );
        }
    }

    #[tokio::test]
    async fn chaos_test() {
        let mut rng = thread_rng();
        let mut values_to_insert = (0..5000i64).collect_vec();
        values_to_insert.shuffle(&mut rng);
        let mut remaining_values = &values_to_insert[..];

        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state =
            ManagedMinState::<_, i64>::new(keyspace, Int64Type::create(false), Some(3))
                .await
                .unwrap();

        let mut heap = BTreeSet::new();

        loop {
            let insert_cnt = rng.gen_range(1..10);
            let ops = vec![Op::Insert; insert_cnt];
            if remaining_values.len() < insert_cnt {
                break;
            }
            let batch = &remaining_values[..insert_cnt];
            let arr = I64Array::from_slice(&batch.iter().map(|x| Some(*x)).collect_vec()).unwrap();
            for data in batch {
                heap.insert(*data);
            }

            managed_state
                .apply_batch(&ops, None, &[&arr.into()])
                .await
                .unwrap();

            remaining_values = &remaining_values[insert_cnt..];

            let delete_cnt = rng.gen_range(1..10.min(heap.len()));
            let ops = vec![Op::Delete; delete_cnt];
            let mut delete_ids = (0..heap.len()).collect_vec();
            delete_ids.shuffle(&mut rng);
            let to_be_delete_idx: HashSet<usize> =
                delete_ids.iter().take(delete_cnt).cloned().collect();
            let mut to_be_delete = vec![];

            for (idx, item) in heap.iter().enumerate() {
                if to_be_delete_idx.contains(&idx) {
                    to_be_delete.push(*item);
                }
            }

            for item in &to_be_delete {
                heap.remove(item);
            }

            assert_eq!(to_be_delete.len(), delete_cnt);

            let arr =
                I64Array::from_slice(&to_be_delete.iter().map(|x| Some(*x)).collect_vec()).unwrap();
            managed_state
                .apply_batch(&ops, None, &[&arr.into()])
                .await
                .unwrap();

            // flush to write batch and write to state store
            let mut write_batch = vec![];
            managed_state.flush(&mut write_batch).unwrap();
            store.ingest_batch(write_batch).await.unwrap();

            let value = managed_state
                .get_output()
                .await
                .unwrap()
                .map(|x| x.into_int64());

            assert_eq!(value, heap.first().cloned());
        }
    }
}
