use std::collections::BTreeMap;

use bytes::Bytes;
use risingwave_common::array::stream_chunk::{Op, Ops};
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::{serialize_datum_not_null_into, Datum, Scalar, ScalarImpl};

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
pub struct ManagedMinState<S: StateStore, K: Scalar + Ord> {
    /// Top N elements in the state, which stores the mapping of sort key -> (dirty, original
    /// value). This BTreeMap always maintain the elements that we are sure it's top n.
    top_n: BTreeMap<K, ScalarImpl>,

    /// The actions that will be taken on next flush
    flush_buffer: BTreeMap<K, Option<ScalarImpl>>,

    /// Number of items in the state including those not in top n cache but in state store.
    total_count: usize,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,
}

impl<S: StateStore, K: Scalar + Ord> ManagedMinState<S, K> {
    /// Create a single-value managed state based on `AggCall` and `Keyspace`.
    pub async fn new(keyspace: Keyspace<S>) -> Result<Self> {
        // TODO: use the RowCount from HashAgg instead of fetching all data from the keyspace.
        // This is critical to performance.
        let total_count = keyspace.scan(None).await?.len();
        // Create the internal state based on the value we get.
        Ok(Self {
            top_n: BTreeMap::new(),
            flush_buffer: BTreeMap::new(),
            total_count,
            keyspace,
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
            todo!()
        }

        Ok(())
    }

    /// Fetch `scan_size` elements from the underlying storage and add them to the `BTreeMap`. It is
    /// possible that all data from the state store are already stored in the top N cache, and
    /// therefore there's no change.
    async fn fill_cache(&mut self, _scan_size: usize) -> Result<()> {
        todo!();
    }

    pub async fn get_output(&mut self) -> Result<Datum> {
        if let Some((_, v)) = self.top_n.first_key_value() {
            Ok(Some(v.clone()))
        } else {
            Ok(None)
        }
    }

    /// Check if this state needs a flush.
    pub fn is_dirty(&self) -> bool {
        !self.flush_buffer.is_empty()
    }

    /// Flush the internal state to a write batch. TODO: add `WriteBatch` to Hummock.
    pub fn flush(&mut self, write_batch: &mut Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        debug_assert!(self.is_dirty());

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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::ScalarImpl;

    use super::*;
    use crate::stream_op::keyspace::MemoryStateStore;
    use risingwave_common::array::{I64Array, Op};

    #[tokio::test]
    async fn test_managed_value_state() {
        let store = MemoryStateStore::default();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state = ManagedMinState::<_, i64>::new(keyspace).await.unwrap();
        assert!(!managed_state.is_dirty());

        // apply a batch and get the output
        managed_state
            .apply_batch(
                &[Op::Insert, Op::Insert, Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(0), Some(1), Some(2)])
                    .unwrap()
                    .into()],
            )
            .await
            .unwrap();
        assert!(managed_state.is_dirty());
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(0))
        );

        // flush to write batch and write to state store
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();
    }
}
