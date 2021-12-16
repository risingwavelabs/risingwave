use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::array::stream_chunk::{Op, Ops};
use risingwave_common::array::{Array, ArrayImpl, DecimalArray, I16Array, I32Array, I64Array};
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::Result;
use risingwave_common::expr::AggKind;
use risingwave_common::types::{
    deserialize_datum_not_null_from, serialize_datum_not_null_into, DataTypeKind, DataTypeRef,
    Datum, ScalarImpl, ScalarRef,
};
use risingwave_storage::{Keyspace, StateStore};

use super::extreme_serializer::{variants, ExtremePk, ExtremeSerializer};
use crate::stream_op::state_aggregation::FlushStatus;
use crate::stream_op::{AggArgs, AggCall};

pub type ManagedMinState<S, A> = GenericManagedState<S, A, { variants::EXTREME_MIN }>;
pub type ManagedMaxState<S, A> = GenericManagedState<S, A, { variants::EXTREME_MAX }>;

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
///
/// The `ExtremeState` need some special properties from the storage engine and the executor.
/// * The output of an `ExtremeState` is only correct when all changes have been flushed to the
///   state store.
/// * The `RowIDs` must be i64
pub struct GenericManagedState<S: StateStore, A: Array, const EXTREME_TYPE: usize>
where
    A::OwnedItem: Ord,
{
    /// Top N elements in the state, which stores the mapping of sort key -> (dirty, original
    /// value). This BTreeMap always maintain the elements that we are sure it's top n.
    top_n: BTreeMap<(A::OwnedItem, ExtremePk), ScalarImpl>,

    /// The actions that will be taken on next flush
    flush_buffer: BTreeMap<(A::OwnedItem, ExtremePk), FlushStatus>,

    /// Number of items in the state including those not in top n cache but in state store.
    total_count: usize,

    /// Number of entries to retain in memory after each flush.
    top_n_count: Option<usize>,

    /// Data type of the sort column
    data_type: DataTypeRef,

    /// The keyspace to operate on.
    keyspace: Keyspace<S>,

    /// The sort key serializer
    serializer: ExtremeSerializer<A::OwnedItem, EXTREME_TYPE>,

    /// Length of primary keys
    pk_length: usize,
}

/// A trait over all extreme states.
///
/// It is true that this interface also fits to other managed state, but we won't implement
/// `ManagedExtremeState` for them. We want to reduce the overhead of BoxedFuture. For
/// ManagedValueState, we can directly forward its async functions to `ManagedStateImpl`, instead of
/// adding a layer of indirection caused by async traits.
#[async_trait]
pub trait ManagedExtremeState<S: StateStore>: Send + Sync + 'static {
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()>;

    /// Get the output of the state. Must flush before getting output.
    async fn get_output(&mut self) -> Result<Datum>;

    /// Check if this state needs a flush.
    fn is_dirty(&self) -> bool;

    /// Flush the internal state to a write batch. TODO: add `WriteBatch` to Hummock.
    fn flush(&mut self, write_batch: &mut Vec<(Bytes, Option<Bytes>)>) -> Result<()>;
}

impl<S: StateStore, A: Array, const EXTREME_TYPE: usize> GenericManagedState<S, A, EXTREME_TYPE>
where
    A::OwnedItem: Ord,
    for<'a> &'a A: From<&'a ArrayImpl>,
{
    /// Create a managed min state based on `Keyspace`. When `top_n_count` is `None`, the cache will
    /// always be retained when flushing the managed state. Otherwise, we will only retain n entries
    /// after each flush.
    pub async fn new(
        keyspace: Keyspace<S>,
        data_type: DataTypeRef,
        top_n_count: Option<usize>,
        row_count: usize,
        pk_length: usize,
    ) -> Result<Self> {
        // Create the internal state based on the value we get.
        let kind = data_type.data_type_kind();

        Ok(Self {
            top_n: BTreeMap::new(),
            flush_buffer: BTreeMap::new(),
            total_count: row_count,
            keyspace,
            top_n_count,
            data_type,
            serializer: ExtremeSerializer::new(kind, pk_length),
            pk_length,
        })
    }

    /// Retain only top n elements in the cache
    fn retain_top_n(&mut self) {
        if let Some(count) = self.top_n_count {
            match EXTREME_TYPE {
                variants::EXTREME_MIN => {
                    while self.top_n.len() > count {
                        self.top_n.pop_last();
                    }
                }
                variants::EXTREME_MAX => {
                    while self.top_n.len() > count {
                        self.top_n.pop_first();
                    }
                }
                _ => unimplemented!(),
            }
        }
    }

    /// Apply a batch of data to the state.
    async fn apply_batch_inner(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()> {
        debug_assert!(super::verify_batch(ops, visibility, data));

        // For extreme state, there's only one column for data, and following columns are for
        // primary keys!
        assert_eq!(
            data.len(),
            1 + self.pk_length,
            "mismatched data input with pk_length"
        );

        let data_column: &A = data[0].into();
        let pk_columns = (0..self.pk_length)
            .map(|idx| data[idx + 1].as_int64()) // FIXME: assuming primary keys are i64
            .collect_vec();

        // `self.top_n` only contains parts of the top keys. When applying batch, we only:
        // 1. Insert keys that is in the range of top n, so that we only maintain the top keys we
        // are sure the minimum.
        // 2. Delete keys if exists in top_n cache.
        // If the top n cache becomes empty after applying the batch, we will merge data from
        // flush_buffer and state store when getting the output.

        for (id, (op, key)) in ops.iter().zip(data_column.iter()).enumerate() {
            let visible = visibility.map(|x| x.is_set(id).unwrap()).unwrap_or(true);
            if !visible {
                continue;
            }

            // Assert key is not null
            let key = key.expect("sort key can't be null").to_owned_scalar();

            // Concat pk with the original key to create a composed key
            let composed_key = (
                key.clone(),
                // Collect pk from columns
                pk_columns
                    .iter()
                    .map(|col| col.value_at(id).expect("pk can't be null"))
                    .collect(),
            );

            match op {
                Op::Insert | Op::UpdateInsert => {
                    let mut do_insert = false;
                    if self.total_count == self.top_n.len() {
                        // Data fully cached in memory
                        do_insert = true;
                    } else {
                        match EXTREME_TYPE {
                            variants::EXTREME_MIN => {
                                if let Some((last_key, _)) = self.top_n.last_key_value() {
                                    if &composed_key < last_key {
                                        do_insert = true;
                                    }
                                }
                            }
                            variants::EXTREME_MAX => {
                                if let Some((first_key, _)) = self.top_n.first_key_value() {
                                    if &composed_key > first_key {
                                        do_insert = true;
                                    }
                                }
                            }
                            _ => unimplemented!(),
                        }
                    }

                    let value: ScalarImpl = key.clone().into();

                    if do_insert {
                        self.top_n.insert(composed_key.clone(), value.clone());
                    }

                    FlushStatus::do_insert(self.flush_buffer.entry(composed_key), value);
                    self.total_count += 1;
                }
                Op::Delete | Op::UpdateDelete => {
                    self.top_n.remove(&composed_key);
                    FlushStatus::do_delete(self.flush_buffer.entry(composed_key));
                    self.total_count -= 1;
                }
            }
        }

        self.retain_top_n();

        Ok(())
    }

    fn get_output_from_cache(&self) -> Option<ScalarImpl> {
        match EXTREME_TYPE {
            variants::EXTREME_MIN => {
                if let Some((_, v)) = self.top_n.first_key_value() {
                    return Some(v.clone());
                }
            }
            variants::EXTREME_MAX => {
                if let Some((_, v)) = self.top_n.last_key_value() {
                    return Some(v.clone());
                }
            }
            _ => unimplemented!(),
        }
        None
    }

    async fn get_output_inner(&mut self) -> Result<Datum> {
        // To make things easier, we do not allow get_output before flushing. Otherwise we will need
        // to merge data from flush_buffer and state store, which is hard to implement.
        //
        // To make ExtremeState produce the correct result, the write batch must be flushed into the
        // state store before getting the output. Note that calling `.flush()` is not enough, as it
        // only generates a write batch without flushing to store.
        debug_assert!(!self.is_dirty());

        // Firstly, check if datum is available in cache.
        if let Some(v) = self.get_output_from_cache() {
            Ok(Some(v))
        } else {
            // Then, fetch from the state store.
            //
            // To future developers: please make **SURE** you have taken `EXTREME_TYPE` into
            // account. EXTREME_MIN and EXTREME_MAX will significantly impact the
            // following logic.

            let all_data = self.keyspace.scan_strip_prefix(self.top_n_count).await?;
            let kind = self.data_type.data_type_kind();

            for (raw_key, raw_value) in all_data {
                let mut deserializer = memcomparable::Deserializer::new(&raw_value[..]);
                let value = deserialize_datum_not_null_from(&kind, &mut deserializer)?.unwrap();
                let key = value.clone().try_into().unwrap();
                let pks = self.serializer.get_pk(&raw_key[..])?;
                self.top_n.insert((key, pks), value);
            }

            if let Some(v) = self.get_output_from_cache() {
                Ok(Some(v))
            } else {
                Ok(None)
            }
        }
    }

    /// Flush the internal state to a write batch. TODO: add `WriteBatch` to Hummock.
    fn flush_inner(&mut self, write_batch: &mut Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        // Generally, we require the state the be dirty before flushing. However, it is possible
        // that after a sequence of operations, the flush buffer becomes empty. Then, the
        // state becomes "dirty", but we do not need to flush anything.
        if !self.is_dirty() {
            self.retain_top_n();
            return Ok(());
        }

        // TODO: we can populate the cache while flushing, but that's hard.

        for ((key, pks), v) in std::mem::take(&mut self.flush_buffer) {
            let key_encoded = self.serializer.serialize(key, &pks)?;
            let key_encoded = self.keyspace.prefixed_key(key_encoded);

            match v.into_option() {
                Some(v) => {
                    let mut serializer = memcomparable::Serializer::new(vec![]);
                    serialize_datum_not_null_into(&Some(v), &mut serializer)?;
                    let value = serializer.into_inner();
                    write_batch.push((key_encoded.into(), Some(value.into())));
                }
                None => {
                    write_batch.push((key_encoded.into(), None));
                }
            }
        }

        self.retain_top_n();

        Ok(())
    }
}

#[async_trait]
impl<S: StateStore, A: Array, const EXTREME_TYPE: usize> ManagedExtremeState<S>
    for GenericManagedState<S, A, EXTREME_TYPE>
where
    A::OwnedItem: Ord,
    for<'a> &'a A: From<&'a ArrayImpl>,
{
    async fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &[&ArrayImpl],
    ) -> Result<()> {
        self.apply_batch_inner(ops, visibility, data).await
    }

    async fn get_output(&mut self) -> Result<Datum> {
        self.get_output_inner().await
    }

    /// Check if this state needs a flush.
    fn is_dirty(&self) -> bool {
        !self.flush_buffer.is_empty()
    }

    fn flush(&mut self, write_batch: &mut Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        self.flush_inner(write_batch)
    }
}

impl<S: StateStore, A: Array, const EXTREME_TYPE: usize> GenericManagedState<S, A, EXTREME_TYPE>
where
    A::OwnedItem: Ord,
{
    #[cfg(test)]
    pub async fn iterate_store(&self) -> Result<Vec<(A::OwnedItem, ExtremePk)>> {
        let all_data = self.keyspace.scan_strip_prefix(None).await?;
        let mut result = vec![];
        let _kind = self.data_type.data_type_kind();

        for (raw_key, raw_value) in all_data {
            let mut deserializer = memcomparable::Deserializer::new(&raw_value[..]);
            let value = deserialize_datum_not_null_from(
                &self.data_type.data_type_kind(),
                &mut deserializer,
            )?
            .unwrap();
            let key = value.clone().try_into().unwrap();
            let pks = self.serializer.get_pk(&raw_key[..])?;
            result.push((key, pks));
        }
        Ok(result)
    }

    #[cfg(test)]
    pub async fn iterate_topn_cache(&self) -> Result<Vec<(A::OwnedItem, ExtremePk)>> {
        Ok(self.top_n.iter().map(|(k, _)| k.clone()).collect())
    }
}

pub async fn create_streaming_extreme_state<S: StateStore>(
    agg_call: AggCall,
    keyspace: Keyspace<S>,
    row_count: usize,
    top_n_count: Option<usize>,
    pk_length: usize,
) -> Result<Box<dyn ManagedExtremeState<S>>> {
    match &agg_call.args {
        AggArgs::Unary(x, _) => {
            if agg_call.return_type.data_type_kind() != x.data_type_kind() {
                panic!(
                    "extreme state input doesn't match return value: {:?}",
                    agg_call
                );
            }
            if x.is_nullable() {
                panic!("extreme state input should not be nullable: {:?}", agg_call);
            }
        }
        _ => panic!("extreme state should only have one arg: {:?}", agg_call),
    }

    match (agg_call.kind, agg_call.return_type.data_type_kind()) {
        (AggKind::Max, DataTypeKind::Int64) => Ok(Box::new(
            ManagedMaxState::<_, I64Array>::new(
                keyspace,
                agg_call.return_type.clone(),
                top_n_count,
                row_count,
                pk_length,
            )
            .await?,
        )),
        (AggKind::Max, DataTypeKind::Int32) => Ok(Box::new(
            ManagedMaxState::<_, I32Array>::new(
                keyspace,
                agg_call.return_type.clone(),
                top_n_count,
                row_count,
                pk_length,
            )
            .await?,
        )),
        (AggKind::Max, DataTypeKind::Int16) => Ok(Box::new(
            ManagedMaxState::<_, I16Array>::new(
                keyspace,
                agg_call.return_type.clone(),
                top_n_count,
                row_count,
                pk_length,
            )
            .await?,
        )),
        (AggKind::Max, DataTypeKind::Decimal) => Ok(Box::new(
            ManagedMaxState::<_, DecimalArray>::new(
                keyspace,
                agg_call.return_type.clone(),
                top_n_count,
                row_count,
                pk_length,
            )
            .await?,
        )),
        (AggKind::Min, DataTypeKind::Int64) => Ok(Box::new(
            ManagedMinState::<_, I64Array>::new(
                keyspace,
                agg_call.return_type.clone(),
                top_n_count,
                row_count,
                pk_length,
            )
            .await?,
        )),
        (AggKind::Min, DataTypeKind::Int32) => Ok(Box::new(
            ManagedMinState::<_, I32Array>::new(
                keyspace,
                agg_call.return_type.clone(),
                top_n_count,
                row_count,
                pk_length,
            )
            .await?,
        )),
        (AggKind::Min, DataTypeKind::Int16) => Ok(Box::new(
            ManagedMinState::<_, I16Array>::new(
                keyspace,
                agg_call.return_type.clone(),
                top_n_count,
                row_count,
                pk_length,
            )
            .await?,
        )),
        (AggKind::Min, DataTypeKind::Decimal) => Ok(Box::new(
            ManagedMinState::<_, DecimalArray>::new(
                keyspace,
                agg_call.return_type.clone(),
                top_n_count,
                row_count,
                pk_length,
            )
            .await?,
        )),
        _ => unreachable!("unsupported"),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};

    use itertools::Itertools;
    use rand::prelude::*;
    use risingwave_common::array::{I64Array, Op};
    use risingwave_common::types::{Int64Type, ScalarImpl};
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;

    #[tokio::test]
    async fn test_managed_extreme_state() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state =
            ManagedMinState::<_, I64Array>::new(keyspace, Int64Type::create(false), Some(5), 0, 0)
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

        let row_count = managed_state.total_count;

        // test recovery
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state = ManagedMinState::<_, I64Array>::new(
            keyspace,
            Int64Type::create(false),
            Some(5),
            row_count,
            0,
        )
        .await
        .unwrap();

        // The minimum should still be 30
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(30))
        );
    }

    #[tokio::test]
    async fn test_replicated_value_min() {
        test_replicated_value::<{ variants::EXTREME_MIN }>().await
    }

    #[tokio::test]
    async fn test_replicated_value_max() {
        test_replicated_value::<{ variants::EXTREME_MAX }>().await
    }

    async fn test_replicated_value<const EXTREME_TYPE: usize>() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());

        let pk_length = 1;
        let mut managed_state = GenericManagedState::<_, I64Array, EXTREME_TYPE>::new(
            keyspace,
            Int64Type::create(false),
            Some(3),
            0,
            pk_length,
        )
        .await
        .unwrap();
        assert!(!managed_state.is_dirty());

        let value_buffer =
            I64Array::from_slice(&[Some(1), Some(1), Some(4), Some(5), Some(1), Some(4)])
                .unwrap()
                .into();

        let pk_buffer = I64Array::from_slice(&[
            Some(1001),
            Some(1002),
            Some(1003),
            Some(1004),
            Some(1005),
            Some(1006),
        ])
        .unwrap()
        .into();

        let extreme = ScalarImpl::Int64(match EXTREME_TYPE {
            variants::EXTREME_MIN => 1,
            variants::EXTREME_MAX => 5,
            _ => unreachable!(),
        });

        // insert 1, 5
        managed_state
            .apply_batch(
                &[Op::Insert; 2],
                None,
                &[
                    &I64Array::from_slice(&[Some(1), Some(5)]).unwrap().into(),
                    &I64Array::from_slice(&[Some(2001), Some(2002)])
                        .unwrap()
                        .into(),
                ],
            )
            .await
            .unwrap();

        // insert 1 1 4 5 1 4
        managed_state
            .apply_batch(&[Op::Insert; 6], None, &[&value_buffer, &pk_buffer])
            .await
            .unwrap();

        // flush
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();

        // The minimum should be 1, or the maximum should be 5
        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(extreme.clone())
        );

        // delete 1 1 4 5 1 4
        managed_state
            .apply_batch(&[Op::Delete; 6], None, &[&value_buffer, &pk_buffer])
            .await
            .unwrap();

        // flush
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();

        // The minimum should still be 1, or the maximum should still be 5
        assert_eq!(managed_state.get_output().await.unwrap(), Some(extreme));
    }

    #[tokio::test]
    async fn test_same_group_of_value() {
        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state =
            ManagedMinState::<_, I64Array>::new(keyspace, Int64Type::create(false), Some(3), 0, 0)
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

            // The minimum should be 6
            assert_eq!(
                managed_state.get_output().await.unwrap(),
                Some(ScalarImpl::Int64(6))
            );
        }
    }

    #[tokio::test]
    async fn chaos_test_min() {
        chaos_test::<{ variants::EXTREME_MIN }>().await;
    }

    #[tokio::test]
    async fn chaos_test_max() {
        chaos_test::<{ variants::EXTREME_MAX }>().await;
    }

    async fn chaos_test<const EXTREME_TYPE: usize>() {
        let mut rng = thread_rng();
        let mut values_to_insert = (0..5000i64).collect_vec();
        values_to_insert.shuffle(&mut rng);
        let mut remaining_values = &values_to_insert[..];

        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state = GenericManagedState::<_, I64Array, EXTREME_TYPE>::new(
            keyspace,
            Int64Type::create(false),
            Some(3),
            0,
            0,
        )
        .await
        .unwrap();

        let mut heap = BTreeSet::new();

        loop {
            let insert_cnt = rng.gen_range(1..=10);
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

            let delete_cnt = rng.gen_range(1..=10.min(heap.len()));
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

            match EXTREME_TYPE {
                variants::EXTREME_MAX => assert_eq!(value, heap.last().cloned()),
                variants::EXTREME_MIN => assert_eq!(value, heap.first().cloned()),
                _ => unimplemented!(),
            }
        }
    }

    async fn helper_flush<S: StateStore>(
        managed_state: &mut impl ManagedExtremeState<S>,
        store: &S,
    ) {
        let mut write_batch = vec![];
        managed_state.flush(&mut write_batch).unwrap();
        store.ingest_batch(write_batch).await.unwrap();
    }

    #[tokio::test]
    async fn test_same_value_delete() {
        // In this test, we test this case:
        //
        // Delete 6, insert 6, and delete 6 in one epoch.
        // The 6 should be deleted from the state store.

        let store = MemoryStateStore::new();
        let keyspace = Keyspace::new(store.clone(), b"233333".to_vec());
        let mut managed_state =
            ManagedMinState::<_, I64Array>::new(keyspace, Int64Type::create(false), Some(3), 0, 0)
                .await
                .unwrap();
        assert!(!managed_state.is_dirty());

        let value_buffer =
            I64Array::from_slice(&[Some(1), Some(2), Some(3), Some(4), Some(5), Some(7)])
                .unwrap()
                .into();

        managed_state
            .apply_batch(&[Op::Insert; 6], None, &[&value_buffer])
            .await
            .unwrap();

        managed_state
            .apply_batch(
                &[Op::Insert],
                None,
                &[&I64Array::from_slice(&[Some(6)]).unwrap().into()],
            )
            .await
            .unwrap();

        // Now we have 1 to 7 in the state store.
        helper_flush(&mut managed_state, &store).await;

        // Delete 6, insert 6, delete 6
        managed_state
            .apply_batch(
                &[Op::Delete, Op::Insert, Op::Delete],
                None,
                &[&I64Array::from_slice(&[Some(6), Some(6), Some(6)])
                    .unwrap()
                    .into()],
            )
            .await
            .unwrap();

        // 6 should be deleted by now
        helper_flush(&mut managed_state, &store).await;

        let value_buffer = I64Array::from_slice(&[Some(1), Some(2), Some(3), Some(4), Some(5)])
            .unwrap()
            .into();

        // delete all remaining items
        managed_state
            .apply_batch(&[Op::Delete; 5], None, &[&value_buffer])
            .await
            .unwrap();

        helper_flush(&mut managed_state, &store).await;

        assert_eq!(
            managed_state.get_output().await.unwrap(),
            Some(ScalarImpl::Int64(7))
        );
    }
}
