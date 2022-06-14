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

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use log::trace;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnDesc, ColumnId, OrderedColumnDesc, Schema};
use risingwave_common::error::RwError;
use risingwave_common::types::Datum;
use risingwave_common::util::hash_util::CRC32FastBuilder;
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::{next_key, range_of_prefix};

use super::mem_table::RowOp;
use super::TableIter;
use crate::cell_based_row_deserializer::{
    make_column_desc_index, CellBasedRowDeserializer, ColumnDescMapping,
};
use crate::cell_deserializer::CellDeserializer;
use crate::cell_serializer::CellSerializer;
use crate::cell_based_row_serializer::CellBasedRowSerializer;
use crate::error::{StorageError, StorageResult};
use crate::keyspace::StripPrefixIterator;
use crate::storage_value::{StorageValue, ValueMeta};
use crate::{Keyspace, StateStore, StateStoreIter};

/// `CellBasedTable` is the interface accessing relational data in KV(`StateStore`) with encoding
/// format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
#[derive(Clone)]
pub struct CellBasedTable<S: StateStore, SER: CellSerializer, DE: CellDeserializer> {
    /// The keyspace that the pk and value of the original table has.
    keyspace: Keyspace<S>,

    /// The schema of this table viewed by some source executor, e.g. RowSeqScanExecutor.
    schema: Schema,

    /// `ColumnDesc` contains strictly more info than `schema`.
    column_descs: Vec<ColumnDesc>,

    /// Mapping from column id to column index
    pk_serializer: OrderedRowSerializer,

    /// Used for serializing the row.
    cell_based_row_serializer: CellBasedRowSerializer,

    /// Used for deserializing the row.
    mapping: Arc<ColumnDescMapping>,

    cell_serializer: SER,

    cell_deserializer: DE,

    column_ids: Vec<ColumnId>,

    /// Indices of distribution keys in full row for computing value meta. None if value meta is
    /// not required.
    dist_key_indices: Option<Vec<usize>>,
}


impl<S: StateStore, SER: CellSerializer, DE: CellDeserializer> std::fmt::Debug for CellBasedTable<S, SER, DE> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellBasedTable")
            .field("column_descs", &self.column_descs)
            .finish()
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::CellBasedTable(rw.into())
}

impl<S: StateStore, SER: CellSerializer, DE: CellDeserializer> CellBasedTable<S, SER, DE> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        dist_key_indices: Option<Vec<usize>>,
    ) -> Self {
        let schema = Schema::new(column_descs.iter().map(Into::into).collect_vec());
        let column_ids = column_descs.iter().map(|d| d.column_id).collect();
        let pk_serializer = OrderedRowSerializer::new(order_types);

        Self {
            keyspace,
            schema,
            mapping: Arc::new(make_column_desc_index(column_descs.clone())),
            column_descs,
            pk_serializer,
            cell_based_row_serializer: CellBasedRowSerializer::new(),
            column_ids,
            dist_key_indices,
            cell_serializer: todo!(),
            cell_deserializer: todo!(),
        }
    }

    pub fn new_for_test(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
    ) -> Self {
        Self::new(keyspace, column_descs, order_types, None)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(super) fn pk_serializer(&self) -> &OrderedRowSerializer {
        &self.pk_serializer
    }
}

/// Get & Write
impl<S: StateStore> CellBasedTable<S> {
    /// Get a single row by point get
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        // TODO: use multi-get for cell_based get_row
        // TODO: encode vnode into key
        // let vnode = self.compute_vnode_by_row(pk);
        let serialized_pk = serialize_pk(pk, &self.pk_serializer);

        let sentinel_key =
            serialize_pk_and_column_id(&serialized_pk, &SENTINEL_CELL_ID).map_err(err)?;
        if self.keyspace.get(&sentinel_key, epoch).await?.is_none() {
            // if sentinel cell is none, this row doesn't exist
            return Ok(None);
        };

        let mut row_deserializer = CellBasedRowDeserializer::new(&*self.mapping);
        for column_id in &self.column_ids {
            let key = serialize_pk_and_column_id(&serialized_pk, column_id).map_err(err)?;
            if let Some(value) = self.keyspace.get(&key, epoch).await? {
                let deserialize_res = row_deserializer.deserialize(&key, &value).map_err(err)?;
                assert!(deserialize_res.is_none());
            }
        }

        let pk_and_row = row_deserializer.take();
        Ok(pk_and_row.map(|(_pk, row)| row))
    }

    /// Get vnode value. Should provide a full row (instead of pk).
    fn compute_vnode_by_row(&self, value: &Row) -> u16 {
        let dist_key_indices = self.dist_key_indices.as_ref().unwrap();

        let hash_builder = CRC32FastBuilder {};
        value
            .hash_by_indices(dist_key_indices, &hash_builder)
            .unwrap()
            .to_vnode()
    }

    /// Get a single row by range scan
    pub async fn get_row_by_scan(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        // get row by state_store scan
        // TODO: encode vnode into key
        // let vnode = self.compute_vnode_by_row(value);
        let start_key = serialize_pk(pk, &self.pk_serializer);
        let key_range = range_of_prefix(&start_key);

        let kv_pairs = self
            .keyspace
            .scan_with_range(key_range, None, epoch)
            .await?;

        let mut deserializer = CellBasedRowDeserializer::new(&*self.mapping);
        for (key, value) in kv_pairs {
            deserializer.deserialize(&key, &value).map_err(err)?;
        }

        let pk_and_row = deserializer.take();
        Ok(pk_and_row.map(|(_pk, row)| row))
    }

    async fn batch_write_rows_inner<const WITH_VALUE_META: bool>(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        // stateful executors need to compute vnode.
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);

        for (pk, row_op) in buffer {
            // If value meta is computed here, then the cell based table is guaranteed to have
            // distribution keys. Also, it is guaranteed that distribution key indices will
            // not exceed the length of pk. So we simply do unwrap here.
            match row_op {
                RowOp::Insert(row) => {
                    let value_meta = if WITH_VALUE_META {
                        ValueMeta::with_vnode(self.compute_vnode_by_row(&row))
                    } else {
                        ValueMeta::default()
                    };
                    let bytes = self
                        .cell_based_row_serializer
                        .serialize(&pk, row, &self.column_ids)
                        .map_err(err)?;
                    for (key, value) in bytes {
                        local.put(key, StorageValue::new_put(value_meta, value))
                    }
                }
                RowOp::Delete(old_row) => {
                    // TODO(wcy-fdu): only serialize key on deletion
                    let value_meta = if WITH_VALUE_META {
                        ValueMeta::with_vnode(self.compute_vnode_by_row(&old_row))
                    } else {
                        ValueMeta::default()
                    };
                    let bytes = self
                        .cell_based_row_serializer
                        .serialize(&pk, old_row, &self.column_ids)
                        .map_err(err)?;
                    for (key, _) in bytes {
                        local.delete_with_value_meta(key, value_meta);
                    }
                }
                RowOp::Update((old_row, new_row)) => {
                    let value_meta = if WITH_VALUE_META {
                        ValueMeta::with_vnode(self.compute_vnode_by_row(&new_row))
                    } else {
                        ValueMeta::default()
                    };
                    let delete_bytes = self
                        .cell_based_row_serializer
                        .serialize_without_filter(&pk, old_row, &self.column_ids)
                        .map_err(err)?;
                    let insert_bytes = self
                        .cell_based_row_serializer
                        .serialize_without_filter(&pk, new_row, &self.column_ids)
                        .map_err(err)?;
                    for (delete, insert) in
                        delete_bytes.into_iter().zip_eq(insert_bytes.into_iter())
                    {
                        match (delete, insert) {
                            (Some((delete_pk, _)), None) => {
                                local.delete_with_value_meta(delete_pk, value_meta);
                            }
                            (None, Some((insert_pk, insert_row))) => {
                                local.put(insert_pk, StorageValue::new_put(value_meta, insert_row));
                            }
                            (None, None) => {}
                            (Some((delete_pk, _)), Some((insert_pk, insert_row))) => {
                                debug_assert_eq!(delete_pk, insert_pk);
                                local.put(insert_pk, StorageValue::new_put(value_meta, insert_row));
                            }
                        }
                    }
                }
            }
        }
        batch.ingest(epoch).await?;
        Ok(())
    }

    /// Write to state store, and use distribution key indices to compute value meta
    pub async fn batch_write_rows_with_value_meta(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        self.batch_write_rows_inner::<true>(buffer, epoch).await
    }

    /// Write to state store without value meta
    pub async fn batch_write_rows(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        self.batch_write_rows_inner::<false>(buffer, epoch).await
    }
}

pub trait PkAndRowStream = Stream<Item = StorageResult<(Vec<u8>, Row)>> + Send;

/// The [`CellBasedIter`] used in streaming executor.
pub type StreamingIter<S: StateStore> = impl PkAndRowStream;
/// The [`CellBasedIter`] used in batch executor, which will wait for the epoch before iteration.
pub type BatchIter<S: StateStore> = impl PkAndRowStream;
/// The [`CellBasedIter`] used in batch executor if pk is not persisted, which will wait for the
/// epoch before iteration.
pub type BatchDedupPkIter<S: StateStore> = impl PkAndRowStream;

#[async_trait::async_trait]
impl<S: PkAndRowStream + Unpin> TableIter for S {
    async fn next_row(&mut self) -> StorageResult<Option<Row>> {
        self.next()
            .await
            .transpose()
            .map(|r| r.map(|(_pk, row)| row))
    }
}

/// Iterators
impl<S: StateStore> CellBasedTable<S> {
    /// Get a [`StreamingIter`] with given `encoded_key_range`.
    pub(super) async fn streaming_iter_with_encoded_key_range<R, B>(
        &self,
        encoded_key_range: R,
        epoch: u64,
    ) -> StorageResult<StreamingIter<S>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        Ok(CellBasedIter::<_, STREAMING_ITER_TYPE>::new(
            &self.keyspace,
            self.mapping.clone(),
            encoded_key_range,
            epoch,
        )
        .await?
        .into_stream())
    }

    /// Get a [`BatchIter`] with given `encoded_key_range`.
    pub(super) async fn batch_iter_with_encoded_key_range<R, B>(
        &self,
        encoded_key_range: R,
        epoch: u64,
    ) -> StorageResult<BatchIter<S>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        Ok(CellBasedIter::<_, BATCH_ITER_TYPE>::new(
            &self.keyspace,
            self.mapping.clone(),
            encoded_key_range,
            epoch,
        )
        .await?
        .into_stream())
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    pub async fn batch_iter(&self, epoch: u64) -> StorageResult<BatchIter<S>> {
        self.batch_iter_with_encoded_key_range::<_, &[u8]>(.., epoch)
            .await
    }

    /// `dedup_pk_iter` should be used when pk is not persisted as value in storage.
    /// It will attempt to decode pk from key instead of cell value.
    /// Tracking issue: <https://github.com/singularity-data/risingwave/issues/588>
    pub async fn batch_dedup_pk_iter(
        &self,
        epoch: u64,
        // TODO: remove this parameter: https://github.com/singularity-data/risingwave/issues/3203
        pk_descs: &[OrderedColumnDesc],
    ) -> StorageResult<BatchDedupPkIter<S>> {
        Ok(DedupPkCellBasedIter::new(
            self.batch_iter(epoch).await?,
            self.mapping.clone(),
            pk_descs,
        )
        .await?
        .into_stream())
    }

    pub async fn batch_iter_with_pk_bounds(
        &self,
        epoch: u64,
        pk_prefix: Row,
        next_col_bounds: impl RangeBounds<Datum>,
    ) -> StorageResult<BatchIter<S>> {
        fn serialize_pk_bound(
            pk_serializer: &OrderedRowSerializer,
            pk_prefix: &Row,
            next_col_bound: Bound<&Datum>,
            is_start_bound: bool,
        ) -> Bound<Vec<u8>> {
            match next_col_bound {
                Included(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size() + 1);
                    let mut key = pk_prefix.clone();
                    key.0.push(k.clone());
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        Included(serialized_key)
                    } else {
                        // Should use excluded next key for end bound.
                        // Otherwise keys starting with the bound is not included.
                        Excluded(next_key(&serialized_key))
                    }
                }
                Excluded(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size() + 1);
                    let mut key = pk_prefix.clone();
                    key.0.push(k.clone());
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        // storage doesn't support excluded begin key yet, so transform it to
                        // included
                        Included(next_key(&serialized_key))
                    } else {
                        Excluded(serialized_key)
                    }
                }
                Unbounded => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.size());
                    let serialized_pk_prefix = serialize_pk(pk_prefix, &pk_prefix_serializer);
                    if pk_prefix.size() == 0 {
                        Unbounded
                    } else if is_start_bound {
                        Included(serialized_pk_prefix)
                    } else {
                        Excluded(next_key(&serialized_pk_prefix))
                    }
                }
            }
        }

        let start_key = serialize_pk_bound(
            &self.pk_serializer,
            &pk_prefix,
            next_col_bounds.start_bound(),
            true,
        );
        let end_key = serialize_pk_bound(
            &self.pk_serializer,
            &pk_prefix,
            next_col_bounds.end_bound(),
            false,
        );

        trace!(
            "iter_with_pk_bounds: start_key: {:?}, end_key: {:?}",
            start_key,
            end_key
        );

        self.batch_iter_with_encoded_key_range((start_key, end_key), epoch)
            .await
    }

    pub async fn batch_iter_with_pk_prefix(
        &self,
        epoch: u64,
        pk_prefix: Row,
    ) -> StorageResult<BatchIter<S>> {
        let prefix_serializer = self.pk_serializer.prefix(pk_prefix.size());
        let serialized_pk_prefix = serialize_pk(&pk_prefix, &prefix_serializer);

        let key_range = range_of_prefix(&serialized_pk_prefix);

        trace!(
            "iter_with_pk_prefix: key_range {:?}",
            (key_range.start_bound(), key_range.end_bound())
        );

        self.batch_iter_with_encoded_key_range(key_range, epoch)
            .await
    }
}

const STREAMING_ITER_TYPE: bool = true;
const BATCH_ITER_TYPE: bool = false;

/// [`CellBasedIter`] iterates on the cell-based table.
/// If `ITER_TYPE` is `BATCH`, it will wait for the given epoch to be committed before iteration.
struct CellBasedIter<S: StateStore, const ITER_TYPE: bool> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,

    /// Cell-based row deserializer
    cell_based_row_deserializer: CellBasedRowDeserializer<Arc<ColumnDescMapping>>,
}

impl<S: StateStore, const ITER_TYPE: bool> CellBasedIter<S, ITER_TYPE> {
    async fn new<R, B>(
        keyspace: &Keyspace<S>,
        table_descs: Arc<ColumnDescMapping>,
        encoded_key_range: R,
        epoch: u64,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        if ITER_TYPE == BATCH_ITER_TYPE {
            keyspace.state_store().wait_epoch(epoch).await?;
        }

        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_descs);

        let iter = keyspace.iter_with_range(encoded_key_range, epoch).await?;
        let iter = Self {
            iter,
            cell_based_row_deserializer,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(mut self) {
        while let Some((key, value)) = self.iter.next().await? {
            if let Some(pk_and_row) = self
                .cell_based_row_deserializer
                .deserialize(&key, &value)
                .map_err(err)?
            {
                yield pk_and_row;
            }
        }

        if let Some(pk_and_row) = self.cell_based_row_deserializer.take() {
            yield pk_and_row;
        }
    }
}

/// Provides a layer on top of [`CellBasedIter`]
/// for decoding pk into its constituent datums in a row.
///
/// Given the following row: `| user_id | age | name |`,
/// if pk was derived from `user_id, name`
/// we can decode pk -> `user_id, name`,
/// and retrieve the row: `|_| age |_|`,
/// then fill in empty spots with datum decoded from pk: `| user_id | age | name |`
struct DedupPkCellBasedIter<I> {
    inner: I,
    pk_decoder: OrderedRowDeserializer,

    // Maps pk fields with:
    // 1. same value and memcomparable encoding,
    // 2. corresponding row positions. e.g. _row_id is unlikely to be part of selected row.
    pk_to_row_mapping: Vec<Option<usize>>,
}

impl<I> DedupPkCellBasedIter<I> {
    async fn new(
        inner: I,
        table_descs: Arc<ColumnDescMapping>,
        pk_descs: &[OrderedColumnDesc],
    ) -> StorageResult<Self> {
        let (data_types, order_types) = pk_descs
            .iter()
            .map(|ordered_desc| {
                (
                    ordered_desc.column_desc.data_type.clone(),
                    ordered_desc.order,
                )
            })
            .unzip();
        let pk_decoder = OrderedRowDeserializer::new(data_types, order_types);

        // TODO: pre-calculate this instead of calculate it every time when creating new iterator
        let col_id_to_row_idx: HashMap<ColumnId, usize> = table_descs
            .iter()
            .map(|(column_id, (_, idx))| (*column_id, *idx))
            .collect();

        let pk_to_row_mapping = pk_descs
            .iter()
            .map(|d| {
                let column_desc = &d.column_desc;
                if column_desc.data_type.mem_cmp_eq_value_enc() {
                    col_id_to_row_idx.get(&column_desc.column_id).copied()
                } else {
                    None
                }
            })
            .collect();

        Ok(Self {
            inner,
            pk_decoder,
            pk_to_row_mapping,
        })
    }
}

impl<I: PkAndRowStream> DedupPkCellBasedIter<I> {
    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(self) {
        #[for_await]
        for r in self.inner {
            let (pk_vec, Row(mut row_inner)) = r?;
            let pk_decoded = self.pk_decoder.deserialize(&pk_vec).map_err(err)?;
            for (pk_idx, datum) in pk_decoded.into_vec().into_iter().enumerate() {
                if let Some(row_idx) = self.pk_to_row_mapping[pk_idx] {
                    row_inner[row_idx] = datum;
                }
            }
            yield (pk_vec, Row(row_inner));
        }
    }
}
