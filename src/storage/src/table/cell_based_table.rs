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

use bytes::Bytes;
use futures_async_stream::try_stream;
use itertools::Itertools;
use log::trace;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, Row};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, OrderedColumnDesc, Schema};
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
use crate::cell_based_row_serializer::CellBasedRowSerializer;
use crate::error::{StorageError, StorageResult};
use crate::keyspace::StripPrefixIterator;
use crate::storage_value::{StorageValue, ValueMeta};
use crate::{Keyspace, StateStore, StateStoreIter};

/// `CellBasedTable` is the interface accessing relational data in KV(`StateStore`) with encoding
/// format: [keyspace | pk | `column_id` (4B)] -> value.
/// if the key of the column id does not exist, it will be Null in the relation
#[derive(Clone)]
pub struct CellBasedTable<S: StateStore> {
    /// The keyspace that the pk and value of the original table has.
    keyspace: Keyspace<S>,

    /// The schema of this table viewed by some source executor, e.g. RowSeqScanExecutor.
    schema: Schema,

    /// `ColumnDesc` contains strictly more info than `schema`.
    column_descs: Vec<ColumnDesc>,

    /// Mapping from column id to column index
    /// TODO: all CellBasedTable users should have a pk serializer, we should remove this option.
    pub pk_serializer: Option<OrderedRowSerializer>,

    /// Used for serializing the row.
    cell_based_row_serializer: CellBasedRowSerializer,

    /// Used for deserializing the row.
    mapping: Arc<ColumnDescMapping>,

    column_ids: Vec<ColumnId>,

    /// Indices of distribution keys in full row for computing value meta. None if value meta is
    /// not required.
    dist_key_indices: Option<Vec<usize>>,
}

impl<S: StateStore> std::fmt::Debug for CellBasedTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellBasedTable")
            .field("column_descs", &self.column_descs)
            .finish()
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::CellBasedTable(rw.into())
}

impl<S: StateStore> CellBasedTable<S> {
    pub fn new(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        ordered_row_serializer: Option<OrderedRowSerializer>,
        dist_key_indices: Option<Vec<usize>>,
    ) -> Self {
        let schema = Schema::new(
            column_descs
                .iter()
                .map(|cd| Field::with_name(cd.data_type.clone(), cd.name.clone()))
                .collect_vec(),
        );
        let column_ids = generate_column_id(&column_descs);

        Self {
            keyspace,
            schema,
            mapping: Arc::new(make_column_desc_index(column_descs.clone())),
            column_descs,
            pk_serializer: ordered_row_serializer,
            cell_based_row_serializer: CellBasedRowSerializer::new(),
            column_ids,
            dist_key_indices,
        }
    }

    pub fn new_for_test(
        keyspace: Keyspace<S>,
        column_descs: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
    ) -> Self {
        Self::new(
            keyspace,
            column_descs,
            Some(OrderedRowSerializer::new(order_types)),
            None,
        )
    }

    /// Get a single row by point get
    pub async fn get_row(&self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        // TODO: use multi-get for cell_based get_row
        // TODO: encode vnode into key
        // let vnode = self.compute_vnode_by_row(pk);
        let pk_serializer = self.pk_serializer.as_ref().expect("pk_serializer is None");
        let serialized_pk = serialize_pk(pk, pk_serializer);
        let sentinel_key =
            serialize_pk_and_column_id(&serialized_pk, &SENTINEL_CELL_ID).map_err(err)?;
        let mut get_res = Vec::new();

        let sentinel_cell = self.keyspace.get(&sentinel_key, epoch).await?;

        if sentinel_cell.is_none() {
            // if sentinel cell is none, this row doesn't exist
            return Ok(None);
        } else {
            get_res.push((sentinel_key, sentinel_cell.unwrap()));
        }
        for column_id in &self.column_ids {
            let key = serialize_pk_and_column_id(&serialized_pk, column_id).map_err(err)?;

            let state_store_get_res = self.keyspace.get(&key, epoch).await?;
            if let Some(state_store_get_res) = state_store_get_res {
                get_res.push((key, state_store_get_res));
            }
        }
        let mut cell_based_row_deserializer = CellBasedRowDeserializer::new(&*self.mapping);
        for (key, value) in get_res {
            let deserialize_res = cell_based_row_deserializer
                .deserialize(&Bytes::from(key), &value)
                .map_err(err)?;
            assert!(deserialize_res.is_none());
        }
        let pk_and_row = cell_based_row_deserializer.take();
        Ok(pk_and_row.map(|(_pk, row)| row))
    }

    /// Get vnode value. Should provide a full row (instead of pk).
    fn compute_vnode_by_value(&self, value: &Row) -> u16 {
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
        let pk_serializer = self.pk_serializer.as_ref().expect("pk_serializer is None");
        let start_key = serialize_pk(pk, pk_serializer);
        let key_range = range_of_prefix(&start_key);

        let state_store_range_scan_res = self
            .keyspace
            .scan_with_range(key_range, None, epoch)
            .await?;
        let mut cell_based_row_deserializer = CellBasedRowDeserializer::new(&*self.mapping);
        for (key, value) in state_store_range_scan_res {
            cell_based_row_deserializer
                .deserialize(&key, &value)
                .map_err(err)?;
        }
        let pk_and_row = cell_based_row_deserializer.take();
        match pk_and_row {
            Some(_) => Ok(pk_and_row.map(|(_pk, row)| row)),
            None => Ok(None),
        }
    }

    async fn batch_write_rows_inner<const WITH_VALUE_META: bool>(
        &mut self,
        buffer: BTreeMap<Vec<u8>, RowOp>,
        epoch: u64,
    ) -> StorageResult<()> {
        // stateful executors need to compute vnode.
        let mut batch = self.keyspace.state_store().start_write_batch();
        let mut local = batch.prefixify(&self.keyspace);
        let hash_builder = CRC32FastBuilder {};
        for (pk, row_op) in buffer {
            // If value meta is computed here, then the cell based table is guaranteed to have
            // distribution keys. Also, it is guaranteed that distribution key indices will
            // not exceed the length of pk. So we simply do unwrap here.
            match row_op {
                RowOp::Insert(row) => {
                    let value_meta = if WITH_VALUE_META {
                        let vnode = self.compute_vnode_by_value(&row);
                        ValueMeta::with_vnode(vnode)
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
                        let vnode = old_row
                            .hash_by_indices(self.dist_key_indices.as_ref().unwrap(), &hash_builder)
                            .unwrap()
                            .to_vnode();
                        ValueMeta::with_vnode(vnode)
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
                        let vnode = new_row
                            .hash_by_indices(self.dist_key_indices.as_ref().unwrap(), &hash_builder)
                            .unwrap()
                            .to_vnode();
                        ValueMeta::with_vnode(vnode)
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

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`
    pub async fn iter(&self, epoch: u64) -> StorageResult<CellBasedTableRowIter<S>> {
        CellBasedTableRowIter::new(&self.keyspace, self.mapping.clone(), epoch).await
    }

    /// `dedup_pk_iter` should be used when pk is not persisted as value in storage.
    /// It will attempt to decode pk from key instead of cell value.
    /// Tracking issue: <https://github.com/singularity-data/risingwave/issues/588>
    pub async fn dedup_pk_iter(
        &self,
        epoch: u64,
        // TODO: remove this parameter: https://github.com/singularity-data/risingwave/issues/3203
        pk_descs: &[OrderedColumnDesc],
    ) -> StorageResult<DedupPkCellBasedTableRowIter<S>> {
        DedupPkCellBasedTableRowIter::new(
            self.keyspace.clone(),
            self.mapping.clone(),
            epoch,
            pk_descs,
        )
        .await
    }

    fn serialize_pk_bound(
        &self,
        pk_prefix: &Row,
        next_col_bound: Bound<&Datum>,
        is_start_bound: bool,
    ) -> Bound<Vec<u8>> {
        let pk_serializer = self.pk_serializer.as_ref().expect("pk_serializer is None");
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
                    // storage doesn't support excluded begin key yet, so transform it to included
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

    pub async fn iter_with_pk_bounds(
        &self,
        epoch: u64,
        pk_prefix: Row,
        next_col_bounds: impl RangeBounds<Datum>,
    ) -> StorageResult<CellBasedTableRowIter<S>> {
        let start_key = self.serialize_pk_bound(&pk_prefix, next_col_bounds.start_bound(), true);
        let end_key = self.serialize_pk_bound(&pk_prefix, next_col_bounds.end_bound(), false);

        trace!(
            "iter_with_pk_bounds: start_key: {:?}, end_key: {:?}",
            start_key,
            end_key
        );

        CellBasedTableRowIter::new_with_bounds(
            &self.keyspace,
            self.mapping.clone(),
            (start_key, end_key),
            epoch,
        )
        .await
    }

    pub async fn iter_with_pk_prefix(
        &self,
        epoch: u64,
        pk_prefix: Row,
    ) -> StorageResult<CellBasedTableRowIter<S>> {
        let pk_serializer = self.pk_serializer.as_ref().expect("pk_serializer is None");
        let prefix_serializer = pk_serializer.prefix(pk_prefix.size());
        let serialized_pk_prefix = serialize_pk(&pk_prefix, &prefix_serializer);

        let key_range = range_of_prefix(&serialized_pk_prefix);

        trace!(
            "iter_with_pk_prefix: key_range {:?}",
            (key_range.start_bound(), key_range.end_bound())
        );

        CellBasedTableRowIter::new_with_bounds(
            &self.keyspace,
            self.mapping.clone(),
            key_range,
            epoch,
        )
        .await
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

fn generate_column_id(column_descs: &[ColumnDesc]) -> Vec<ColumnId> {
    column_descs.iter().map(|d| d.column_id).collect()
}

// (st1page): Maybe we will have a "ChunkIter" trait which returns a chunk each time, so the name
// "RowTableIter" is reserved now
pub struct CellBasedTableRowIter<S: StateStore> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,

    cell_based_row_deserializer: CellBasedRowDeserializer<Arc<ColumnDescMapping>>,
}

impl<S: StateStore> CellBasedTableRowIter<S> {
    pub(super) async fn new(
        keyspace: &Keyspace<S>,
        table_descs: Arc<ColumnDescMapping>,
        epoch: u64,
    ) -> StorageResult<Self> {
        Self::new_with_bounds::<_, &[u8]>(keyspace, table_descs, .., epoch).await
    }

    async fn new_with_bounds<R, B>(
        keyspace: &Keyspace<S>,
        table_descs: Arc<ColumnDescMapping>,
        serialized_pk_bounds: R,
        epoch: u64,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        keyspace.state_store().wait_epoch(epoch).await?;

        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_descs);

        let iter = keyspace
            .iter_with_range(serialized_pk_bounds, epoch)
            .await?;
        let iter = Self {
            iter,
            cell_based_row_deserializer,
        };
        Ok(iter)
    }

    async fn next_pk_and_row(&mut self) -> StorageResult<Option<(Vec<u8>, Row)>> {
        loop {
            match self.iter.next().await? {
                None => return Ok(self.cell_based_row_deserializer.take()),
                Some((key, value)) => {
                    tracing::trace!(
                        target: "events::storage::CellBasedTable::scan",
                        "CellBasedTable scanned key = {:?}, value = {:?}",
                        key,
                        value
                    );
                    let pk_and_row = self
                        .cell_based_row_deserializer
                        .deserialize(&key, &value)
                        .map_err(err)?;
                    match pk_and_row {
                        Some(_) => return Ok(pk_and_row),
                        None => {}
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl<S: StateStore> TableIter for CellBasedTableRowIter<S> {
    async fn next(&mut self) -> StorageResult<Option<Row>> {
        self.next_pk_and_row()
            .await
            .map(|r| r.map(|(_pk, row)| row))
    }
}

#[async_trait::async_trait]
impl<S: StateStore> CellTableChunkIter for CellBasedTableRowIter<S> {}

// Provides a layer on top of CellBasedTableRowIter
// for decoding pk into its constituent datums in a row
//
// Given the following row: | user_id | age | name |
// if pk was derived from `user_id, name`
// we can decode pk -> user_id, name,
// and retrieve the row: |_| age |_|,
// then fill in empty spots with datum decoded from pk: | user_id | age | name |
pub struct DedupPkCellBasedTableRowIter<S: StateStore> {
    inner: CellBasedTableRowIter<S>,
    pk_decoder: OrderedRowDeserializer,

    // Maps pk fields with:
    // 1. same value and memcomparable encoding,
    // 2. corresponding row positions. e.g. _row_id is unlikely to be part of selected row.
    pk_to_row_mapping: Vec<Option<usize>>,
}

impl<S: StateStore> DedupPkCellBasedTableRowIter<S> {
    pub async fn new(
        keyspace: Keyspace<S>,
        table_descs: Arc<ColumnDescMapping>,
        epoch: u64,
        pk_descs: &[OrderedColumnDesc],
    ) -> StorageResult<Self> {
        let inner = CellBasedTableRowIter::new(&keyspace, table_descs.clone(), epoch).await?;

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

#[async_trait::async_trait]
impl<S: StateStore> TableIter for DedupPkCellBasedTableRowIter<S> {
    async fn next(&mut self) -> StorageResult<Option<Row>> {
        if let Some((pk_vec, Row(mut row_inner))) = self.inner.next_pk_and_row().await? {
            let pk_decoded = self.pk_decoder.deserialize(&pk_vec).map_err(err)?;
            for (pk_idx, datum) in pk_decoded.into_vec().into_iter().enumerate() {
                if let Some(row_idx) = self.pk_to_row_mapping[pk_idx] {
                    row_inner[row_idx] = datum;
                }
            }
            Ok(Some(Row(row_inner)))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl<S: StateStore> CellTableChunkIter for DedupPkCellBasedTableRowIter<S> {}

#[async_trait::async_trait]
pub trait CellTableChunkIter
where
    Self: TableIter,
{
    async fn collect_data_chunk(
        &mut self,
        schema: &Schema,
        chunk_size: Option<usize>,
    ) -> StorageResult<Option<DataChunk>> {
        let mut builders = schema
            .create_array_builders(chunk_size.unwrap_or(0))
            .map_err(err)?;

        let mut row_count = 0;
        for _ in 0..chunk_size.unwrap_or(usize::MAX) {
            match self.next().await? {
                Some(row) => {
                    for (datum, builder) in row.0.into_iter().zip_eq(builders.iter_mut()) {
                        builder.append_datum(&datum).map_err(err)?;
                    }
                    row_count += 1;
                }
                None => break,
            }
        }

        let chunk = {
            let columns: Vec<Column> = builders
                .into_iter()
                .map(|builder| builder.finish().map(|a| Column::new(Arc::new(a))))
                .try_collect()
                .map_err(err)?;
            DataChunk::new(columns, row_count)
        };

        if chunk.cardinality() == 0 {
            Ok(None)
        } else {
            Ok(Some(chunk))
        }
    }
}

/// [`CellBasedTableStreamingIter`] is used for streaming executor, and will not wait for epoch.
pub struct CellBasedTableStreamingIter<S: StateStore> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,
    /// Cell-based row deserializer
    cell_based_row_deserializer: CellBasedRowDeserializer<Arc<ColumnDescMapping>>,
}

impl<S: StateStore> CellBasedTableStreamingIter<S> {
    pub async fn new(
        keyspace: &Keyspace<S>,
        table_descs: Arc<ColumnDescMapping>,
        epoch: u64,
    ) -> StorageResult<Self> {
        Self::new_with_bounds::<_, &[u8]>(keyspace, table_descs, .., epoch).await
    }

    pub async fn new_with_bounds<R, B>(
        keyspace: &Keyspace<S>,
        table_descs: Arc<ColumnDescMapping>,
        serialized_pk_bounds: R,
        epoch: u64,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        let cell_based_row_deserializer = CellBasedRowDeserializer::new(table_descs);

        let iter = keyspace
            .iter_with_range(serialized_pk_bounds, epoch)
            .await?;
        let iter = Self {
            iter,
            cell_based_row_deserializer,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    pub async fn into_stream(mut self) {
        while let Some((key, value)) = self.iter.next().await? {
            tracing::trace!(
                target: "events::storage::CellBasedTable::scan",
                "CellBasedTable scanned key = {:?}, value = {:?}",
                key,
                value
            );

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
