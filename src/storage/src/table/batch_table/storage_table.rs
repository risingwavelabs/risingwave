// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::default::Default;
use std::future::Future;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::{Index, RangeBounds};
use std::pin::Pin;
use std::sync::Arc;
use std::{iter, mem};

use auto_enums::auto_enum;
use await_tree::InstrumentAwait;
use bytes::{BufMut, Bytes, BytesMut};
use foyer::memory::CacheContext;
use futures::future::{join_all, try_join_all};
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::{Either, Itertools};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId, TableOption};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::row_serde::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::{deserialize_datum, BasicSerde, EitherSerde};
use risingwave_hummock_sdk::key::{
    end_bound_of_prefix, next_key, prefixed_range_with_vnode,
    prefixed_range_with_vnode_and_column_id, TableKey, TableKeyRange,
};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_pb::plan_common::StorageTableDesc;
use tracing::trace;

use crate::error::{StorageError, StorageResult};
use crate::hummock::CachePolicy;
use crate::row_serde::row_serde_util::{
    serialize_pk, serialize_pk_with_vnode, serialize_pk_with_vnode_and_column_id,
};
use crate::row_serde::value_serde::{ValueRowSerde, ValueRowSerdeNew};
use crate::row_serde::{find_columns_by_ids, ColumnMapping};
use crate::store::{PrefetchOptions, ReadOptions, StateStoreIter};
use crate::table::merge_sort::merge_sort;
use crate::table::{KeyedRow, TableDistribution, TableIter};
use crate::StateStore;

/// [`StorageTableInner`] is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding format, and is used in batch mode.
#[derive(Clone)]
pub struct StorageTableInner<S: StateStore, SD: ValueRowSerde> {
    /// Id for this table.
    table_id: TableId,

    /// State store backend.
    store: S,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// `RowSeqScanExecutor`.
    schema: Schema,

    /// Used for serializing and deserializing the primary key.
    pk_serializer: OrderedRowSerde,

    output_indices: Vec<usize>,

    /// the key part of `output_indices`.
    key_output_indices: Option<Vec<usize>>,

    /// the value part of `output_indices`.
    value_output_indices: Vec<usize>,

    /// used for deserializing key part of output row from pk.
    output_row_in_key_indices: Vec<usize>,

    /// Mapping from column id to column index for deserializing the row.
    mapping: Arc<ColumnMapping>,

    /// Row deserializer to deserialize the whole value in storage to a row.
    row_serde: Arc<SD>,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    pk_indices: Vec<usize>,

    distribution: TableDistribution,

    /// Used for catalog `table_properties`
    table_option: TableOption,

    read_prefix_len_hint: usize,

    /// in default order
    column_ids: Vec<ColumnId>,
    is_columnar_store: bool,
}

/// `StorageTable` will use [`EitherSerde`] as default so that we can support both versioned and
/// non-versioned tables with the same type.
pub type StorageTable<S> = StorageTableInner<S, EitherSerde>;

impl<S: StateStore, SD: ValueRowSerde> std::fmt::Debug for StorageTableInner<S, SD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageTableInner").finish_non_exhaustive()
    }
}

// init
impl<S: StateStore> StorageTableInner<S, EitherSerde> {
    /// Create a  [`StorageTableInner`] given a complete set of `columns` and a partial
    /// set of `output_column_ids`.
    /// When reading from the storage table,
    /// the chunks or rows will only contain columns with the given ids (`output_column_ids`).
    /// They will in the same order as the given `output_column_ids`.
    ///
    /// NOTE(kwannoel): The `output_column_ids` here may be slightly different
    /// from those supplied to associated executors.
    /// These `output_column_ids` may have `pk` appended, since they will be needed to scan from
    /// storage. The associated executors may not have these `pk` fields.
    pub fn new_partial(
        store: S,
        output_column_ids: Vec<ColumnId>,
        vnodes: Option<Arc<Bitmap>>,
        table_desc: &StorageTableDesc,
    ) -> Self {
        let table_id = TableId {
            table_id: table_desc.table_id,
        };
        let column_descs = table_desc
            .columns
            .iter()
            .map(ColumnDesc::from)
            .collect_vec();
        let order_types: Vec<OrderType> = table_desc
            .pk
            .iter()
            .map(|order| OrderType::from_protobuf(order.get_order_type().unwrap()))
            .collect();

        let pk_indices = table_desc
            .pk
            .iter()
            .map(|k| k.column_index as usize)
            .collect_vec();

        let table_option = TableOption {
            retention_seconds: table_desc.retention_seconds,
        };
        let value_indices = table_desc
            .get_value_indices()
            .iter()
            .map(|&k| k as usize)
            .collect_vec();
        let prefix_hint_len = table_desc.get_read_prefix_len_hint() as usize;
        let versioned = table_desc.versioned;
        let distribution = TableDistribution::new_from_storage_table_desc(vnodes, table_desc);
        let is_columnar_store = table_desc.is_columnar_store.unwrap_or(false);
        Self::new_inner(
            store,
            table_id,
            column_descs,
            output_column_ids,
            order_types,
            pk_indices,
            distribution,
            table_option,
            value_indices,
            prefix_hint_len,
            versioned,
            is_columnar_store,
        )
    }

    pub fn for_test_with_partial_columns(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        output_column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        value_indices: Vec<usize>,
    ) -> Self {
        Self::new_inner(
            store,
            table_id,
            columns,
            output_column_ids,
            order_types,
            pk_indices,
            TableDistribution::singleton(),
            Default::default(),
            value_indices,
            0,
            false,
            // TODO(columnar_store): use is_columnar_store
            true,
        )
    }

    pub fn for_test(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        value_indices: Vec<usize>,
    ) -> Self {
        let output_column_ids = columns.iter().map(|c| c.column_id).collect();
        Self::for_test_with_partial_columns(
            store,
            table_id,
            columns,
            output_column_ids,
            order_types,
            pk_indices,
            value_indices,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        output_column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        distribution: TableDistribution,
        table_option: TableOption,
        value_indices: Vec<usize>,
        read_prefix_len_hint: usize,
        versioned: bool,
        is_columnar_store: bool,
    ) -> Self {
        assert_eq!(order_types.len(), pk_indices.len());

        let (output_columns, output_indices) =
            find_columns_by_ids(&table_columns, &output_column_ids);
        let mut value_output_indices = vec![];
        let mut key_output_indices = vec![];

        for idx in &output_indices {
            if value_indices.contains(idx) {
                value_output_indices.push(*idx);
            } else {
                key_output_indices.push(*idx);
            }
        }

        let output_row_in_value_indices = value_output_indices
            .iter()
            .map(|&di| value_indices.iter().position(|&pi| di == pi).unwrap())
            .collect_vec();
        let output_row_in_key_indices = key_output_indices
            .iter()
            .map(|&di| pk_indices.iter().position(|&pi| di == pi).unwrap())
            .collect_vec();
        let schema = Schema::new(output_columns.iter().map(Into::into).collect());

        let mapping = ColumnMapping::new(output_row_in_value_indices);

        let pk_data_types = pk_indices
            .iter()
            .map(|i| table_columns[*i].data_type.clone())
            .collect();
        let pk_serializer = OrderedRowSerde::new(pk_data_types, order_types);
        let column_ids = table_columns.iter().map(|c| c.column_id).collect_vec();
        let row_serde = {
            if versioned {
                ColumnAwareSerde::new(value_indices.into(), table_columns.into()).into()
            } else {
                BasicSerde::new(value_indices.into(), table_columns.into()).into()
            }
        };

        let key_output_indices = match key_output_indices.is_empty() {
            true => None,
            false => Some(key_output_indices),
        };
        Self {
            table_id,
            store,
            schema,
            pk_serializer,
            output_indices,
            key_output_indices,
            value_output_indices,
            output_row_in_key_indices,
            mapping: Arc::new(mapping),
            row_serde: Arc::new(row_serde),
            pk_indices,
            distribution,
            table_option,
            read_prefix_len_hint,
            column_ids,
            is_columnar_store,
        }
    }
}

impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    pub fn pk_serializer(&self) -> &OrderedRowSerde {
        &self.pk_serializer
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub fn output_indices(&self) -> &[usize] {
        &self.output_indices
    }

    /// Get the indices of the primary key columns in the output columns.
    ///
    /// Returns `None` if any of the primary key columns is not in the output columns.
    pub fn pk_in_output_indices(&self) -> Option<Vec<usize>> {
        self.pk_indices
            .iter()
            .map(|&i| self.output_indices.iter().position(|&j| i == j))
            .collect()
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn vnodes(&self) -> &Arc<Bitmap> {
        self.distribution.vnodes()
    }
}
/// Point get
impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    /// Get a single row by point get
    pub async fn get_row(
        &self,
        pk: impl Row,
        wait_epoch: HummockReadEpoch,
    ) -> StorageResult<Option<OwnedRow>> {
        if self.is_columnar_store {
            return self.columnar_store_get_row(pk, wait_epoch).await;
        }
        let epoch = wait_epoch.get_epoch();
        let read_backup = matches!(wait_epoch, HummockReadEpoch::Backup(_));
        self.store.try_wait_epoch(wait_epoch).await?;
        let serialized_pk = serialize_pk_with_vnode(
            &pk,
            &self.pk_serializer,
            self.distribution.compute_vnode_by_pk(&pk),
        );
        assert!(pk.len() <= self.pk_indices.len());

        let prefix_hint = if self.read_prefix_len_hint != 0 && self.read_prefix_len_hint == pk.len()
        {
            Some(serialized_pk.slice(VirtualNode::SIZE..))
        } else {
            None
        };

        let read_options = ReadOptions {
            prefix_hint,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            read_version_from_backup: read_backup,
            cache_policy: CachePolicy::Fill(CacheContext::Default),
            ..Default::default()
        };
        if let Some(value) = self.store.get(serialized_pk, epoch, read_options).await? {
            // Refer to [`StorageTableInnerIterInner::new`] for necessity of `validate_read_epoch`.
            self.store.validate_read_epoch(wait_epoch)?;
            let full_row = self.row_serde.deserialize(&value)?;
            let result_row_in_value = self
                .mapping
                .project(OwnedRow::new(full_row))
                .into_owned_row();
            match &self.key_output_indices {
                Some(key_output_indices) => {
                    let result_row_in_key =
                        pk.project(&self.output_row_in_key_indices).into_owned_row();
                    let mut result_row_vec = vec![];
                    for idx in &self.output_indices {
                        if self.value_output_indices.contains(idx) {
                            let item_position_in_value_indices = &self
                                .value_output_indices
                                .iter()
                                .position(|p| idx == p)
                                .unwrap();
                            result_row_vec.push(
                                result_row_in_value
                                    .index(*item_position_in_value_indices)
                                    .clone(),
                            );
                        } else {
                            let item_position_in_pk_indices =
                                key_output_indices.iter().position(|p| idx == p).unwrap();
                            result_row_vec
                                .push(result_row_in_key.index(item_position_in_pk_indices).clone());
                        }
                    }
                    let result_row = OwnedRow::new(result_row_vec);
                    Ok(Some(result_row))
                }
                None => Ok(Some(result_row_in_value)),
            }
        } else {
            Ok(None)
        }
    }

    // TODO(columnar_store): avoid code duplication when compared to get_row
    async fn columnar_store_get_row(
        &self,
        pk: impl Row,
        wait_epoch: HummockReadEpoch,
    ) -> StorageResult<Option<OwnedRow>> {
        tracing::debug!(pk=?pk, "columnar_store_get_row");
        let epoch = wait_epoch.get_epoch();
        let read_backup = matches!(wait_epoch, HummockReadEpoch::Backup(_));
        self.store.try_wait_epoch(wait_epoch).await?;
        assert!(pk.len() <= self.pk_indices.len());
        let vnode = self.distribution.compute_vnode_by_pk(&pk);
        // TODO(columnar_store): reuse serde result
        let sentinel_pk = serialize_pk_with_vnode_and_column_id(
            &pk,
            &self.pk_serializer,
            vnode,
            &ColumnId::new(0),
        );
        let prefix_hint = if self.read_prefix_len_hint != 0 && self.read_prefix_len_hint == pk.len()
        {
            Some(sentinel_pk.slice(VirtualNode::SIZE..))
        } else {
            None
        };
        let read_options = ReadOptions {
            prefix_hint,
            retention_seconds: self.table_option.retention_seconds,
            table_id: self.table_id,
            read_version_from_backup: read_backup,
            cache_policy: CachePolicy::Fill(CacheContext::Default),
            ..Default::default()
        };

        tracing::debug!(sentinel_pk=?sentinel_pk, "read sentinel_pk");
        if self
            .store
            .get(sentinel_pk, epoch, read_options.clone())
            .await?
            .is_none()
        {
            tracing::debug!("skip by sentinel_pk");
            return Ok(None);
        }
        // TODO(columnar_store): to_owned_row can be optimized out
        let owned_pk = pk.to_owned_row();
        let mut iter_fut: Vec<
            Pin<Box<dyn Future<Output = StorageResult<risingwave_common::types::Datum>> + Send>>,
        > = Vec::with_capacity(self.output_indices.len());
        for (pos, output_col_idx) in self.output_indices.iter().enumerate() {
            if let Some(i) = self
                .pk_indices
                .iter()
                .position(|pk_idx| *pk_idx == *output_col_idx)
            {
                let col_val = owned_pk.index(i).clone();
                let fut = async move { Ok::<_, StorageError>(col_val) };
                iter_fut.push(Box::pin(fut));
                continue;
            }
            let column_id = self.column_ids[*output_col_idx];
            let col_pk =
                serialize_pk_with_vnode_and_column_id(&pk, &self.pk_serializer, vnode, &column_id);
            let col_prefix_hint =
                if self.read_prefix_len_hint != 0 && self.read_prefix_len_hint == pk.len() {
                    Some(col_pk.slice(VirtualNode::SIZE..))
                } else {
                    None
                };
            let col_read_options = ReadOptions {
                prefix_hint: col_prefix_hint,
                ..read_options.clone()
            };
            let fut = async move {
                let col_val_bytes = self
                    .store
                    .get(col_pk, epoch, col_read_options)
                    .await?
                    .unwrap_or_else(|| panic!("missing col val, column_id={}", column_id));
                // TODO(columnar_store): use a deserializer
                let col_val = deserialize_datum(col_val_bytes, &self.schema[pos].data_type)?;
                Ok::<_, StorageError>(col_val)
            };
            iter_fut.push(Box::pin(fut));
        }
        let result_row_vec = try_join_all(iter_fut).await?;
        Ok(Some(OwnedRow::new(result_row_vec)))
    }

    /// Update the vnode bitmap of the storage table, returns the previous vnode bitmap.
    #[must_use = "the executor should decide whether to manipulate the cache based on the previous vnode bitmap"]
    pub fn update_vnode_bitmap(&mut self, new_vnodes: Arc<Bitmap>) -> Arc<Bitmap> {
        self.distribution.update_vnode_bitmap(new_vnodes)
    }
}

pub trait PkAndRowStream = Stream<Item = StorageResult<KeyedRow<Bytes>>> + Send;

pub type StorageTableInnerIter = Pin<Box<dyn PkAndRowStream>>;

#[async_trait::async_trait]
impl<S: PkAndRowStream + Unpin> TableIter for S {
    async fn next_row(&mut self) -> StorageResult<Option<OwnedRow>> {
        self.next()
            .await
            .transpose()
            .map(|r| r.map(|keyed_row| keyed_row.into_owned_row()))
    }
}

/// Iterators
impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    /// Get multiple [`StorageTableInnerIter`] based on the specified vnodes of this table with
    /// `vnode_hint`, and merge or concat them by given `ordered`.
    async fn iter_with_encoded_key_range(
        &self,
        prefix_hint: Option<Bytes>,
        encoded_key_range: (Bound<Bytes>, Bound<Bytes>),
        wait_epoch: HummockReadEpoch,
        vnode_hint: Option<VirtualNode>,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<StorageTableInnerIter> {
        let cache_policy = match (
            encoded_key_range.start_bound(),
            encoded_key_range.end_bound(),
        ) {
            // To prevent unbounded range scan queries from polluting the block cache, use the
            // low priority fill policy.
            (Unbounded, _) | (_, Unbounded) => CachePolicy::Fill(CacheContext::LruPriorityLow),
            _ => CachePolicy::Fill(CacheContext::Default),
        };

        let table_key_ranges = {
            // Vnodes that are set and should be accessed.
            let vnodes = match vnode_hint {
                // If `vnode_hint` is set, we can only access this single vnode.
                Some(vnode) => Either::Left(std::iter::once(vnode)),
                // Otherwise, we need to access all vnodes of this table.
                None => Either::Right(self.distribution.vnodes().iter_vnodes()),
            };
            vnodes.map(|vnode| prefixed_range_with_vnode(encoded_key_range.clone(), vnode))
        };

        // For each key range, construct an iterator.
        let iterators: Vec<_> = try_join_all(table_key_ranges.map(|table_key_range| {
            let prefix_hint = prefix_hint.clone();
            let read_backup = matches!(wait_epoch, HummockReadEpoch::Backup(_));
            async move {
                let read_options = ReadOptions {
                    prefix_hint,
                    retention_seconds: self.table_option.retention_seconds,
                    table_id: self.table_id,
                    read_version_from_backup: read_backup,
                    prefetch_options,
                    cache_policy,
                    ..Default::default()
                };
                let pk_serializer = match self.output_row_in_key_indices.is_empty() {
                    true => None,
                    false => Some(Arc::new(self.pk_serializer.clone())),
                };
                let iter = StorageTableInnerIterInner::<S, SD>::new(
                    &self.store,
                    self.mapping.clone(),
                    pk_serializer,
                    self.output_indices.clone(),
                    self.key_output_indices.clone(),
                    self.value_output_indices.clone(),
                    self.output_row_in_key_indices.clone(),
                    self.row_serde.clone(),
                    table_key_range,
                    read_options,
                    wait_epoch,
                )
                .await?
                .into_stream();

                Ok::<_, StorageError>(iter)
            }
        }))
        .await?;

        #[auto_enum(futures03::Stream)]
        let iter = match iterators.len() {
            0 => unreachable!(),
            1 => iterators.into_iter().next().unwrap(),
            // Concat all iterators if not to preserve order.
            _ if !ordered => {
                futures::stream::iter(iterators.into_iter().map(Box::pin).collect_vec())
                    .flatten_unordered(1024)
            }
            // Merge all iterators if to preserve order.
            _ => merge_sort(iterators.into_iter().map(Box::pin).collect()),
        };

        Ok(Box::pin(iter))
    }

    async fn columnar_store_iter_with_encoded_key_range(
        &self,
        prefix_hint: Option<Bytes>,
        encoded_key_range: (Bound<Bytes>, Bound<Bytes>),
        wait_epoch: HummockReadEpoch,
        vnode_hint: Option<VirtualNode>,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<StorageTableInnerIter> {
        let cache_policy = match (
            encoded_key_range.start_bound(),
            encoded_key_range.end_bound(),
        ) {
            // To prevent unbounded range scan queries from polluting the block cache, use the
            // low priority fill policy.
            (Unbounded, _) | (_, Unbounded) => CachePolicy::Fill(CacheContext::LruPriorityLow),
            _ => CachePolicy::Fill(CacheContext::Default),
        };

        let table_key_ranges = {
            // Vnodes that are set and should be accessed.
            let vnodes = match vnode_hint {
                // If `vnode_hint` is set, we can only access this single vnode.
                Some(vnode) => Either::Left(std::iter::once(vnode)),
                // Otherwise, we need to access all vnodes of this table.
                None => Either::Right(self.distribution.vnodes().iter_vnodes()),
            };
            vnodes.map(|vnode| (vnode, encoded_key_range.clone()))
        };

        // For each key range, construct an iterator.
        let iterators: Vec<_> = try_join_all(table_key_ranges.map(|(vnode, table_key_range)| {
            // this prefix_hint should be further prefixed with column id
            let prefix_hint = prefix_hint.clone();
            let read_backup = matches!(wait_epoch, HummockReadEpoch::Backup(_));
            async move {
                let read_options = ReadOptions {
                    prefix_hint,
                    retention_seconds: self.table_option.retention_seconds,
                    table_id: self.table_id,
                    read_version_from_backup: read_backup,
                    prefetch_options,
                    cache_policy,
                    ..Default::default()
                };
                let pk_serializer = self.pk_serializer.clone();
                let iter = ColumnarStoreStorageTableInnerIterInner::<S>::new(
                    &self.store,
                    vnode,
                    pk_serializer,
                    self.output_indices.clone(),
                    self.pk_indices.clone(),
                    self.column_ids.clone(),
                    self.schema
                        .fields
                        .iter()
                        .map(|f| f.data_type.clone())
                        .collect_vec(),
                    table_key_range,
                    read_options,
                    wait_epoch,
                )
                .await?
                .into_stream();

                Ok::<_, StorageError>(iter)
            }
        }))
        .await?;

        #[auto_enum(futures03::Stream)]
        let iter = match iterators.len() {
            0 => unreachable!(),
            1 => iterators.into_iter().next().unwrap(),
            // Concat all iterators if not to preserve order.
            _ if !ordered => {
                futures::stream::iter(iterators.into_iter().map(Box::pin).collect_vec())
                    .flatten_unordered(1024)
            }
            // Merge all iterators if to preserve order.
            _ => merge_sort(iterators.into_iter().map(Box::pin).collect()),
        };

        Ok(Box::pin(iter))
    }

    /// Iterates on the table with the given prefix of the pk in `pk_prefix` and the range bounds.
    async fn iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row,
        range_bounds: impl RangeBounds<OwnedRow>,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<StorageTableInnerIter> {
        // TODO: directly use `prefixed_range`.
        fn serialize_pk_bound(
            pk_serializer: &OrderedRowSerde,
            pk_prefix: impl Row,
            range_bound: Bound<&OwnedRow>,
            is_start_bound: bool,
        ) -> Bound<Bytes> {
            match range_bound {
                Included(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.len() + k.len());
                    let key = pk_prefix.chain(k);
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        Included(serialized_key)
                    } else {
                        // Should use excluded next key for end bound.
                        // Otherwise keys starting with the bound is not included.
                        end_bound_of_prefix(&serialized_key)
                    }
                }
                Excluded(k) => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.len() + k.len());
                    let key = pk_prefix.chain(k);
                    let serialized_key = serialize_pk(&key, &pk_prefix_serializer);
                    if is_start_bound {
                        // Storage doesn't support excluded begin key yet, so transform it to
                        // included.
                        // We always serialize a u8 for null of datum which is not equal to '\xff',
                        // so we can assert that the next_key would never be empty.
                        let next_serialized_key = next_key(&serialized_key);
                        assert!(!next_serialized_key.is_empty());
                        Included(Bytes::from(next_serialized_key))
                    } else {
                        Excluded(serialized_key)
                    }
                }
                Unbounded => {
                    let pk_prefix_serializer = pk_serializer.prefix(pk_prefix.len());
                    let serialized_pk_prefix = serialize_pk(&pk_prefix, &pk_prefix_serializer);
                    if pk_prefix.is_empty() {
                        Unbounded
                    } else if is_start_bound {
                        Included(serialized_pk_prefix)
                    } else {
                        end_bound_of_prefix(&serialized_pk_prefix)
                    }
                }
            }
        }

        let start_key = serialize_pk_bound(
            &self.pk_serializer,
            &pk_prefix,
            range_bounds.start_bound(),
            true,
        );
        let end_key = serialize_pk_bound(
            &self.pk_serializer,
            &pk_prefix,
            range_bounds.end_bound(),
            false,
        );

        assert!(pk_prefix.len() <= self.pk_indices.len());
        let pk_prefix_indices = (0..pk_prefix.len())
            .map(|index| self.pk_indices[index])
            .collect_vec();

        let prefix_hint = if self.read_prefix_len_hint != 0
            && self.read_prefix_len_hint <= pk_prefix.len()
        {
            let encoded_prefix = if let Bound::Included(start_key) = start_key.as_ref() {
                start_key
            } else {
                unreachable!()
            };
            let prefix_len = self
                .pk_serializer
                .deserialize_prefix_len(encoded_prefix, self.read_prefix_len_hint)?;
            Some(Bytes::from(encoded_prefix[..prefix_len].to_vec()))
        } else {
            trace!(
                    "iter_with_pk_bounds dist_key_indices table_id {} not match prefix pk_prefix {:?}  pk_prefix_indices {:?}",
                    self.table_id,
                    pk_prefix,
                    pk_prefix_indices
                );
            None
        };

        trace!(
            "iter_with_pk_bounds table_id {} prefix_hint {:?} start_key: {:?}, end_key: {:?} pk_prefix {:?}  pk_prefix_indices {:?}" ,
            self.table_id,
            prefix_hint,
            start_key,
            end_key,
            pk_prefix,
            pk_prefix_indices
        );

        if self.is_columnar_store {
            self.columnar_store_iter_with_encoded_key_range(
                prefix_hint,
                (start_key, end_key),
                epoch,
                self.distribution.try_compute_vnode_by_pk_prefix(pk_prefix),
                ordered,
                prefetch_options,
            )
            .await
        } else {
            self.iter_with_encoded_key_range(
                prefix_hint,
                (start_key, end_key),
                epoch,
                self.distribution.try_compute_vnode_by_pk_prefix(pk_prefix),
                ordered,
                prefetch_options,
            )
            .await
        }
    }

    /// Construct a [`StorageTableInnerIter`] for batch executors.
    /// Differs from the streaming one, this iterator will wait for the epoch before iteration
    pub async fn batch_iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row,
        range_bounds: impl RangeBounds<OwnedRow>,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<StorageTableInnerIter> {
        self.iter_with_pk_bounds(epoch, pk_prefix, range_bounds, ordered, prefetch_options)
            .await
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`.
    pub async fn batch_iter(
        &self,
        epoch: HummockReadEpoch,
        ordered: bool,
        prefetch_options: PrefetchOptions,
    ) -> StorageResult<StorageTableInnerIter> {
        self.batch_iter_with_pk_bounds(epoch, row::empty(), .., ordered, prefetch_options)
            .await
    }
}

/// [`StorageTableInnerIterInner`] iterates on the storage table.
struct StorageTableInnerIterInner<S: StateStore, SD: ValueRowSerde> {
    /// An iterator that returns raw bytes from storage.
    iter: S::Iter,

    mapping: Arc<ColumnMapping>,

    row_deserializer: Arc<SD>,

    /// Used for serializing and deserializing the primary key.
    pk_serializer: Option<Arc<OrderedRowSerde>>,

    output_indices: Vec<usize>,

    /// the key part of `output_indices`.
    key_output_indices: Option<Vec<usize>>,

    /// the value part of `output_indices`.
    value_output_indices: Vec<usize>,

    /// used for deserializing key part of output row from pk.
    output_row_in_key_indices: Vec<usize>,
}

impl<S: StateStore, SD: ValueRowSerde> StorageTableInnerIterInner<S, SD> {
    /// If `wait_epoch` is true, it will wait for the given epoch to be committed before iteration.
    #[allow(clippy::too_many_arguments)]
    async fn new(
        store: &S,
        mapping: Arc<ColumnMapping>,
        pk_serializer: Option<Arc<OrderedRowSerde>>,
        output_indices: Vec<usize>,
        key_output_indices: Option<Vec<usize>>,
        value_output_indices: Vec<usize>,
        output_row_in_key_indices: Vec<usize>,
        row_deserializer: Arc<SD>,
        table_key_range: TableKeyRange,
        read_options: ReadOptions,
        epoch: HummockReadEpoch,
    ) -> StorageResult<Self> {
        let raw_epoch = epoch.get_epoch();
        store.try_wait_epoch(epoch).await?;
        let iter = store.iter(table_key_range, raw_epoch, read_options).await?;
        // For `HummockStorage`, a cluster recovery will clear storage data and make subsequent
        // `HummockReadEpoch::Current` read incomplete.
        // `validate_read_epoch` is a safeguard against that incorrect read. It rejects the read
        // result if any recovery has happened after `try_wait_epoch`.
        store.validate_read_epoch(epoch)?;
        let iter = Self {
            iter,
            mapping,
            row_deserializer,
            pk_serializer,
            output_indices,
            key_output_indices,
            value_output_indices,
            output_row_in_key_indices,
        };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = KeyedRow<Bytes>, error = StorageError)]
    async fn into_stream(mut self) {
        while let Some((k, v)) = self
            .iter
            .try_next()
            .verbose_instrument_await("storage_table_iter_next")
            .await?
        {
            let (table_key, value) = (k.user_key.table_key, v);
            let full_row = self.row_deserializer.deserialize(value)?;
            let result_row_in_value = self
                .mapping
                .project(OwnedRow::new(full_row))
                .into_owned_row();
            match &self.key_output_indices {
                Some(key_output_indices) => {
                    let result_row_in_key = match self.pk_serializer.clone() {
                        Some(pk_serializer) => {
                            let pk = pk_serializer.deserialize(table_key.key_part().as_ref())?;

                            pk.project(&self.output_row_in_key_indices).into_owned_row()
                        }
                        None => OwnedRow::empty(),
                    };

                    let mut result_row_vec = vec![];
                    for idx in &self.output_indices {
                        if self.value_output_indices.contains(idx) {
                            let item_position_in_value_indices = &self
                                .value_output_indices
                                .iter()
                                .position(|p| idx == p)
                                .unwrap();
                            result_row_vec.push(
                                result_row_in_value
                                    .index(*item_position_in_value_indices)
                                    .clone(),
                            );
                        } else {
                            let item_position_in_pk_indices =
                                key_output_indices.iter().position(|p| idx == p).unwrap();
                            result_row_vec
                                .push(result_row_in_key.index(item_position_in_pk_indices).clone());
                        }
                    }
                    let row = OwnedRow::new(result_row_vec);

                    // TODO: may optimize the key clone
                    yield KeyedRow {
                        vnode_prefixed_key: table_key.copy_into(),
                        row,
                    }
                }
                None => {
                    yield KeyedRow {
                        vnode_prefixed_key: table_key.copy_into(),
                        row: result_row_in_value,
                    }
                }
            }
        }
    }
}

struct ColumnarStoreStorageTableInnerIterInner<S: StateStore> {
    iters: VecDeque<S::Iter>,
    mapping: ColumnMapping,
    output_indices: Vec<usize>,
    output_pk_position_in_pk_indices: Vec<usize>,
    non_pk_position_in_output_indices: Vec<usize>,
    data_types: Vec<DataType>,
    pk_serializer: OrderedRowSerde,
}

impl<S: StateStore> ColumnarStoreStorageTableInnerIterInner<S> {
    async fn new(
        store: &S,
        vnode: VirtualNode,
        pk_serializer: OrderedRowSerde,
        output_indices: Vec<usize>,
        pk_indices: Vec<usize>,
        column_ids: Vec<ColumnId>,
        data_types: Vec<DataType>,
        encoded_key_range: (Bound<Bytes>, Bound<Bytes>),
        read_options: ReadOptions,
        epoch: HummockReadEpoch,
    ) -> StorageResult<Self> {
        assert_eq!(output_indices.len(), data_types.len());
        let mut output_pk_position_in_pk_indices = vec![];
        let mut non_pk_position_in_output_indices = vec![];
        for (pos, col_idx) in output_indices.iter().enumerate() {
            if let Some(pos_in_pk_indices) = pk_indices
                .iter()
                .position(|pk_col_idx| pk_col_idx == col_idx)
            {
                output_pk_position_in_pk_indices.push(pos_in_pk_indices);
            } else {
                non_pk_position_in_output_indices.push(pos);
            }
        }
        let mut iter_to_output_mapping = vec![0; output_indices.len()];
        let mut iter_output_pos = 0;
        for pk_pos in &output_pk_position_in_pk_indices {
            let output_pos = output_indices
                .iter()
                .position(|output_col_idx| *output_col_idx == pk_indices[*pk_pos])
                .unwrap();
            iter_to_output_mapping[output_pos] = iter_output_pos;
            iter_output_pos += 1;
        }
        for non_pk_pos in &non_pk_position_in_output_indices {
            iter_to_output_mapping[*non_pk_pos] = iter_output_pos;
            iter_output_pos += 1;
        }
        let raw_epoch = epoch.get_epoch();
        store.try_wait_epoch(epoch).await?;
        let iter_ranges = iter::once(ColumnId::new(0))
            .chain(
                non_pk_position_in_output_indices
                    .iter()
                    .map(|pos| column_ids[output_indices[*pos]]),
            )
            .map(|column_id| {
                (
                    column_id,
                    prefixed_range_with_vnode_and_column_id(
                        encoded_key_range.clone(),
                        vnode,
                        column_id,
                    ),
                )
            });
        // .skip(if non_pk_position_in_output_indices.is_empty() {
        //     0
        // } else {
        //     1
        // });
        let mut iter_fut = Vec::with_capacity(1 + non_pk_position_in_output_indices.len());
        for (column_id, table_key_range) in iter_ranges {
            let col_prefix_hint = read_options.prefix_hint.as_ref().map(|bytes| {
                let column_id_slice = column_id.get_id().to_be_bytes();
                let mut buffer = BytesMut::with_capacity(column_id_slice.len() + bytes.len());
                buffer.extend_from_slice(&column_id_slice);
                buffer.extend_from_slice(bytes);
                buffer.freeze()
            });
            let col_read_options = ReadOptions {
                prefix_hint: col_prefix_hint,
                ..read_options.clone()
            };
            let fut = store.iter(table_key_range, raw_epoch, col_read_options);
            iter_fut.push(fut);
        }
        let iters = try_join_all(iter_fut).await?;
        tracing::debug!(iter_num = iters.len(), "column store iter num");
        store.validate_read_epoch(epoch)?;
        let iter = Self {
            iters: iters.into(),
            mapping: ColumnMapping::new(iter_to_output_mapping),
            output_indices,
            non_pk_position_in_output_indices,
            output_pk_position_in_pk_indices,
            data_types,
            pk_serializer,
        };
        Ok(iter)
    }

    #[try_stream(ok = KeyedRow<Bytes>, error = StorageError)]
    async fn into_stream(mut self) {
        fn strip_vnode_and_column_id_from_table_key<T: AsRef<[u8]>>(
            table_key: &TableKey<T>,
        ) -> &[u8] {
            &table_key.0.as_ref()[(VirtualNode::SIZE + mem::size_of::<ColumnId>())..]
        }
        fn vnode_prefixed_key<T: AsRef<[u8]>>(table_key: &TableKey<T>) -> Bytes {
            let mut buffer = BytesMut::new();
            buffer.put_slice(&table_key.0.as_ref()[..VirtualNode::SIZE]);
            buffer.put_slice(
                &table_key.0.as_ref()[(VirtualNode::SIZE + mem::size_of::<ColumnId>())..],
            );
            buffer.freeze()
        }
        let buffer_size = std::env::var("RW_COLUMN_STORE_BUFFER_SIZE")
            .unwrap_or("1024".into())
            .parse()
            .unwrap();
        let mut pk_iter = self.iters.pop_front().unwrap();
        let (pk_tx, mut pk_rx) = tokio::sync::mpsc::channel(buffer_size);
        let _pk_task = tokio::spawn(async move {
            let pk_tx_ = pk_tx.clone();
            let loop_fn = async move {
                while let Some((k, _)) = pk_iter.try_next().await? {
                    let table_key_with_vnode_and_column_id = k.user_key.table_key;
                    let vnode_prefixed_key =
                        TableKey(vnode_prefixed_key(&table_key_with_vnode_and_column_id));
                    let mut iter_output = vec![];
                    if !self.output_pk_position_in_pk_indices.is_empty() {
                        // TODO(columnar_store): try to optimize deserialize out.
                        let table_key = strip_vnode_and_column_id_from_table_key(
                            &table_key_with_vnode_and_column_id,
                        );
                        let pk = self.pk_serializer.deserialize(table_key)?;
                        for datum in pk
                            .project(&self.output_pk_position_in_pk_indices)
                            .into_owned_row()
                        {
                            iter_output.push(datum);
                        }
                    }
                    let _ = pk_tx_.send(Ok((vnode_prefixed_key, iter_output))).await;
                }
                Ok::<(), StorageError>(())
            };
            if let Err(err) = loop_fn.await {
                let _ = pk_tx.send(Err(err)).await;
            }
        });

        let mut col_rxs = vec![];
        for (pos, mut col_iter) in self
            .non_pk_position_in_output_indices
            .iter()
            .zip_eq_debug(self.iters)
        {
            let (col_tx, col_rx) = tokio::sync::mpsc::channel(buffer_size);
            col_rxs.push(col_rx);
            let data_type = self.data_types[*pos].clone();
            let _col_task = tokio::spawn(async move {
                let col_tx_ = col_tx.clone();
                let loop_fn = async move {
                    while let Some((_, v)) = col_iter.try_next().await? {
                        let col_val = deserialize_datum(v, &data_type)?;
                        let _ = col_tx_.send(Ok(col_val)).await;
                    }
                    Ok::<(), StorageError>(())
                };
                if let Err(err) = loop_fn.await {
                    let _ = col_tx.send(Err(err)).await;
                }
            });
        }

        loop {
            let (vnode_prefixed_key, mut iter_output) = match pk_rx.recv().await {
                Some(r) => r?,
                None => {
                    break;
                }
            };
            iter_output.reserve(self.non_pk_position_in_output_indices.len());
            let fut_iter = col_rxs.iter_mut().map(|rx| rx.recv());
            let kvs = join_all(fut_iter).await;
            for col_val in kvs
                .into_iter()
                .map(|kv| kv.unwrap_or_else(|| panic!("col expected")))
            {
                iter_output.push(col_val?);
            }
            let row = self
                .mapping
                .project(OwnedRow::new(iter_output))
                .into_owned_row();
            yield KeyedRow {
                vnode_prefixed_key,
                row,
            }
        }
    }
}
