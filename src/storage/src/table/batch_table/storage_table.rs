// Copyright 2023 RisingWave Labs
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

use std::assert_matches::assert_matches;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::{Index, RangeBounds};
use std::sync::Arc;

use auto_enums::auto_enum;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use futures::future::try_join_all;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::{Either, Itertools};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{
    get_dist_key_in_pk_indices, ColumnDesc, ColumnId, Schema, TableId, TableOption,
};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::{BasicSerde, ValueRowSerde};
use risingwave_hummock_sdk::key::{end_bound_of_prefix, next_key, prefixed_range};
use risingwave_hummock_sdk::HummockReadEpoch;
use tracing::trace;

use super::iter_utils;
use crate::error::{StorageError, StorageResult};
use crate::row_serde::row_serde_util::{
    parse_raw_key_to_vnode_and_key, serialize_pk, serialize_pk_with_vnode,
};
use crate::row_serde::{find_columns_by_ids, ColumnMapping};
use crate::store::ReadOptions;
use crate::table::{compute_vnode, Distribution, TableIter, DEFAULT_VNODE};
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
    /// RowSeqScanExecutor.
    schema: Schema,

    /// Used for serializing and deserializing the primary key.
    pk_serializer: OrderedRowSerde,

    output_indices: Vec<usize>,

    /// the key part of output_indices.
    key_output_indices: Option<Vec<usize>>,

    /// the value part of output_indices.
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

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the all columns of the table, instead of the output ones.
    // FIXME: revisit constructions and usages.
    dist_key_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the primary key columns by `pk_indices`.
    dist_key_in_pk_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    ///
    /// Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. For READ_WRITE instances, the table will also check whether the written rows
    /// confirm to this partition.
    vnodes: Arc<Bitmap>,

    /// Used for catalog table_properties
    table_option: TableOption,

    read_prefix_len_hint: usize,
}

/// `StorageTable` will use `BasicSerde` as default
pub type StorageTable<S> = StorageTableInner<S, BasicSerde>;

impl<S: StateStore, SD: ValueRowSerde> std::fmt::Debug for StorageTableInner<S, SD> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageTableInner").finish_non_exhaustive()
    }
}

// init
impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    /// Create a  [`StorageTableInner`] given a complete set of `columns` and a partial
    /// set of `column_ids`. The output will only contains columns with the given ids in the same
    /// order.
    #[allow(clippy::too_many_arguments)]
    pub fn new_partial(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        distribution: Distribution,
        table_options: TableOption,
        value_indices: Vec<usize>,
        read_prefix_len_hint: usize,
    ) -> Self {
        Self::new_inner(
            store,
            table_id,
            table_columns,
            column_ids,
            order_types,
            pk_indices,
            distribution,
            table_options,
            value_indices,
            read_prefix_len_hint,
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
        let column_ids = columns.iter().map(|c| c.column_id).collect();
        Self::new_inner(
            store,
            table_id,
            columns,
            column_ids,
            order_types,
            pk_indices,
            Distribution::fallback(),
            Default::default(),
            value_indices,
            0,
        )
    }

    pub fn pk_serializer(&self) -> &OrderedRowSerde {
        &self.pk_serializer
    }
}

impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        store: S,
        table_id: TableId,
        table_columns: Vec<ColumnDesc>,
        column_ids: Vec<ColumnId>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        Distribution {
            dist_key_indices,
            vnodes,
        }: Distribution,
        table_option: TableOption,
        value_indices: Vec<usize>,
        read_prefix_len_hint: usize,
    ) -> Self {
        assert_eq!(order_types.len(), pk_indices.len());

        let (output_columns, output_indices) = find_columns_by_ids(&table_columns, &column_ids);
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
        let all_data_types = table_columns
            .iter()
            .map(|d| d.data_type.clone())
            .collect_vec();
        let data_types = value_indices
            .iter()
            .map(|idx| all_data_types[*idx].clone())
            .collect_vec();
        let column_ids = value_indices
            .iter()
            .map(|idx| table_columns[*idx].column_id)
            .collect_vec();
        let pk_serializer = OrderedRowSerde::new(pk_data_types, order_types);
        let row_serde = SD::new(&column_ids, Arc::from(data_types.into_boxed_slice()));

        let dist_key_in_pk_indices = get_dist_key_in_pk_indices(&dist_key_indices, &pk_indices);
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
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option,
            read_prefix_len_hint,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }
}

/// Point get
impl<S: StateStore, SD: ValueRowSerde> StorageTableInner<S, SD> {
    /// Get vnode value with given primary key.
    fn compute_vnode_by_pk(&self, pk: impl Row) -> VirtualNode {
        compute_vnode(pk, &self.dist_key_in_pk_indices, &self.vnodes)
    }

    /// Try getting vnode value with given primary key prefix, used for `vnode_hint` in iterators.
    /// Return `None` if the provided columns are not enough.
    fn try_compute_vnode_by_pk_prefix(&self, pk_prefix: impl Row) -> Option<VirtualNode> {
        self.dist_key_in_pk_indices
            .iter()
            .all(|&d| d < pk_prefix.len())
            .then(|| compute_vnode(pk_prefix, &self.dist_key_in_pk_indices, &self.vnodes))
    }

    /// Get a single row by point get
    pub async fn get_row(
        &self,
        pk: impl Row,
        wait_epoch: HummockReadEpoch,
    ) -> StorageResult<Option<OwnedRow>> {
        let epoch = wait_epoch.get_epoch();
        let read_backup = matches!(wait_epoch, HummockReadEpoch::Backup(_));
        self.store.try_wait_epoch(wait_epoch).await?;
        let serialized_pk =
            serialize_pk_with_vnode(&pk, &self.pk_serializer, self.compute_vnode_by_pk(&pk));
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
            ignore_range_tombstone: false,
            table_id: self.table_id,
            read_version_from_backup: read_backup,
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
}

pub trait PkAndRowStream = Stream<Item = StorageResult<(Vec<u8>, OwnedRow)>> + Send;

/// The row iterator of the storage table.
/// The wrapper of [`StorageTableInnerIter`] if pk is not persisted.
pub type StorageTableInnerIter<S: StateStore, SD: ValueRowSerde> = impl PkAndRowStream;

#[async_trait::async_trait]
impl<S: PkAndRowStream + Unpin> TableIter for S {
    async fn next_row(&mut self) -> StorageResult<Option<OwnedRow>> {
        self.next()
            .await
            .transpose()
            .map(|r| r.map(|(_pk, row)| row))
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
    ) -> StorageResult<StorageTableInnerIter<S, SD>> {
        let raw_key_ranges = if !ordered
            && matches!(encoded_key_range.start_bound(), Unbounded)
            && matches!(encoded_key_range.end_bound(), Unbounded)
        {
            // If the range is unbounded and order is not required, we can create a single iterator
            // for each continuous vnode range.

            // In this case, the `vnode_hint` must be default for singletons and `None` for
            // distributed tables.
            assert_eq!(vnode_hint.unwrap_or(DEFAULT_VNODE), DEFAULT_VNODE);

            Either::Left(self.vnodes.vnode_ranges().map(|r| {
                let start = Included(Bytes::copy_from_slice(&r.start().to_be_bytes()[..]));
                let end = end_bound_of_prefix(&r.end().to_be_bytes());
                assert_matches!(end, Excluded(_) | Unbounded);
                (start, end)
            }))
        } else {
            // Vnodes that are set and should be accessed.
            let vnodes = match vnode_hint {
                // If `vnode_hint` is set, we can only access this single vnode.
                Some(vnode) => Either::Left(std::iter::once(vnode)),
                // Otherwise, we need to access all vnodes of this table.
                None => Either::Right(self.vnodes.iter_vnodes()),
            };
            Either::Right(
                vnodes.map(|vnode| prefixed_range(encoded_key_range.clone(), &vnode.to_be_bytes())),
            )
        };

        // For each key range, construct an iterator.
        let iterators: Vec<_> = try_join_all(raw_key_ranges.map(|raw_key_range| {
            let prefix_hint = prefix_hint.clone();
            let wait_epoch = wait_epoch;
            let read_backup = matches!(wait_epoch, HummockReadEpoch::Backup(_));
            async move {
                let read_options = ReadOptions {
                    prefix_hint,
                    ignore_range_tombstone: false,
                    retention_seconds: self.table_option.retention_seconds,
                    table_id: self.table_id,
                    read_version_from_backup: read_backup,
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
                    raw_key_range,
                    read_options,
                    wait_epoch,
                )
                .await?
                .into_stream();

                Ok::<_, StorageError>(iter)
            }
        }))
        .await?;

        #[auto_enum(futures::Stream)]
        let iter = match iterators.len() {
            0 => unreachable!(),
            1 => iterators.into_iter().next().unwrap(),
            // Concat all iterators if not to preserve order.
            _ if !ordered => futures::stream::iter(iterators).flatten(),
            // Merge all iterators if to preserve order.
            _ => iter_utils::merge_sort(iterators.into_iter().map(Box::pin).collect()),
        };

        Ok(iter)
    }

    /// Iterates on the table with the given prefix of the pk in `pk_prefix` and the range bounds.
    async fn iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row,
        range_bounds: impl RangeBounds<OwnedRow>,
        ordered: bool,
    ) -> StorageResult<StorageTableInnerIter<S, SD>> {
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
                    "iter_with_pk_bounds dist_key_indices table_id {} not match prefix pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}",
                    self.table_id,
                    pk_prefix,
                    self.dist_key_indices,
                    pk_prefix_indices
                );
            None
        };

        trace!(
            "iter_with_pk_bounds table_id {} prefix_hint {:?} start_key: {:?}, end_key: {:?} pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}" ,
            self.table_id,
            prefix_hint,
            start_key,
            end_key,
            pk_prefix,
            self.dist_key_indices,
            pk_prefix_indices
        );

        self.iter_with_encoded_key_range(
            prefix_hint,
            (start_key, end_key),
            epoch,
            self.try_compute_vnode_by_pk_prefix(pk_prefix),
            ordered,
        )
        .await
    }

    /// Construct a [`StorageTableInnerIter`] for batch executors.
    /// Differs from the streaming one, this iterator will wait for the epoch before iteration
    pub async fn batch_iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: impl Row,
        range_bounds: impl RangeBounds<OwnedRow>,
        ordered: bool,
    ) -> StorageResult<StorageTableInnerIter<S, SD>> {
        self.iter_with_pk_bounds(epoch, pk_prefix, range_bounds, ordered)
            .await
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`.
    pub async fn batch_iter(
        &self,
        epoch: HummockReadEpoch,
        ordered: bool,
    ) -> StorageResult<StorageTableInnerIter<S, SD>> {
        self.batch_iter_with_pk_bounds(epoch, row::empty(), .., ordered)
            .await
    }
}

/// [`StorageTableInnerIterInner`] iterates on the storage table.
struct StorageTableInnerIterInner<S: StateStore, SD: ValueRowSerde> {
    /// An iterator that returns raw bytes from storage.
    iter: S::IterStream,

    mapping: Arc<ColumnMapping>,

    row_deserializer: Arc<SD>,

    /// Used for serializing and deserializing the primary key.
    pk_serializer: Option<Arc<OrderedRowSerde>>,

    output_indices: Vec<usize>,

    /// the key part of output_indices.
    key_output_indices: Option<Vec<usize>>,

    /// the value part of output_indices.
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
        raw_key_range: (Bound<Bytes>, Bound<Bytes>),
        read_options: ReadOptions,
        epoch: HummockReadEpoch,
    ) -> StorageResult<Self> {
        let raw_epoch = epoch.get_epoch();
        store.try_wait_epoch(epoch).await?;
        let iter = store.iter(raw_key_range, raw_epoch, read_options).await?;
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
    #[try_stream(ok = (Vec<u8>, OwnedRow), error = StorageError)]
    async fn into_stream(self) {
        use futures::TryStreamExt;

        // No need for table id and epoch.
        let iter = self.iter.map_ok(|(k, v)| (k.user_key.table_key.0, v));
        futures::pin_mut!(iter);
        while let Some((raw_key, value)) = iter
            .try_next()
            .verbose_instrument_await("storage_table_iter_next")
            .await?
        {
            let (_, key) = parse_raw_key_to_vnode_and_key(&raw_key);

            let full_row = self.row_deserializer.deserialize(&value)?;
            let result_row_in_value = self
                .mapping
                .project(OwnedRow::new(full_row))
                .into_owned_row();
            match &self.key_output_indices {
                Some(key_output_indices) => {
                    let result_row_in_key = match self.pk_serializer.clone() {
                        Some(pk_serializer) => {
                            let pk = pk_serializer.deserialize(key)?;

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

                    yield (key.to_vec(), row)
                }
                None => yield (key.to_vec(), result_row_in_value),
            }
        }
    }
}
