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

use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use auto_enums::auto_enum;
use futures::future::try_join_all;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId, TableOption};
use risingwave_common::error::RwError;
use risingwave_common::types::{Datum, VirtualNode};
use risingwave_common::util::ordered::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::key::{end_bound_of_prefix, next_key, prefixed_range};
use risingwave_hummock_sdk::HummockReadEpoch;
use tracing::trace;

use super::iter_utils;
use crate::error::{StorageError, StorageResult};
use crate::keyspace::StripPrefixIterator;
use crate::row_serde::row_serde_util::{
    batch_deserialize, parse_raw_key_to_vnode_and_key, serialize_pk, serialize_pk_with_vnode,
};
use crate::row_serde::ColumnDescMapping;
use crate::store::ReadOptions;
use crate::table::{compute_vnode, Distribution, TableIter};
use crate::{Keyspace, StateStore, StateStoreIter};

/// [`StorageTable`] is the interface accessing relational data in KV(`StateStore`) with
/// row-based encoding format, and is used in batch mode.
#[derive(Clone)]
pub struct StorageTable<S: StateStore> {
    /// The keyspace that the pk and value of the original table has.
    keyspace: Keyspace<S>,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// RowSeqScanExecutor.
    schema: Schema,

    /// Used for serializing the primary key.
    pk_serializer: OrderedRowSerializer,

    /// Mapping from column id to column index. Used for deserializing the row.
    mapping: Arc<ColumnDescMapping>,

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
}

impl<S: StateStore> std::fmt::Debug for StorageTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageTable").finish_non_exhaustive()
    }
}

fn err(rw: impl Into<RwError>) -> StorageError {
    StorageError::StorageTable(rw.into())
}

// init
impl<S: StateStore> StorageTable<S> {
    /// Create a  [`StorageTable`] given a complete set of `columns` and a partial
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
        )
    }

    pub fn new_for_test(
        store: S,
        table_id: TableId,
        columns: Vec<ColumnDesc>,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
    ) -> Self {
        let column_ids = columns.iter().map(|c| c.column_id).collect();
        let value_indices = (0..columns.len()).collect_vec();
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
        )
    }
}

impl<S: StateStore> StorageTable<S> {
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
    ) -> Self {
        assert_eq!(order_types.len(), pk_indices.len());
        let mapping = ColumnDescMapping::new_partial(&table_columns, &column_ids, &value_indices);
        let schema = Schema::new(mapping.output_columns.iter().map(Into::into).collect());
        let pk_serializer = OrderedRowSerializer::new(order_types);

        let dist_key_in_pk_indices = dist_key_indices
            .iter()
            .map(|&di| {
                pk_indices
                    .iter()
                    .position(|&pi| di == pi)
                    .unwrap_or_else(|| {
                        panic!(
                            "distribution key {:?} must be a subset of primary key {:?}",
                            dist_key_indices, pk_indices
                        )
                    })
            })
            .collect_vec();
        let keyspace = Keyspace::table_root(store, &table_id);
        Self {
            keyspace,
            schema,
            pk_serializer,
            mapping,
            pk_indices,
            dist_key_indices,
            dist_key_in_pk_indices,
            vnodes,
            table_option,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Point get
impl<S: StateStore> StorageTable<S> {
    /// Get vnode value with given primary key.
    fn compute_vnode_by_pk(&self, pk: &Row) -> VirtualNode {
        compute_vnode(pk, &self.dist_key_in_pk_indices, &self.vnodes)
    }

    /// Try getting vnode value with given primary key prefix, used for `vnode_hint` in iterators.
    /// Return `None` if the provided columns are not enough.
    fn try_compute_vnode_by_pk_prefix(&self, pk_prefix: &Row) -> Option<VirtualNode> {
        self.dist_key_in_pk_indices
            .iter()
            .all(|&d| d < pk_prefix.0.len())
            .then(|| compute_vnode(pk_prefix, &self.dist_key_in_pk_indices, &self.vnodes))
    }

    /// Get a single row by point get
    pub async fn get_row(&mut self, pk: &Row, epoch: u64) -> StorageResult<Option<Row>> {
        let serialized_pk =
            serialize_pk_with_vnode(pk, &self.pk_serializer, self.compute_vnode_by_pk(pk));
        let read_options = self.get_read_option(epoch);
        assert!(pk.size() <= self.pk_indices.len());
        let key_indices = (0..pk.size())
            .into_iter()
            .map(|index| self.pk_indices[index])
            .collect_vec();
        if let Some(value) = self
            .keyspace
            .get(
                &serialized_pk,
                self.dist_key_indices == key_indices,
                read_options,
            )
            .await?
        {
            let deserialize_res = batch_deserialize(self.mapping.clone(), &value).map_err(err)?;
            Ok(Some(deserialize_res))
        } else {
            Ok(None)
        }
    }

    fn get_read_option(&self, epoch: u64) -> ReadOptions {
        ReadOptions {
            epoch,
            table_id: Some(self.keyspace.table_id()),
            retention_seconds: self.table_option.retention_seconds,
        }
    }
}

pub trait PkAndRowStream = Stream<Item = StorageResult<(Vec<u8>, Row)>> + Send;

/// The row iterator of the storage table.
/// The wrapper of [`StorageTableIter`] if pk is not persisted.
pub type StorageTableIter<S: StateStore> = impl PkAndRowStream;

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
impl<S: StateStore> StorageTable<S> {
    /// Get multiple [`StorageTableIter`] based on the specified vnodes of this table with
    /// `vnode_hint`, and merge or concat them by given `ordered`.
    async fn iter_with_encoded_key_range<R, B>(
        &self,
        prefix_hint: Option<Vec<u8>>,
        encoded_key_range: R,
        wait_epoch: HummockReadEpoch,
        vnode_hint: Option<VirtualNode>,
        ordered: bool,
    ) -> StorageResult<StorageTableIter<S>>
    where
        R: RangeBounds<B> + Send + Clone,
        B: AsRef<[u8]> + Send,
    {
        // Vnodes that are set and should be accessed.
        #[auto_enum(Iterator)]
        let vnodes = match vnode_hint {
            // If `vnode_hint` is set, we can only access this single vnode.
            Some(vnode) => std::iter::once(vnode),
            // Otherwise, we need to access all vnodes of this table.
            None => self
                .vnodes
                .iter()
                .enumerate()
                .filter(|&(_, set)| set)
                .map(|(i, _)| i as VirtualNode),
        };

        // For each vnode, construct an iterator.
        // TODO: if there're some vnodes continuously in the range and we don't care about order, we
        // can use a single iterator.
        let iterators: Vec<_> = try_join_all(vnodes.map(|vnode| {
            let raw_key_range = prefixed_range(encoded_key_range.clone(), &vnode.to_be_bytes());
            let prefix_hint = prefix_hint
                .clone()
                .map(|prefix_hint| [&vnode.to_be_bytes(), prefix_hint.as_slice()].concat());
            let wait_epoch = wait_epoch.clone();
            async move {
                let read_options = self.get_read_option(wait_epoch.get_epoch());
                let iter = StorageTableIterInner::<S>::new(
                    &self.keyspace,
                    self.mapping.clone(),
                    prefix_hint,
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

    /// Iterates on the table with the given prefix of the pk in `pk_prefix` and the range bounds of
    /// the next primary key column in `next_col_bounds`.
    // TODO: support multiple datums or `Row` for `next_col_bounds`.
    async fn iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: &Row,
        next_col_bounds: impl RangeBounds<Datum>,
        ordered: bool,
    ) -> StorageResult<StorageTableIter<S>> {
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
                        end_bound_of_prefix(&serialized_key)
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
                        // FIXME: What if `serialized_key` is `\xff\xff..`? Should the frontend
                        // reject this?
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
                        end_bound_of_prefix(&serialized_pk_prefix)
                    }
                }
            }
        }

        let start_key = serialize_pk_bound(
            &self.pk_serializer,
            pk_prefix,
            next_col_bounds.start_bound(),
            true,
        );
        let end_key = serialize_pk_bound(
            &self.pk_serializer,
            pk_prefix,
            next_col_bounds.end_bound(),
            false,
        );

        assert!(pk_prefix.size() <= self.pk_indices.len());
        let pk_prefix_indices = (0..pk_prefix.size())
            .into_iter()
            .map(|index| self.pk_indices[index])
            .collect_vec();
        let prefix_hint = if self.dist_key_indices.is_empty()
            || self.dist_key_indices != pk_prefix_indices
        {
            trace!(
                "iter_with_pk_bounds dist_key_indices table_id {} not match prefix pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}",
                self.keyspace.table_id(),
                pk_prefix,
                self.dist_key_indices,
                pk_prefix_indices
            );
            None
        } else {
            let pk_prefix_serializer = self.pk_serializer.prefix(pk_prefix.size());
            let serialized_pk_prefix = serialize_pk(pk_prefix, &pk_prefix_serializer);
            Some(serialized_pk_prefix)
        };

        trace!(
            "iter_with_pk_bounds table_id {} prefix_hint {:?} start_key: {:?}, end_key: {:?} pk_prefix {:?} dist_key_indices {:?} pk_prefix_indices {:?}" ,
            self.keyspace.table_id(),
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

    /// Construct a [`StorageTableIter`] for batch executors.
    /// Differs from the streaming one, this iterator will wait for the epoch before iteration
    pub async fn batch_iter_with_pk_bounds(
        &self,
        epoch: HummockReadEpoch,
        pk_prefix: &Row,
        next_col_bounds: impl RangeBounds<Datum>,
    ) -> StorageResult<StorageTableIter<S>> {
        self.iter_with_pk_bounds(epoch, pk_prefix, next_col_bounds, true)
            .await
    }

    // The returned iterator will iterate data from a snapshot corresponding to the given `epoch`.
    pub async fn batch_iter(&self, epoch: HummockReadEpoch) -> StorageResult<StorageTableIter<S>> {
        self.batch_iter_with_pk_bounds(epoch, Row::empty(), ..)
            .await
    }
}

/// [`StorageTableIterInner`] iterates on the storage table.
struct StorageTableIterInner<S: StateStore> {
    /// An iterator that returns raw bytes from storage.
    iter: StripPrefixIterator<S::Iter>,

    mapping: Arc<ColumnDescMapping>,
}

impl<S: StateStore> StorageTableIterInner<S> {
    /// If `wait_epoch` is true, it will wait for the given epoch to be committed before iteration.
    async fn new<R, B>(
        keyspace: &Keyspace<S>,
        mapping: Arc<ColumnDescMapping>,
        prefix_hint: Option<Vec<u8>>,
        raw_key_range: R,
        read_options: ReadOptions,
        epoch: HummockReadEpoch,
    ) -> StorageResult<Self>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        if !matches!(epoch, HummockReadEpoch::NoWait(_)) {
            keyspace.state_store().try_wait_epoch(epoch).await?;
        }
        let iter = keyspace
            .iter_with_range(prefix_hint, raw_key_range, read_options)
            .await?;
        let iter = Self { iter, mapping };
        Ok(iter)
    }

    /// Yield a row with its primary key.
    #[try_stream(ok = (Vec<u8>, Row), error = StorageError)]
    async fn into_stream(mut self) {
        while let Some((raw_key, value)) = self
            .iter
            .next()
            .stack_trace("storage_table_iter_next")
            .await?
        {
            let (_, key) = parse_raw_key_to_vnode_and_key(&raw_key);
            let row = batch_deserialize(self.mapping.clone(), &value).map_err(err)?;

            yield (key.to_vec(), row)
        }
    }
}
