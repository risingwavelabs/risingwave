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
use risingwave_common::cache::CachePriority;
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema, TableId, TableOption};
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::util::row_serde::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::{BasicSerde, EitherSerde};
use risingwave_hummock_sdk::key::{end_bound_of_prefix, next_key, prefixed_range};
use risingwave_hummock_sdk::HummockReadEpoch;
use tracing::trace;

use crate::error::{StorageError, StorageResult};
use crate::hummock::CachePolicy;
use crate::row_serde::row_serde_util::{
    parse_raw_key_to_vnode_and_key, serialize_pk, serialize_pk_with_vnode,
};
use crate::row_serde::value_serde::{ValueRowSerde, ValueRowSerdeNew};
use crate::row_serde::{find_columns_by_ids, ColumnMapping};
use crate::store::{PrefetchOptions, ReadOptions};
use crate::table::batch_table::{UpstreamCdcTable, UpstreamTable};
use crate::table::merge_sort::merge_sort;
use crate::table::{compute_vnode, Distribution, TableIter, DEFAULT_VNODE};
use crate::StateStore;

pub type ExternalUpstreamTable<S> = ExternalTableInner<S, EitherSerde>;

#[derive(Clone)]
pub struct ExternalTableInner<S: StateStore, SD: ValueRowSerde> {
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

impl<S: StateStore, SD: ValueRowSerde> UpstreamTable for ExternalTableInner<S, SD> {
    fn pk_serializer(&self) -> &OrderedRowSerde {
        &self.pk_serializer
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    fn output_indices(&self) -> &[usize] {
        &self.output_indices
    }

    /// Get the indices of the primary key columns in the output columns.
    ///
    /// Returns `None` if any of the primary key columns is not in the output columns.
    fn pk_in_output_indices(&self) -> Option<Vec<usize>> {
        self.pk_indices
            .iter()
            .map(|&i| self.output_indices.iter().position(|&j| i == j))
            .collect()
    }

    fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl<S: StateStore> UpstreamCdcTable for ExternalUpstreamTable<S> {
    async fn get_current_wal_offset(&self) -> Option<String> {
        None
    }
}
