// Copyright 2025 RisingWave Labs
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

use fixedbitset::FixedBitSet;
use risingwave_pb::catalog::Table;
use risingwave_pb::plan_common::StorageTableDesc;

use super::{ColumnDesc, TableId};
use crate::catalog::get_dist_key_in_pk_indices;
use crate::hash::{VnodeCount, VnodeCountCompat};
use crate::util::sort_util::ColumnOrder;

/// Includes necessary information for compute node to access data of the table.
///
/// It's a subset of `TableCatalog` in frontend. Refer to `TableCatalog` for more details.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct TableDesc {
    /// Id of the table, to find in storage.
    pub table_id: TableId,
    /// The key used to sort in storage.
    pub pk: Vec<ColumnOrder>,
    /// All columns in the table, noticed it is NOT sorted by columnId in the vec.
    pub columns: Vec<ColumnDesc>,
    /// Distribution keys of this table, which corresponds to the corresponding column of the
    /// index. e.g., if `distribution_key = [1, 2]`, then `columns[1]` and `columns[2]` are used
    /// as distribution key.
    pub distribution_key: Vec<usize>,
    /// Column indices for primary keys.
    pub stream_key: Vec<usize>,

    pub vnode_col_index: Option<usize>,

    /// Whether the table source is append-only
    pub append_only: bool,

    // TTL of the record in the table, to ensure the consistency with other tables in the streaming plan, it only applies to append-only tables.
    pub retention_seconds: Option<u32>,

    pub value_indices: Vec<usize>,

    /// The prefix len of pk, used in bloom filter.
    pub read_prefix_len_hint: usize,

    /// the column indices which could receive watermarks.
    pub watermark_columns: FixedBitSet,

    /// Total vnode count of the table.
    pub vnode_count: usize,

    /// Whether the table is versioned. If `true`, column-aware row encoding will be used
    /// to be compatible with schema changes.
    ///
    /// See `version` field in `TableCatalog` for more details.
    pub versioned: bool,
}

impl TableDesc {
    pub fn try_to_protobuf(&self) -> anyhow::Result<StorageTableDesc> {
        let dist_key_indices: Vec<u32> = self.distribution_key.iter().map(|&k| k as u32).collect();
        let pk_indices: Vec<u32> = self
            .pk
            .iter()
            .map(|v| v.to_protobuf().column_index)
            .collect();
        let vnode_col_idx_in_pk = self
            .vnode_col_index
            .and_then(|vnode_col_index| {
                pk_indices
                    .iter()
                    .position(|&pk_index| pk_index == vnode_col_index as u32)
            })
            .map(|i| i as u32);

        let dist_key_in_pk_indices = if vnode_col_idx_in_pk.is_none() {
            get_dist_key_in_pk_indices(&dist_key_indices, &pk_indices)?
        } else {
            Vec::new()
        };
        Ok(StorageTableDesc {
            table_id: self.table_id.into(),
            columns: self.columns.iter().map(Into::into).collect(),
            pk: self.pk.iter().map(|v| v.to_protobuf()).collect(),
            dist_key_in_pk_indices,
            retention_seconds: self.retention_seconds,
            value_indices: self.value_indices.iter().map(|&v| v as u32).collect(),
            read_prefix_len_hint: self.read_prefix_len_hint as u32,
            versioned: self.versioned,
            stream_key: self.stream_key.iter().map(|&x| x as u32).collect(),
            vnode_col_idx_in_pk,
            maybe_vnode_count: VnodeCount::set(self.vnode_count).to_protobuf(),
        })
    }

    pub fn from_pb_table(table: &Table) -> Self {
        let vnode_count = table.vnode_count();

        Self {
            table_id: TableId::new(table.id),
            pk: table.pk.iter().map(ColumnOrder::from_protobuf).collect(),
            columns: table
                .columns
                .iter()
                .map(|col| ColumnDesc::from(col.column_desc.as_ref().unwrap()))
                .collect(),
            distribution_key: table.distribution_key.iter().map(|i| *i as _).collect(),
            stream_key: table.stream_key.iter().map(|i| *i as _).collect(),
            vnode_col_index: table.vnode_col_index.map(|i| i as _),
            append_only: table.append_only,
            retention_seconds: table.retention_seconds,
            value_indices: table.value_indices.iter().map(|i| *i as _).collect(),
            read_prefix_len_hint: table.read_prefix_len_hint as _,
            watermark_columns: table.watermark_indices.iter().map(|i| *i as _).collect(),
            versioned: table.version.is_some(),
            vnode_count,
        }
    }
}
