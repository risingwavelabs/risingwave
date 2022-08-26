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

use risingwave_pb::plan_common::{ColumnOrder, StorageTableDesc};

use super::{ColumnDesc, ColumnId, TableId};
use crate::util::sort_util::OrderPair;

/// Includes necessary information for compute node to access data of the table.
///
/// It's a subset of `TableCatalog` in frontend. Refer to `TableCatalog` for more details.
#[derive(Debug, Clone, Default)]
pub struct TableDesc {
    /// Id of the table, to find in storage.
    pub table_id: TableId,
    /// The key used to sort in storage.
    pub order_key: Vec<OrderPair>,
    /// All columns in the table, noticed it is NOT sorted by columnId in the vec.
    pub columns: Vec<ColumnDesc>,
    /// Distribution keys of this table, which corresponds to the corresponding column of the
    /// index. e.g., if `distribution_key = [1, 2]`, then `columns[1]` and `columns[2]` are used
    /// as distribution key.
    pub distribution_key: Vec<usize>,
    /// Column indices for primary keys.
    pub stream_key: Vec<usize>,

    /// Whether the table source is append-only
    pub appendonly: bool,

    pub retention_seconds: u32,
}

impl TableDesc {
    pub fn arrange_key_orders_prost(&self) -> Vec<ColumnOrder> {
        // Set materialize key as arrange key + pk
        self.order_key.iter().map(|x| x.to_protobuf()).collect()
    }

    pub fn order_column_indices(&self) -> Vec<usize> {
        self.order_key.iter().map(|col| (col.column_idx)).collect()
    }

    pub fn order_column_ids(&self) -> Vec<ColumnId> {
        self.order_key
            .iter()
            .map(|col| self.columns[col.column_idx].column_id)
            .collect()
    }

    pub fn to_protobuf(&self) -> StorageTableDesc {
        StorageTableDesc {
            table_id: self.table_id.into(),
            columns: self.columns.iter().map(Into::into).collect(),
            order_key: self.order_key.iter().map(|v| v.to_protobuf()).collect(),
            dist_key_indices: self.distribution_key.iter().map(|&k| k as u32).collect(),
            retention_seconds: self.retention_seconds,
        }
    }
}
