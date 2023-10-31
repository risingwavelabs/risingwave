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

use std::collections::{BTreeMap, HashMap};

use risingwave_pb::plan_common::ExternalTableDesc;

use super::{ColumnDesc, ColumnId, TableId};
use crate::util::sort_util::ColumnOrder;

/// Necessary information for compute node to access data in the external database.
/// Compute node will use this information to connect to the external database and scan the table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct CdcTableDesc {
    /// Id of the upstream source in sharing cdc mode
    pub table_id: TableId,

    /// The full name of the table in external database, e.g. `database_name.table.name` in MySQL
    /// and `schema_name.table_name` in the Postgres.
    pub external_table_name: String,
    /// The key used to sort in storage.
    pub pk: Vec<ColumnOrder>,
    /// All columns in the table, noticed it is NOT sorted by columnId in the vec.
    pub columns: Vec<ColumnDesc>,

    /// Column indices for primary keys.
    pub stream_key: Vec<usize>,

    pub value_indices: Vec<usize>,

    /// properties will be passed into the ChainNode
    pub connect_properties: BTreeMap<String, String>,
}

impl CdcTableDesc {
    pub fn order_column_indices(&self) -> Vec<usize> {
        self.pk.iter().map(|col| (col.column_index)).collect()
    }

    pub fn order_column_ids(&self) -> Vec<ColumnId> {
        self.pk
            .iter()
            .map(|col| self.columns[col.column_index].column_id)
            .collect()
    }

    pub fn to_protobuf(&self) -> ExternalTableDesc {
        ExternalTableDesc {
            table_id: self.table_id.into(),
            columns: self.columns.iter().map(Into::into).collect(),
            pk: self.pk.iter().map(|v| v.to_protobuf()).collect(),
            table_name: self.external_table_name.clone(),
            stream_key: self.stream_key.iter().map(|k| *k as _).collect(),
            connect_properties: self.connect_properties.clone(),
        }
    }

    /// Helper function to create a mapping from `column id` to `column index`
    pub fn get_id_to_op_idx_mapping(&self) -> HashMap<ColumnId, usize> {
        let mut id_to_idx = HashMap::new();
        self.columns.iter().enumerate().for_each(|(idx, c)| {
            id_to_idx.insert(c.column_id, idx);
        });
        id_to_idx
    }
}
