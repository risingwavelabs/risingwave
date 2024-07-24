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

use risingwave_common::catalog::ColumnCatalog;
use risingwave_pb::ddl_service::{
    SchemaChangeEnvelope as PbSchemaChangeEnvelope, TableSchemaChange as PbTableSchemaChange,
};

#[derive(Debug)]
pub struct SchemaChangeEnvelope {
    pub table_changes: Vec<TableSchemaChange>,
}

#[derive(Debug)]
pub struct TableSchemaChange {
    pub(crate) cdc_table_name: String,
    pub(crate) columns: Vec<ColumnCatalog>,
}

impl SchemaChangeEnvelope {
    pub fn to_protobuf(&self) -> PbSchemaChangeEnvelope {
        let table_changes = self
            .table_changes
            .iter()
            .map(|table_change| {
                let columns = table_change
                    .columns
                    .iter()
                    .map(|column| column.to_protobuf())
                    .collect();
                PbTableSchemaChange {
                    cdc_table_name: table_change.cdc_table_name.clone(),
                    columns,
                }
            })
            .collect();

        PbSchemaChangeEnvelope { table_changes }
    }
}
