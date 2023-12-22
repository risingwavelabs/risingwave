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

use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::external::{ExternalTableReaderImpl, SchemaTableName};

/// This struct represents an external table to be read during backfill
pub struct ExternalStorageTable {
    /// Id for this table.
    table_id: TableId,

    /// The normalized name of the table, e.g. `dbname.schema_name.table_name`.
    table_name: String,

    schema_name: String,

    table_reader: ExternalTableReaderImpl,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// RowSeqScanExecutor.
    /// todo: the schema of the external table defined in the CREATE TABLE DDL
    schema: Schema,

    pk_order_types: Vec<OrderType>,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table.
    pk_indices: Vec<usize>,

    output_indices: Vec<usize>,
}

impl ExternalStorageTable {
    pub fn new(
        table_id: TableId,
        SchemaTableName {
            table_name,
            schema_name,
        }: SchemaTableName,
        table_reader: ExternalTableReaderImpl,
        schema: Schema,
        pk_order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        output_indices: Vec<usize>,
    ) -> Self {
        Self {
            table_id,
            table_name,
            schema_name,
            table_reader,
            schema,
            pk_order_types,
            pk_indices,
            output_indices,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn pk_order_types(&self) -> &[OrderType] {
        &self.pk_order_types
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
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

    pub fn schema_table_name(&self) -> SchemaTableName {
        SchemaTableName {
            schema_name: self.schema_name.clone(),
            table_name: self.table_name.clone(),
        }
    }

    pub fn table_reader(&self) -> &ExternalTableReaderImpl {
        &self.table_reader
    }

    pub fn qualified_table_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.table_name)
    }
}
