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

use std::sync::Arc;

use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::util::row_serde::*;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::source::external::{ExternalTableReaderImpl, SchemaTableName};
use risingwave_storage::table::Distribution;

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

    /// Used for serializing and deserializing the primary key.
    pk_serializer: OrderedRowSerde,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table.
    pk_indices: Vec<usize>,

    output_indices: Vec<usize>,

    /// Indices of distribution key for computing vnode.
    /// Note that the index is based on the primary key columns by `pk_indices`.
    dist_key_in_pk_indices: Vec<usize>,

    /// Virtual nodes that the table is partitioned into.
    ///
    /// Only the rows whose vnode of the primary key is in this set will be visible to the
    /// executor. For READ_WRITE instances, the table will also check whether the written rows
    /// confirm to this partition.
    vnodes: Arc<Bitmap>,
}

impl ExternalStorageTable {
    pub fn new(
        table_id: TableId,
        table_name: String,
        schema_name: String,
        table_reader: ExternalTableReaderImpl,
        schema: Schema,
        order_types: Vec<OrderType>,
        pk_indices: Vec<usize>,
        output_indices: Vec<usize>,
        Distribution {
            dist_key_in_pk_indices,
            vnodes,
        }: Distribution,
    ) -> Self {
        let pk_data_types = pk_indices
            .iter()
            .map(|i| schema.fields[*i].data_type.clone())
            .collect();
        let pk_serializer = OrderedRowSerde::new(pk_data_types, order_types);

        Self {
            table_id,
            table_name,
            schema_name,
            table_reader,
            schema,
            pk_serializer,
            pk_indices,
            output_indices,
            dist_key_in_pk_indices,
            vnodes,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn pk_serializer(&self) -> &OrderedRowSerde {
        &self.pk_serializer
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
}
