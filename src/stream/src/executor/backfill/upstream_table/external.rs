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

use anyhow::anyhow;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::util::row_serde::*;
use risingwave_connector::source::external::{ExternalTableReaderImpl, SchemaTableName};
use risingwave_storage::row_serde::ColumnMapping;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

/// This struct represents an external table to be read during backfill
pub struct ExternalStorageTable {
    /// The normalized name of the table, e.g. `dbname.schema_name.table_name`.
    table_name: String,

    schema_name: String,

    table_reader: ExternalTableReaderImpl,

    /// The schema of the output columns, i.e., this table VIEWED BY some executor like
    /// RowSeqScanExecutor.
    /// todo: the schema of the external table defined in the CREATE TABLE DDL
    schema: Schema,

    /// Indices of primary key.
    /// Note that the index is based on the all columns of the table.
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
}

impl ExternalStorageTable {
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
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
