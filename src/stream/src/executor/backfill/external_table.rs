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
use risingwave_common::catalog::{Schema, TableId, TableOption};
use risingwave_common::row::Row;
use risingwave_common::util::row_serde::*;
use risingwave_common::util::value_encoding::EitherSerde;
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::row_serde::ColumnMapping;
use risingwave_storage::StateStore;

use crate::executor::backfill::upstream_table::binlog::UpstreamBinlogOffsetRead;

pub type ExternalStorageTable = ExternalTableInner<EitherSerde>;

#[derive(Clone)]
pub struct ExternalTableInner<SD: ValueRowSerde> {
    /// Id for this table.
    table_id: TableId,

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

impl<SD: ValueRowSerde> ExternalTableInner<SD> {
    pub fn pk_serializer(&self) -> &OrderedRowSerde {
        &self.pk_serializer
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }

    pub fn output_indices(&self) -> &[usize] {
        &self.output_indices
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

    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

impl UpstreamBinlogOffsetRead for ExternalStorageTable {
    fn current_binlog_offset(&self) -> Option<String> {
        // todo(siyuan): issue different sql query to get the binlog offset
        match self {
            &_ => {}
        }

        todo!()
    }
}
