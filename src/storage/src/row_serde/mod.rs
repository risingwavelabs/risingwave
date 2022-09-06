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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::catalog::{ColumnDesc, ColumnId};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, VirtualNode};
use risingwave_common::util::ordered::OrderedRowSerializer;

pub mod row_serde_util;

/// `ColumnDescMapping` is the record mapping from [`ColumnDesc`], [`ColumnId`], and is used in
/// row-based encoding deserialization.
#[derive(Clone)]
pub struct ColumnDescMapping {
    /// output_columns are some of the columns that to be partially scan.
    pub output_columns: Vec<ColumnDesc>,

    /// The full row data types, which is used in row-based deserialize.
    pub all_data_types: Vec<DataType>,

    /// The output column's column index in full row, which is used in row-based deserialize.
    pub output_index: Vec<usize>,
}

#[allow(clippy::len_without_is_empty)]
impl ColumnDescMapping {
    fn new_inner(
        output_columns: Vec<ColumnDesc>,
        all_data_types: Vec<DataType>,
        output_index: Vec<usize>,
    ) -> Arc<Self> {
        Self {
            output_columns,
            all_data_types,
            output_index,
        }
        .into()
    }

    /// Create a mapping with given `output_columns`.
    pub fn new(output_columns: Vec<ColumnDesc>) -> Arc<Self> {
        let all_data_types = output_columns.iter().map(|d| d.data_type.clone()).collect();
        let output_index: Vec<usize> = output_columns
            .iter()
            .map(|c| c.column_id.get_id() as usize)
            .collect();
        Self::new_inner(output_columns, all_data_types, output_index)
    }

    /// Create a mapping with given `table_columns` projected on the `column_ids`.
    pub fn new_partial(table_columns: &[ColumnDesc], output_column_ids: &[ColumnId]) -> Arc<Self> {
        let all_data_types = table_columns.iter().map(|d| d.data_type.clone()).collect();
        let mut table_columns = table_columns
            .iter()
            .enumerate()
            .map(|(index, c)| (c.column_id, (c.clone(), index)))
            .collect::<HashMap<_, _>>();
        let (output_columns, output_index): (
            Vec<risingwave_common::catalog::ColumnDesc>,
            Vec<usize>,
        ) = output_column_ids
            .iter()
            .map(|id| table_columns.remove(id).unwrap())
            .unzip();
        Self::new_inner(output_columns, all_data_types, output_index)
    }

    /// Get the length of output columns.
    pub fn len(&self) -> usize {
        self.output_columns.len()
    }
}
