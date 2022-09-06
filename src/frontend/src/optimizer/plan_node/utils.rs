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

use std::collections::{HashMap, HashSet};
use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Field, Schema};
use risingwave_common::util::sort_util::OrderType;

use crate::catalog::column_catalog::ColumnCatalog;
use crate::catalog::{FragmentId, TableCatalog, TableId};
use crate::optimizer::property::{Direction, FieldOrder};
use crate::utils::WithOptions;

#[derive(Default)]
pub struct TableCatalogBuilder {
    columns: Vec<ColumnCatalog>,
    column_names: HashMap<String, i32>,
    order_key: Vec<FieldOrder>,
    // FIXME(stonepage): stream_key should be meaningless in internal state table, check if we
    // can remove it later
    stream_key: Vec<usize>,
    properties: HashMap<String, String>,
}

/// For DRY, mainly used for construct internal table catalog in stateful streaming executors.
/// Be careful of the order of add column.
impl TableCatalogBuilder {
    // TODO: Add more fields if internal table is more configurable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a column from Field info, return the column index of the table
    pub fn add_column(&mut self, field: &Field) -> usize {
        let column_idx = self.columns.len();
        let column_id = column_idx as i32;
        // Add column desc.
        let mut column_desc = ColumnDesc::from_field_with_column_id(field, column_id);

        // Avoid column name duplicate.
        self.avoid_duplicate_col_name(&mut column_desc);

        self.columns.push(ColumnCatalog {
            column_desc: column_desc.clone(),
            // All columns in internal table are invisible to batch query.
            is_hidden: false,
        });
        column_idx
    }

    /// Check whether need to add a ordered column. Different from value, order desc equal pk in
    /// semantics and they are encoded as storage key.
    pub fn add_order_column(&mut self, index: usize, order_type: OrderType) {
        self.stream_key.push(index);
        self.order_key.push(FieldOrder {
            index,
            direct: match order_type {
                OrderType::Ascending => Direction::Asc,
                OrderType::Descending => Direction::Desc,
            },
        });
    }

    /// Add `properties` for `TableCatalog`
    pub fn add_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }

    pub fn set_properties(&mut self, properties: WithOptions) {
        self.properties = properties.inner().clone();
    }

    /// Check the column name whether exist before. if true, record occurrence and change the name
    /// to avoid duplicate.
    fn avoid_duplicate_col_name(&mut self, column_desc: &mut ColumnDesc) {
        let column_name = column_desc.name.clone();
        if let Some(occurrence) = self.column_names.get_mut(&column_name) {
            column_desc.name = format!("{}_{}", column_name, occurrence);
            *occurrence += 1;
        } else {
            self.column_names.insert(column_name, 0);
        }
    }

    /// Consume builder and create `TableCatalog` (for proto).
    pub fn build(
        self,
        distribution_key: Vec<usize>,
        append_only: bool,
        vnode_col_idx: Option<usize>,
    ) -> TableCatalog {
        TableCatalog {
            id: TableId::placeholder(),
            associated_source_id: None,
            name: String::new(),
            columns: self.columns,
            order_key: self.order_key,
            stream_key: self.stream_key,
            is_index_on: None,
            distribution_key,
            appendonly: append_only,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            properties: self.properties,
            // TODO(zehua): replace it with FragmentId::placeholder()
            fragment_id: FragmentId::MAX - 1,
            vnode_col_idx,
        }
    }

    /// Consume builder and create `TableCatalog` (for proto).
    pub fn build_with_column_mapping(
        self,
        distribution_key: Vec<usize>,
        append_only: bool,
        column_mapping: &[usize],
        vnode_col_idx: Option<usize>,
    ) -> TableCatalog {
        // Transform indices to set for checking.
        let input_dist_key_indices_set: HashSet<usize> =
            HashSet::from_iter(distribution_key.iter().cloned());
        let column_mapping_indices_set: HashSet<usize> =
            HashSet::from_iter(column_mapping.iter().cloned());

        // Only if all `distribution_key` is in `column_mapping`, we return transformed dist key
        // indices, otherwise empty.
        if !column_mapping_indices_set.is_superset(&input_dist_key_indices_set) {
            return self.build(vec![], append_only, None);
        }

        // Transform `distribution_key` (based on input schema) to distribution indices on internal
        // table columns via `column_mapping` (input col idx -> state table col idx).
        let dist_indices_on_table_columns = distribution_key
            .iter()
            .map(|x| {
                column_mapping
                    .iter()
                    .position(|col_idx| *col_idx == *x)
                    .expect("Have checked that all input indices must be found")
            })
            .collect();
        let vnode_col_idx_in_table_columns =
            vnode_col_idx.and_then(|x| column_mapping.iter().position(|col_idx| *col_idx == x));
        self.build(
            dist_indices_on_table_columns,
            append_only,
            vnode_col_idx_in_table_columns,
        )
    }
}

#[derive(Clone, Copy)]
pub struct IndicesDisplay<'a> {
    pub indices: &'a [usize],
    pub input_schema: &'a Schema,
}

impl fmt::Display for IndicesDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}]",
            self.indices
                .iter()
                .map(|i| self.input_schema.fields.get(*i).unwrap().name.clone())
                .collect_vec()
                .join(", ")
        )
    }
}

impl fmt::Debug for IndicesDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}]",
            self.indices
                .iter()
                .map(|i| self.input_schema.fields.get(*i).unwrap().name.clone())
                .collect_vec()
                .join(", ")
        )
    }
}
