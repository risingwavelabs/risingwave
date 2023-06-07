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

use std::collections::HashMap;
use std::{fmt, vec};

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::Pretty;
use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ConflictBehavior, Field, Schema};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use crate::catalog::table_catalog::TableType;
use crate::catalog::{FragmentId, TableCatalog, TableId};
use crate::utils::WithOptions;

#[derive(Default)]
pub struct TableCatalogBuilder {
    /// All columns in this table
    columns: Vec<ColumnCatalog>,
    pk: Vec<ColumnOrder>,
    properties: WithOptions,
    value_indices: Option<Vec<usize>>,
    vnode_col_idx: Option<usize>,
    column_names: HashMap<String, i32>,
    read_prefix_len_hint: usize,
    watermark_columns: Option<FixedBitSet>,
    dist_key_in_pk: Option<Vec<usize>>,
}

/// For DRY, mainly used for construct internal table catalog in stateful streaming executors.
/// Be careful of the order of add column.
impl TableCatalogBuilder {
    // TODO: Add more fields if internal table is more configurable.
    pub fn new(properties: WithOptions) -> Self {
        Self {
            properties,
            ..Default::default()
        }
    }

    /// Add a column from Field info, return the column index of the table
    pub fn add_column(&mut self, field: &Field) -> usize {
        let column_idx = self.columns.len();
        let column_id = column_idx as i32;
        // Add column desc.
        let mut column_desc = ColumnDesc::from_field_with_column_id(field, column_id);

        // Replace dot of the internal table column name with underline.
        column_desc.name = column_desc.name.replace('.', "_");
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
    pub fn add_order_column(&mut self, column_index: usize, order_type: OrderType) {
        self.pk.push(ColumnOrder::new(column_index, order_type));
    }

    /// get the current exist field number of the primary key.
    pub fn get_current_pk_len(&self) -> usize {
        self.pk.len()
    }

    pub fn set_vnode_col_idx(&mut self, vnode_col_idx: usize) {
        self.vnode_col_idx = Some(vnode_col_idx);
    }

    pub fn set_value_indices(&mut self, value_indices: Vec<usize>) {
        self.value_indices = Some(value_indices);
    }

    #[allow(dead_code)]
    pub fn set_watermark_columns(&mut self, watermark_columns: FixedBitSet) {
        self.watermark_columns = Some(watermark_columns);
    }

    pub fn set_dist_key_in_pk(&mut self, dist_key_in_pk: Vec<usize>) {
        self.dist_key_in_pk = Some(dist_key_in_pk);
    }

    /// Check the column name whether exist before. if true, record occurrence and change the name
    /// to avoid duplicate.
    fn avoid_duplicate_col_name(&mut self, column_desc: &mut ColumnDesc) {
        if let Some(old_identity) = self.column_names.get(&column_desc.name) {
            let column_name = column_desc.name.clone();
            let mut identity = *old_identity;
            loop {
                column_desc.name = format!("{}_{}", column_name, identity);
                identity += 1;
                if !self.column_names.contains_key(&column_desc.name) {
                    break;
                }
            }
            *self.column_names.get_mut(&column_name).unwrap() = identity;
        }
        self.column_names.insert(column_desc.name.clone(), 0);
    }

    /// Consume builder and create `TableCatalog` (for proto). The `read_prefix_len_hint` is the
    /// anticipated read prefix pattern (number of fields) for the table, which can be utilized for
    /// implementing the table's bloom filter or other storage optimization techniques.
    pub fn build(self, distribution_key: Vec<usize>, read_prefix_len_hint: usize) -> TableCatalog {
        assert!(self.read_prefix_len_hint <= self.pk.len());
        let watermark_columns = match self.watermark_columns {
            Some(w) => w,
            None => FixedBitSet::with_capacity(self.columns.len()),
        };
        TableCatalog {
            id: TableId::placeholder(),
            associated_source_id: None,
            name: String::new(),
            columns: self.columns.clone(),
            pk: self.pk,
            stream_key: vec![],
            distribution_key,
            // NOTE: This should be altered if `TableCatalogBuilder` is used to build something
            // other than internal tables.
            table_type: TableType::Internal,
            append_only: false,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            properties: self.properties,
            // TODO(zehua): replace it with FragmentId::placeholder()
            fragment_id: FragmentId::MAX - 1,
            vnode_col_index: self.vnode_col_idx,
            row_id_index: None,
            value_indices: self
                .value_indices
                .unwrap_or_else(|| (0..self.columns.len()).collect_vec()),
            definition: "".into(),
            conflict_behavior: ConflictBehavior::NoCheck,
            read_prefix_len_hint,
            version: None, // the internal table is not versioned and can't be schema changed
            watermark_columns,
            dist_key_in_pk: self.dist_key_in_pk.unwrap_or(vec![]),
        }
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }
}

/// See also [`super::generic::DistillUnit`].
pub trait Distill {
    fn distill<'a>(&self) -> Pretty<'a>;
}

macro_rules! impl_distill_by_unit {
    ($ty:ty, $core:ident, $name:expr) => {
        use pretty_xmlish::Pretty;
        use $crate::optimizer::plan_node::generic::DistillUnit;
        use $crate::optimizer::plan_node::utils::Distill;
        impl Distill for $ty {
            fn distill<'a>(&self) -> Pretty<'a> {
                self.$core.distill_with_name($name)
            }
        }

        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.$core.fmt_with_name(f, $name)
            }
        }
    };
}
pub(crate) use impl_distill_by_unit;

#[derive(Clone, Copy)]
pub struct IndicesDisplay<'a> {
    pub indices: &'a [usize],
    pub input_schema: &'a Schema,
}

impl fmt::Display for IndicesDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl fmt::Debug for IndicesDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut f = f.debug_list();
        for i in self.indices {
            let name = &self.input_schema.fields.get(*i).unwrap().name;
            f.entry(&format_args!("{}", name));
        }
        f.finish()
    }
}

/// Call `debug_struct` on the given formatter to create a debug struct builder.
/// If a property list is provided, properties in it will be added to the struct name according to
/// the condition of that property.
macro_rules! formatter_debug_plan_node {
    ($formatter:ident, $name:literal $(, { $prop:literal, $cond:expr } )* $(,)?) => {
        {
            #[allow(unused_mut)]
            let mut properties: Vec<&str> = vec![];
            $( if $cond { properties.push($prop); } )*
            let mut name = $name.to_string();
            if !properties.is_empty() {
                name += " [";
                name += &properties.join(", ");
                name += "]";
            }
            $formatter.debug_struct(&name)
        }
    };
}
pub(crate) use formatter_debug_plan_node;
