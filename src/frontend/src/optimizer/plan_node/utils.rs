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
use std::vec;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, Str, StrAssocArr, XmlNode};
use risingwave_common::catalog::{
    ColumnCatalog, ColumnDesc, ConflictBehavior, Field, FieldDisplay, Schema,
};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use crate::catalog::table_catalog::{CreateType, TableType};
use crate::catalog::{ColumnId, FragmentId, TableCatalog, TableId};
use crate::optimizer::property::Cardinality;
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
            column_desc,
            // All columns in internal table are invisible to batch query.
            is_hidden: false,
        });
        column_idx
    }

    /// Extend the columns with column ids reset. The input columns should NOT have duplicate names.
    ///
    /// Returns the indices of the extended columns.
    pub fn extend_columns(&mut self, columns: &[ColumnCatalog]) -> Vec<usize> {
        let base_idx = self.columns.len();
        columns.iter().enumerate().for_each(|(i, col)| {
            assert!(!self.column_names.contains_key(col.name()));
            self.column_names.insert(col.name().to_string(), 0);

            // Reset the column id for the columns.
            let mut new_col = col.clone();
            new_col.column_desc.column_id = ColumnId::new((base_idx + i) as _);
            self.columns.push(new_col);
        });
        Vec::from_iter(base_idx..(base_idx + columns.len()))
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
            dml_fragment_id: None,
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
            dist_key_in_pk: self.dist_key_in_pk.unwrap_or_default(),
            cardinality: Cardinality::unknown(), // TODO(card): cardinality of internal table
            created_at_epoch: None,
            initialized_at_epoch: None,
            cleaned_by_watermark: false,
            // NOTE(kwannoel): This may not match the create type of the materialized table.
            // It should be ignored for internal tables.
            create_type: CreateType::Foreground,
            description: None,
            incoming_sinks: vec![],
        }
    }

    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }
}

/// See also [`super::generic::DistillUnit`].
pub trait Distill {
    fn distill<'a>(&self) -> XmlNode<'a>;

    fn distill_to_string(&self) -> String {
        let mut config = pretty_config();
        let mut output = String::with_capacity(2048);
        config.unicode(&mut output, &Pretty::Record(self.distill()));
        output
    }
}

pub(super) fn childless_record<'a>(
    name: impl Into<Str<'a>>,
    fields: StrAssocArr<'a>,
) -> XmlNode<'a> {
    XmlNode::simple_record(name, fields, Default::default())
}

macro_rules! impl_distill_by_unit {
    ($ty:ty, $core:ident, $name:expr) => {
        use pretty_xmlish::XmlNode;
        use $crate::optimizer::plan_node::generic::DistillUnit;
        use $crate::optimizer::plan_node::utils::Distill;
        impl Distill for $ty {
            fn distill<'a>(&self) -> XmlNode<'a> {
                self.$core.distill_with_name($name)
            }
        }
    };
}
pub(crate) use impl_distill_by_unit;

pub(crate) fn column_names_pretty<'a>(schema: &Schema) -> Pretty<'a> {
    let columns = (schema.fields.iter())
        .map(|f| f.name.clone())
        .map(Pretty::from)
        .collect();
    Pretty::Array(columns)
}

pub(crate) fn watermark_pretty<'a>(
    watermark_columns: &FixedBitSet,
    schema: &Schema,
) -> Option<Pretty<'a>> {
    if watermark_columns.count_ones(..) > 0 {
        Some(watermark_fields_pretty(watermark_columns.ones(), schema))
    } else {
        None
    }
}
pub(crate) fn watermark_fields_pretty<'a>(
    watermark_columns: impl Iterator<Item = usize>,
    schema: &Schema,
) -> Pretty<'a> {
    let arr = watermark_columns
        .map(|idx| FieldDisplay(schema.fields.get(idx).unwrap()))
        .map(|d| Pretty::display(&d))
        .collect();
    Pretty::Array(arr)
}

#[derive(Clone, Copy)]
pub struct IndicesDisplay<'a> {
    pub indices: &'a [usize],
    pub schema: &'a Schema,
}

impl<'a> IndicesDisplay<'a> {
    /// Returns `None` means all
    pub fn from_join<'b, PlanRef: GenericPlanRef>(
        join: &'a generic::Join<PlanRef>,
        input_schema: &'a Schema,
    ) -> Pretty<'b> {
        let col_num = join.internal_column_num();
        let id = Self::from(&join.output_indices, col_num, input_schema);
        id.map_or_else(|| Pretty::from("all"), Self::distill)
    }

    /// Returns `None` means all
    fn from(indices: &'a [usize], col_num: usize, schema: &'a Schema) -> Option<Self> {
        if indices.iter().copied().eq(0..col_num) {
            return None;
        }
        Some(Self { indices, schema })
    }

    pub fn distill<'b>(self) -> Pretty<'b> {
        let vec = self.indices.iter().map(|&i| {
            let name = self.schema.fields.get(i).unwrap().name.clone();
            Pretty::from(name)
        });
        Pretty::Array(vec.collect())
    }
}

/// Call `debug_struct` on the given formatter to create a debug struct builder.
/// If a property list is provided, properties in it will be added to the struct name according to
/// the condition of that property.
macro_rules! plan_node_name {
    ($name:literal $(, { $prop:literal, $cond:expr } )* $(,)?) => {
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
            name
        }
    };
}
pub(crate) use plan_node_name;

use super::generic::{self, GenericPlanRef};
use super::pretty_config;
