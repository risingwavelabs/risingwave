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

use itertools::Itertools;
use risingwave_common::catalog::Field;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;

use super::generic::GenericPlanRef;
use super::utils::TableCatalogBuilder;
use crate::optimizer::property::Distribution;
use crate::TableCatalog;

pub trait StreamPlanRef: GenericPlanRef {
    fn distribution(&self) -> &Distribution;
    fn append_only(&self) -> bool;
    fn emit_on_window_close(&self) -> bool;
}

/// Implements [`generic::Join`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HashJoin {}
// impl_plan_tree_node_v2_for_stream_binary_node_with_core_delegating!(HashJoin, core, left, right);

impl HashJoin {
    /// Return hash join internal table catalog and degree table catalog.
    pub fn infer_internal_and_degree_table_catalog(
        input: &impl GenericPlanRef,
        join_key_indices: Vec<usize>,
        dk_indices_in_jk: Vec<usize>,
    ) -> (TableCatalog, TableCatalog, Vec<usize>) {
        let schema = input.schema();

        let internal_table_dist_keys = dk_indices_in_jk
            .iter()
            .map(|idx| join_key_indices[*idx])
            .collect_vec();

        let degree_table_dist_keys = dk_indices_in_jk.clone();

        // The pk of hash join internal and degree table should be join_key + input_pk.
        let join_key_len = join_key_indices.len();
        let mut pk_indices = join_key_indices;

        // dedup the pk in dist key..
        let mut deduped_input_pk_indices = vec![];
        for input_pk_idx in input.stream_key().unwrap() {
            if !pk_indices.contains(input_pk_idx)
                && !deduped_input_pk_indices.contains(input_pk_idx)
            {
                deduped_input_pk_indices.push(*input_pk_idx);
            }
        }

        pk_indices.extend(deduped_input_pk_indices.clone());

        // Build internal table
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(input.ctx().with_options().internal_table_subset());
        let internal_columns_fields = schema.fields().to_vec();

        internal_columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending())
        });

        // Build degree table.
        let mut degree_table_catalog_builder =
            TableCatalogBuilder::new(input.ctx().with_options().internal_table_subset());

        let degree_column_field = Field::with_name(DataType::Int64, "_degree");

        pk_indices.iter().enumerate().for_each(|(order_idx, idx)| {
            degree_table_catalog_builder.add_column(&internal_columns_fields[*idx]);
            degree_table_catalog_builder.add_order_column(order_idx, OrderType::ascending());
        });
        degree_table_catalog_builder.add_column(&degree_column_field);
        degree_table_catalog_builder
            .set_value_indices(vec![degree_table_catalog_builder.columns().len() - 1]);

        internal_table_catalog_builder.set_dist_key_in_pk(dk_indices_in_jk.clone());
        degree_table_catalog_builder.set_dist_key_in_pk(dk_indices_in_jk);

        (
            internal_table_catalog_builder.build(internal_table_dist_keys, join_key_len),
            degree_table_catalog_builder.build(degree_table_dist_keys, join_key_len),
            deduped_input_pk_indices,
        )
    }
}
