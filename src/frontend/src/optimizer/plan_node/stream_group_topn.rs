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

use std::collections::HashSet;

use risingwave_common::util::sort_util::OrderType;

use super::utils::TableCatalogBuilder;
use super::PlanBase;
use crate::optimizer::property::{Distribution, Order};
use crate::{PlanRef, TableCatalog};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct StreamGroupTopN {
    pub base: PlanBase,
    group_key: Vec<usize>,
    limit: usize,
    offset: usize,
    order: Order,
}

#[allow(dead_code)]
impl StreamGroupTopN {
    pub fn new(
        input: PlanRef,
        group_key: Vec<usize>,
        limit: usize,
        offset: usize,
        order: Order,
    ) -> Self {
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            Distribution::HashShard(group_key.clone()),
            false,
        );
        StreamGroupTopN {
            base,
            group_key,
            limit,
            offset,
            order,
        }
    }

    pub fn infer_internal_table_catalog(&self) -> TableCatalog {
        let schema = &self.base.schema;
        let dist_keys = self.base.dist.dist_column_indices().to_vec();
        let pk_indices = &self.base.logical_pk;
        let columns_fields = schema.fields().to_vec();
        let field_order = &self.order.field_order;
        let mut internal_table_catalog_builder = TableCatalogBuilder::new();

        columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });

        // Here we want the state table to store the states in the order we want, fisrtly in
        // ascending order by the columns specified by the group key, then by the columns specified
        // by `order`. If we do that, when the later group topN operator does a prefix scannimg with
        // the group key, we can fetch the data in the desired order.

        // Used to prevent duplicate additions
        let mut order_cols = HashSet::new();
        // order by group key first
        self.group_key.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending);
            order_cols.insert(*idx);
        });

        // order by field order recorded in `order` secondly.
        field_order.iter().for_each(|field_order| {
            if !order_cols.contains(&field_order.index) {
                internal_table_catalog_builder
                    .add_order_column(field_order.index, OrderType::from(field_order.direct));
                order_cols.insert(field_order.index);
            }
        });

        // record pk indices in table catalog
        pk_indices.iter().for_each(|idx| {
            if !order_cols.contains(idx) {
                internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending);
                order_cols.insert(*idx);
            }
        });
        internal_table_catalog_builder.build(dist_keys, self.base.append_only)
    }
}
