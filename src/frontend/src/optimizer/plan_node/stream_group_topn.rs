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
use super::{LogicalTopN, PlanBase, PlanTreeNodeUnary};
use crate::optimizer::property::Distribution;
use crate::TableCatalog;

#[derive(Debug, Clone)]
pub struct StreamGroupTopN {
    pub base: PlanBase,
    logical: LogicalTopN,
    group_key: Vec<usize>,
}

impl StreamGroupTopN {
    #[allow(dead_code)]
    pub fn new(logical: LogicalTopN, group_key: Vec<usize>) -> Self {
        let dist = match logical.input().distribution() {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        let base = PlanBase::new_stream(
            logical.base.ctx.clone(),
            logical.schema().clone(),
            logical.input().pk_indices().to_vec(),
            dist,
            false,
        );
        StreamGroupTopN {
            base,
            logical,
            group_key,
        }
    }

    #[allow(dead_code)]
    pub fn infer_internal_table_catalog(&self) -> TableCatalog {
        let schema = &self.base.schema;
        let dist_keys = self.base.dist.dist_column_indices().to_vec();
        let pk_indices = &self.base.pk_indices;
        let columns_fields = schema.fields().to_vec();
        let field_order = &self.logical.order().field_order;
        let mut internal_table_catalog_builder = TableCatalogBuilder::new();

        columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });

        // order by group key first
        let mut order_cols = HashSet::new();
        self.group_key.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending);
            order_cols.insert(*idx);
        });

        // order by field order record in `logical` secondly.
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
