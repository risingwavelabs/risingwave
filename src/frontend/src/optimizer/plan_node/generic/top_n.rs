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

use std::collections::HashSet;

use risingwave_common::catalog::Schema;
use risingwave_common::util::sort_util::OrderType;

use super::super::utils::TableCatalogBuilder;
use super::{stream, GenericPlanNode, GenericPlanRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::Order;
use crate::TableCatalog;
/// `TopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopN<PlanRef> {
    pub input: PlanRef,
    pub limit: u64,
    pub offset: u64,
    pub with_ties: bool,
    pub order: Order,
    pub group_key: Vec<usize>,
}

impl<PlanRef: stream::StreamPlanRef> TopN<PlanRef> {
    /// Infers the state table catalog for [`StreamTopN`] and [`StreamGroupTopN`].
    pub fn infer_internal_table_catalog(
        &self,
        me: &impl stream::StreamPlanRef,
        vnode_col_idx: Option<usize>,
    ) -> TableCatalog {
        let schema = me.schema();
        let pk_indices = me.logical_pk();
        let columns_fields = schema.fields().to_vec();
        let field_order = &self.order.field_order;
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(me.ctx().with_options().internal_table_subset());

        columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        let mut order_cols = HashSet::new();

        // Here we want the state table to store the states in the order we want, firstly in
        // ascending order by the columns specified by the group key, then by the columns
        // specified by `order`. If we do that, when the later group topN operator
        // does a prefix scanning with the group key, we can fetch the data in the
        // desired order.
        self.group_key.iter().for_each(|&idx| {
            internal_table_catalog_builder.add_order_column(idx, OrderType::Ascending);
            order_cols.insert(idx);
        });

        field_order.iter().for_each(|field_order| {
            if !order_cols.contains(&field_order.index) {
                internal_table_catalog_builder
                    .add_order_column(field_order.index, OrderType::from(field_order.direct));
                order_cols.insert(field_order.index);
            }
        });

        pk_indices.iter().for_each(|idx| {
            if !order_cols.contains(idx) {
                internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending);
                order_cols.insert(*idx);
            }
        });
        if let Some(vnode_col_idx) = vnode_col_idx {
            internal_table_catalog_builder.set_vnode_col_idx(vnode_col_idx);
        }

        internal_table_catalog_builder.set_read_prefix_len_hint(self.group_key.len());
        internal_table_catalog_builder
            .build(self.input.distribution().dist_column_indices().to_vec())
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for TopN<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some(self.input.logical_pk().to_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
}
