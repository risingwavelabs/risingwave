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
use std::fmt;

use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;

use super::utils::TableCatalogBuilder;
use super::{PlanBase, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{Distribution, Order, OrderDisplay};
use crate::{PlanRef, TableCatalog};

#[derive(Debug, Clone)]
pub struct StreamGroupTopN {
    pub base: PlanBase,
    input: PlanRef,
    group_key: Vec<usize>,
    limit: usize,
    offset: usize,
    order: Order,
}

impl StreamGroupTopN {
    pub fn new(
        input: PlanRef,
        group_key: Vec<usize>,
        limit: usize,
        offset: usize,
        order: Order,
    ) -> Self {
        let dist = match input.distribution() {
            Distribution::HashShard(_) => Distribution::HashShard(group_key.clone()),
            Distribution::UpstreamHashShard(_) => {
                Distribution::UpstreamHashShard(group_key.clone())
            }
            _ => input.distribution().clone(),
        };
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            input.functional_dependency().clone(),
            dist,
            false,
        );
        StreamGroupTopN {
            base,
            input,
            group_key,
            limit,
            offset,
            order,
        }
    }

    pub fn infer_internal_table_catalog(&self) -> TableCatalog {
        let schema = &self.base.schema;
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
        internal_table_catalog_builder.build(vec![], self.base.append_only)
    }
}

impl ToStreamProst for StreamGroupTopN {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;
        let group_key = self.group_key.iter().map(|idx| *idx as u32).collect();

        if self.limit == 0 {
            panic!("topN's limit shouldn't be 0.");
        }
        let group_topn_node = GroupTopNNode {
            limit: self.limit as u64,
            offset: self.offset as u64,
            group_key,
            table: Some(self.infer_internal_table_catalog().to_state_table_prost()),
        };

        ProstStreamNode::GroupTopN(group_topn_node)
    }
}

impl fmt::Display for StreamGroupTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("StreamGroupTopN");
        let input = self.input();
        let input_schema = input.schema();
        builder.field(
            "order",
            &format!(
                "{}",
                OrderDisplay {
                    order: &self.order,
                    input_schema
                }
            ),
        );
        builder
            .field("limit", &format_args!("{}", self.limit))
            .field("offset", &format_args!("{}", self.offset))
            .field("group_key", &format_args!("{:?}", self.group_key))
            .finish()
    }
}

impl_plan_tree_node_for_unary! { StreamGroupTopN }

impl PlanTreeNodeUnary for StreamGroupTopN {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            input,
            self.group_key.clone(),
            self.limit,
            self.offset,
            self.order.clone(),
        )
    }
}
