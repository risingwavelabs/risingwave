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
use std::fmt;

use pretty_xmlish::{Pretty, Str, XmlNode};
use risingwave_common::catalog::Schema;
use risingwave_common::util::sort_util::OrderType;

use super::super::utils::TableCatalogBuilder;
use super::{stream, DistillUnit, GenericPlanNode, GenericPlanRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::utils::childless_record;
use crate::optimizer::property::{FunctionalDependencySet, Order, OrderDisplay};
use crate::TableCatalog;

/// `TopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopN<PlanRef> {
    pub input: PlanRef,
    pub limit_attr: TopNLimit,
    pub offset: u64,
    pub order: Order,
    pub group_key: Vec<usize>,
}

impl<PlanRef: stream::StreamPlanRef> TopN<PlanRef> {
    /// Infers the state table catalog for [`StreamTopN`] and [`StreamGroupTopN`].
    pub fn infer_internal_table_catalog(
        &self,
        schema: &Schema,
        ctx: OptimizerContextRef,
        stream_key: &[usize],
        vnode_col_idx: Option<usize>,
    ) -> TableCatalog {
        let columns_fields = schema.fields().to_vec();
        let column_orders = &self.order.column_orders;
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(ctx.with_options().internal_table_subset());

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
            internal_table_catalog_builder.add_order_column(idx, OrderType::ascending());
            order_cols.insert(idx);
        });

        let read_prefix_len_hint = internal_table_catalog_builder.get_current_pk_len();
        column_orders.iter().for_each(|order| {
            internal_table_catalog_builder.add_order_column(order.column_index, order.order_type);
            order_cols.insert(order.column_index);
        });

        stream_key.iter().for_each(|idx| {
            if !order_cols.contains(idx) {
                internal_table_catalog_builder.add_order_column(*idx, OrderType::ascending());
                order_cols.insert(*idx);
            }
        });
        if let Some(vnode_col_idx) = vnode_col_idx {
            internal_table_catalog_builder.set_vnode_col_idx(vnode_col_idx);
        }

        internal_table_catalog_builder.build(
            self.input.distribution().dist_column_indices().to_vec(),
            read_prefix_len_hint,
        )
    }
}

impl<PlanRef: GenericPlanRef> TopN<PlanRef> {
    pub fn with_group(
        input: PlanRef,
        limit_attr: TopNLimit,
        offset: u64,
        order: Order,
        group_key: Vec<usize>,
    ) -> Self {
        if limit_attr.with_ties() {
            assert!(offset == 0, "WITH TIES is not supported with OFFSET");
        }

        Self {
            input,
            limit_attr,
            offset,
            order,
            group_key,
        }
    }

    pub fn without_group(input: PlanRef, limit_attr: TopNLimit, offset: u64, order: Order) -> Self {
        if limit_attr.with_ties() {
            assert!(offset == 0, "WITH TIES is not supported with OFFSET");
        }

        Self {
            input,
            limit_attr,
            offset,
            order,
            group_key: vec![],
        }
    }

    pub(crate) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        let input_schema = self.input.schema();
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
            .field("limit", &self.limit_attr.limit())
            .field("offset", &self.offset);
        if self.limit_attr.with_ties() {
            builder.field("with_ties", &true);
        }
        if !self.group_key.is_empty() {
            builder.field("group_key", &self.group_key);
        }
        builder.finish()
    }
}

impl<PlanRef: GenericPlanRef> DistillUnit for TopN<PlanRef> {
    fn distill_with_name<'a>(&self, name: impl Into<Str<'a>>) -> XmlNode<'a> {
        let mut vec = Vec::with_capacity(5);
        let input_schema = self.input.schema();
        let order_d = Pretty::display(&OrderDisplay {
            order: &self.order,
            input_schema,
        });
        vec.push(("order", order_d));
        vec.push(("limit", Pretty::debug(&self.limit_attr.limit())));
        vec.push(("offset", Pretty::debug(&self.offset)));
        if self.limit_attr.with_ties() {
            vec.push(("with_ties", Pretty::debug(&true)));
        }
        if !self.group_key.is_empty() {
            vec.push(("group_key", Pretty::debug(&self.group_key)));
        }
        childless_record(name, vec)
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for TopN<PlanRef> {
    fn schema(&self) -> Schema {
        self.input.schema().clone()
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        // We can use the group key as the stream key when there is at most one record for each
        // value of the group key.
        if self.limit_attr.max_one_row() {
            Some(self.group_key.clone())
        } else {
            Some(self.input.logical_pk().to_vec())
        }
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }

    fn functional_dependency(&self) -> FunctionalDependencySet {
        self.input.functional_dependency().clone()
    }
}

/// [`Limit`] is used to specify the number of records to return.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TopNLimit {
    /// The number of records returned is exactly the same as the number after `LIMIT` in the SQL
    /// query.
    Simple(u64),
    /// If the SQL query contains `WITH TIES`, then it is supposed to bring all records with the
    /// same value even if the number of records exceeds the number specified after `LIMIT` in the
    /// query.
    WithTies(u64),
}

impl TopNLimit {
    pub fn new(limit: u64, with_ties: bool) -> Self {
        if with_ties {
            Self::WithTies(limit)
        } else {
            Self::Simple(limit)
        }
    }

    pub fn limit(&self) -> u64 {
        match self {
            TopNLimit::Simple(limit) => *limit,
            TopNLimit::WithTies(limit) => *limit,
        }
    }

    pub fn with_ties(&self) -> bool {
        match self {
            TopNLimit::Simple(_) => false,
            TopNLimit::WithTies(_) => true,
        }
    }

    /// Whether this [`Limit`] returns at most one record for each value. Only `LIMIT 1` without
    /// `WITH TIES` satisfies this condition.
    pub fn max_one_row(&self) -> bool {
        match self {
            TopNLimit::Simple(limit) => *limit == 1,
            TopNLimit::WithTies(_) => false,
        }
    }
}
