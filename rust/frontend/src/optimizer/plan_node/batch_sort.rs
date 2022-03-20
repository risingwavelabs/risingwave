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
//
use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::{ColumnOrder, OrderByNode};

use super::{BatchBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::property::{Distribution, Order, WithOrder, WithSchema};

/// `BatchSort` buffers all data from input and sort these rows by specified order, providing the
/// collation required by user or parent plan node.
#[derive(Debug, Clone)]
pub struct BatchSort {
    pub base: BatchBase,
    input: PlanRef,
    schema: Schema,
}

impl BatchSort {
    pub fn new(input: PlanRef, order: Order) -> Self {
        let ctx = input.ctx();
        let schema = input.schema().clone();
        let dist = input.distribution().clone();
        let base = BatchBase {
            order,
            dist,
            ctx: ctx.clone(),
            id: ctx.borrow_mut().get_id(),
        };
        BatchSort {
            input,
            base,
            schema,
        }
    }
}

impl fmt::Display for BatchSort {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BatchSort {{ order: {} }}", self.order())
    }
}

impl PlanTreeNodeUnary for BatchSort {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }
    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.base.order.clone())
    }
}
impl_plan_tree_node_for_unary! {BatchSort}

impl WithSchema for BatchSort {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ToDistributedBatch for BatchSort {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchSort {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_orders_without_type = self.base.order.to_protobuf();
        let column_types = self
            .base
            .order
            .field_order
            .iter()
            .map(|field_order| self.schema[field_order.index].data_type.to_protobuf())
            .collect_vec();
        let column_orders = column_orders_without_type
            .into_iter()
            .zip_eq(column_types.into_iter())
            .map(|((input_ref, order_type), return_type)| ColumnOrder {
                order_type: order_type as i32,
                input_ref: Some(input_ref),
                return_type: Some(return_type),
            })
            .collect_vec();
        NodeBody::OrderBy(OrderByNode { column_orders })
    }
}
