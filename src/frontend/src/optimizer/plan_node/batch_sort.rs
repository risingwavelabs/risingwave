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

use std::fmt;

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::OrderByNode;

use super::{PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Order, OrderDisplay};

/// `BatchSort` buffers all data from input and sort these rows by specified order, providing the
/// collation required by user or parent plan node.
#[derive(Debug, Clone)]
pub struct BatchSort {
    pub base: PlanBase,
    input: PlanRef,
}

impl BatchSort {
    pub fn new(input: PlanRef, order: Order) -> Self {
        let ctx = input.ctx();
        let schema = input.schema().clone();
        let dist = input.distribution().clone();
        let base = PlanBase::new_batch(ctx, schema, dist, order);
        BatchSort { base, input }
    }
}

impl fmt::Display for BatchSort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BatchSort {{ order: {} }}",
            OrderDisplay {
                order: self.order(),
                input_schema: self.input.schema()
            }
        )
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

impl ToDistributedBatch for BatchSort {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchProst for BatchSort {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_orders = self.base.order.to_protobuf(&self.base.schema);
        NodeBody::OrderBy(OrderByNode { column_orders })
    }
}

impl ToLocalBatch for BatchSort {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}
