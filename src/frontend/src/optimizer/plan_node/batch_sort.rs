// Copyright 2025 RisingWave Labs
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

use pretty_xmlish::XmlNode;
use risingwave_pb::batch_plan::SortNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Order, OrderDisplay};

/// `BatchSort` buffers all data from input and sort these rows by specified order, providing the
/// collation required by user or parent plan node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSort {
    pub base: PlanBase<Batch>,
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

impl Distill for BatchSort {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let data = OrderDisplay {
            order: self.order(),
            input_schema: self.input.schema(),
        };
        childless_record("BatchSort", vec![("order", data.distill())])
    }
}

impl PlanTreeNodeUnary for BatchSort {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.base.order().clone())
    }
}
impl_plan_tree_node_for_unary! {BatchSort}

impl ToDistributedBatch for BatchSort {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchSort {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_orders = self.base.order().to_protobuf();
        NodeBody::Sort(SortNode { column_orders })
    }
}

impl ToLocalBatch for BatchSort {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchSort {}

impl ExprVisitable for BatchSort {}
