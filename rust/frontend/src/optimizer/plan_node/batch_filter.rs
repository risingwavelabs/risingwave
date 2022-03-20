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

use risingwave_common::catalog::Schema;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::FilterNode;

use super::{LogicalFilter, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::PlanBase;
use crate::optimizer::property::{Distribution, WithSchema};
use crate::utils::Condition;

/// `BatchFilter` implements [`super::LogicalFilter`]
#[derive(Debug, Clone)]
pub struct BatchFilter {
    pub base: PlanBase,
    logical: LogicalFilter,
}

impl BatchFilter {
    pub fn new(logical: LogicalFilter) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            logical.input().distribution().clone(),
            logical.input().order().clone(),
        );
        BatchFilter { logical, base }
    }

    pub fn predicate(&self) -> &Condition {
        self.logical.predicate()
    }
}

impl fmt::Display for BatchFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BatchFilter {{ predicate: {} }}", self.predicate())
    }
}

impl PlanTreeNodeUnary for BatchFilter {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchFilter }

impl WithSchema for BatchFilter {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchFilter {
    fn to_distributed(&self) -> PlanRef {
        let new_input = self
            .input()
            .to_distributed_with_required(self.input_order_required(), Distribution::any());
        self.clone_with_input(new_input).into()
    }
}

impl ToBatchProst for BatchFilter {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Filter(FilterNode {
            search_condition: Some(self.logical.predicate().as_expr().to_protobuf()),
        })
    }
}
