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
use risingwave_pb::batch_plan::FilterNode;

use super::{LogicalFilter, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::{PlanBase, ToLocalBatch};
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
        BatchFilter { base, logical }
    }

    pub fn predicate(&self) -> &Condition {
        self.logical.predicate()
    }
}

impl fmt::Display for BatchFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchFilter")
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

impl ToDistributedBatch for BatchFilter {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchProst for BatchFilter {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Filter(FilterNode {
            search_condition: Some(
                ExprImpl::from(self.logical.predicate().clone()).to_expr_proto(),
            ),
        })
    }
}

impl ToLocalBatch for BatchFilter {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}
