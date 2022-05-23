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
use risingwave_pb::batch_plan::ProjectNode;
use risingwave_pb::expr::ExprNode;

use super::{
    LogicalProject, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch,
};
use crate::expr::Expr;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows
#[derive(Debug, Clone)]
pub struct BatchProject {
    pub base: PlanBase,
    logical: LogicalProject,
}

impl BatchProject {
    pub fn new(logical: LogicalProject) -> Self {
        let ctx = logical.base.ctx.clone();
        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(logical.input().distribution());

        // TODO: Derive order from input
        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            distribution,
            Order::any().clone(),
        );
        BatchProject { base, logical }
    }
}

impl fmt::Display for BatchProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchProject")
    }
}

impl PlanTreeNodeUnary for BatchProject {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchProject }

impl ToDistributedBatch for BatchProject {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }

    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &RequiredDist,
    ) -> Result<PlanRef> {
        let input_required = if required_dist.satisfies(&RequiredDist::AnyShard) {
            RequiredDist::Any
        } else {
            let input_required = self
                .logical
                .o2i_col_mapping()
                .rewrite_required_distribution(required_dist);
            match input_required {
                RequiredDist::PhysicalDist(dist) => match dist {
                    Distribution::Single | Distribution::Broadcast => RequiredDist::Any,
                    _ => RequiredDist::PhysicalDist(dist),
                },
                _ => input_required,
            }
        };
        let new_input = self
            .input()
            .to_distributed_with_required(required_order, &input_required)?;
        let new_logical = self.logical.clone_with_input(new_input);
        let batch_plan = BatchProject::new(new_logical);
        let batch_plan = required_order.enforce_if_not_satisfies(batch_plan.into())?;
        required_dist.enforce_if_not_satisfies(batch_plan, required_order)
    }
}

impl ToBatchProst for BatchProject {
    fn to_batch_prost_body(&self) -> NodeBody {
        let select_list = self
            .logical
            .exprs()
            .iter()
            .map(Expr::to_expr_proto)
            .collect::<Vec<ExprNode>>();
        NodeBody::Project(ProjectNode { select_list })
    }
}

impl ToLocalBatch for BatchProject {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}
