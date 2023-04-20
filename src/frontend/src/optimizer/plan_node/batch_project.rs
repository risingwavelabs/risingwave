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

use std::fmt;

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::ProjectNode;
use risingwave_pb::expr::ExprNode;

use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
};
use crate::expr::{ExprImpl, ExprRewriter, Expr};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::utils::ColIndexMappingRewriteExt;

/// `BatchProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchProject {
    pub base: PlanBase,
    logical: generic::Project<PlanRef>,
}

impl BatchProject {
    pub fn new(logical: generic::Project<PlanRef>) -> Self {
        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(logical.input.distribution());
        let order = logical
            .i2o_col_mapping()
            .rewrite_provided_order(logical.input.order());

        let base = PlanBase::new_batch_from_logical(&logical, distribution, order);
        BatchProject { base, logical }
    }

    pub fn as_logical(&self) -> &generic::Project<PlanRef> {
        &self.logical
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        &self.logical.exprs
    }
}

impl fmt::Display for BatchProject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchProject", self.schema())
    }
}

impl PlanTreeNodeUnary for BatchProject {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}

impl_plan_tree_node_for_unary! { BatchProject }

impl ToDistributedBatch for BatchProject {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ToBatchPb for BatchProject {
    fn to_batch_prost_body(&self) -> NodeBody {
        let select_list = self
            .logical
            .exprs
            .iter()
            .map(|expr| expr.to_expr_proto())
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

impl ExprRewritable for BatchProject {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new(logical).into()
    }
}
