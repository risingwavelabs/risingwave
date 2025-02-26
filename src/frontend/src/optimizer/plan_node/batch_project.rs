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
use risingwave_pb::batch_plan::ProjectNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::expr::ExprNode;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{
    ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::utils::ColIndexMappingRewriteExt;

/// `BatchProject` implements [`super::LogicalProject`] to evaluate specified expressions on input
/// rows
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchProject {
    pub base: PlanBase<Batch>,
    core: generic::Project<PlanRef>,
}

impl BatchProject {
    pub fn new(core: generic::Project<PlanRef>) -> Self {
        let distribution = core
            .i2o_col_mapping()
            .rewrite_provided_distribution(core.input.distribution());
        let order = core
            .i2o_col_mapping()
            .rewrite_provided_order(core.input.order());

        let base = PlanBase::new_batch_with_core(&core, distribution, order);
        BatchProject { base, core }
    }

    pub fn as_logical(&self) -> &generic::Project<PlanRef> {
        &self.core
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        &self.core.exprs
    }
}

impl Distill for BatchProject {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("BatchProject", self.core.fields_pretty(self.schema()))
    }
}

impl PlanTreeNodeUnary for BatchProject {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
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
            .core
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
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}

impl ExprVisitable for BatchProject {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
