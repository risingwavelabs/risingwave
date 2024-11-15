// Copyright 2024 RisingWave Labs
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

use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::UpdateNode;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
};
use crate::error::Result;
use crate::expr::{Expr, ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{utils, ToLocalBatch};
use crate::optimizer::plan_visitor::DistributedDmlVisitor;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchUpdate` implements [`super::LogicalUpdate`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchUpdate {
    pub base: PlanBase<Batch>,
    pub core: generic::Update<PlanRef>,
}

impl BatchUpdate {
    pub fn new(core: generic::Update<PlanRef>, schema: Schema) -> Self {
        let ctx = core.input.ctx();
        let base =
            PlanBase::new_batch(ctx, schema, core.input.distribution().clone(), Order::any());
        Self { base, core }
    }
}

impl PlanTreeNodeUnary for BatchUpdate {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core, self.schema().clone())
    }
}

impl_plan_tree_node_for_unary! { BatchUpdate }
impl_distill_by_unit!(BatchUpdate, core, "BatchUpdate");

impl ToDistributedBatch for BatchUpdate {
    fn to_distributed(&self) -> Result<PlanRef> {
        if DistributedDmlVisitor::dml_should_run_in_distributed(self.input()) {
            // Add an hash shuffle between the update and its input.
            let new_input = RequiredDist::PhysicalDist(Distribution::HashShard(
                (0..self.input().schema().len()).collect(),
            ))
            .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
            let new_update: PlanRef = self.clone_with_input(new_input).into();
            if self.core.returning {
                Ok(new_update)
            } else {
                utils::sum_affected_row(new_update)
            }
        } else {
            let new_input = RequiredDist::single()
                .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
            Ok(self.clone_with_input(new_input).into())
        }
    }
}

impl ToBatchPb for BatchUpdate {
    fn to_batch_prost_body(&self) -> NodeBody {
        let old_exprs = (self.core.old_exprs)
            .iter()
            .map(|x| x.to_expr_proto())
            .collect();
        let new_exprs = (self.core.new_exprs)
            .iter()
            .map(|x| x.to_expr_proto())
            .collect();

        NodeBody::Update(UpdateNode {
            table_id: self.core.table_id.table_id(),
            table_version_id: self.core.table_version_id,
            returning: self.core.returning,
            old_exprs,
            new_exprs,
            session_id: self.base.ctx().session_ctx().session_id().0 as u32,
        })
    }
}

impl ToLocalBatch for BatchUpdate {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchUpdate {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.schema().clone()).into()
    }
}

impl ExprVisitable for BatchUpdate {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
