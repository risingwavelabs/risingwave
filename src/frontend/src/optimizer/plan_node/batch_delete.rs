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

use risingwave_pb::batch_plan::DeleteNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{
    ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{ToLocalBatch, utils};
use crate::optimizer::plan_visitor::DistributedDmlVisitor;
use crate::optimizer::property::{Distribution, Order, RequiredDist};

/// `BatchDelete` implements [`super::LogicalDelete`]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchDelete {
    pub base: PlanBase<Batch>,
    pub core: generic::Delete<PlanRef>,
}

impl BatchDelete {
    pub fn new(core: generic::Delete<PlanRef>) -> Self {
        let base =
            PlanBase::new_batch_with_core(&core, core.input.distribution().clone(), Order::any());
        Self { base, core }
    }
}

impl PlanTreeNodeUnary for BatchDelete {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { BatchDelete }
impl_distill_by_unit!(BatchDelete, core, "BatchDelete");

impl ToDistributedBatch for BatchDelete {
    fn to_distributed(&self) -> Result<PlanRef> {
        if DistributedDmlVisitor::dml_should_run_in_distributed(self.input()) {
            // Add an hash shuffle between the delete and its input.
            let new_input = RequiredDist::PhysicalDist(Distribution::HashShard(
                (0..self.input().schema().len()).collect(),
            ))
            .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
            let new_delete: PlanRef = self.clone_with_input(new_input).into();
            if self.core.returning {
                Ok(new_delete)
            } else {
                utils::sum_affected_row(new_delete)
            }
        } else {
            let new_input = RequiredDist::single()
                .enforce_if_not_satisfies(self.input().to_distributed()?, &Order::any())?;
            Ok(self.clone_with_input(new_input).into())
        }
    }
}

impl ToBatchPb for BatchDelete {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::Delete(DeleteNode {
            table_id: self.core.table_id.table_id(),
            table_version_id: self.core.table_version_id,
            returning: self.core.returning,
            session_id: self.base.ctx().session_ctx().session_id().0 as u32,
        })
    }
}

impl ToLocalBatch for BatchDelete {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = RequiredDist::single()
            .enforce_if_not_satisfies(self.input().to_local()?, &Order::any())?;
        Ok(self.clone_with_input(new_input).into())
    }
}

impl ExprRewritable for BatchDelete {}

impl ExprVisitable for BatchDelete {}
