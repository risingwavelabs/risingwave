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

use pretty_xmlish::XmlNode;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::MaxOneRowNode;

use super::batch::prelude::*;
use super::generic::{DistillUnit, PhysicalPlanRef};
use super::utils::Distill;
use super::{
    generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchPb, ToDistributedBatch,
};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::ToLocalBatch;

/// [`BatchMaxOneRow`] fetches up to one row from the input, returning an error
/// if the input contains more than one row at runtime.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchMaxOneRow {
    pub base: PlanBase<Batch>,
    core: generic::MaxOneRow<PlanRef>,
}

impl BatchMaxOneRow {
    pub fn new(core: generic::MaxOneRow<PlanRef>) -> Self {
        let base = PlanBase::new_batch_with_core(
            &core,
            core.input.distribution().clone(),
            core.input.order().clone(),
        );
        BatchMaxOneRow { base, core }
    }
}

impl PlanTreeNodeUnary for BatchMaxOneRow {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let core = generic::MaxOneRow { input };

        Self::new(core)
    }
}
impl_plan_tree_node_for_unary! {BatchMaxOneRow}

impl Distill for BatchMaxOneRow {
    fn distill<'a>(&self) -> XmlNode<'a> {
        self.core.distill_with_name("BatchMaxOneRow")
    }
}

impl ToDistributedBatch for BatchMaxOneRow {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_input(self.input().to_distributed()?).into())
    }
}

impl ToBatchPb for BatchMaxOneRow {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::MaxOneRow(MaxOneRowNode {})
    }
}

impl ToLocalBatch for BatchMaxOneRow {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_input(self.input().to_local()?).into())
    }
}

impl ExprRewritable for BatchMaxOneRow {}

impl ExprVisitable for BatchMaxOneRow {}
