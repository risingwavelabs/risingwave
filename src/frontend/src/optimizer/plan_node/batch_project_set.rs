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

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::ProjectSetNode;

use crate::optimizer::plan_node::{
    LogicalProjectSet, PlanBase, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch, ToLocalBatch,
};
use crate::optimizer::PlanRef;

#[derive(Debug, Clone)]
pub struct BatchProjectSet {
    pub base: PlanBase,
    logical: LogicalProjectSet,
}

impl BatchProjectSet {
    pub fn new(logical: LogicalProjectSet) -> Self {
        let ctx = logical.base.ctx.clone();
        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(logical.input().distribution());

        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            distribution,
            logical.get_out_column_index_order(),
        );
        BatchProjectSet { base, logical }
    }
}

impl fmt::Display for BatchProjectSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchProjectSet")
    }
}

impl PlanTreeNodeUnary for BatchProjectSet {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchProjectSet }

impl ToDistributedBatch for BatchProjectSet {
    fn to_distributed(&self) -> Result<PlanRef> {
        let new_input = self.input().to_distributed()?;
        Ok(self.clone_with_input(new_input).into())
    }

    // TODO: implement to_distributed_with_required like BatchProject
}

impl ToBatchProst for BatchProjectSet {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::ProjectSet(ProjectSetNode {
            select_list: self
                .logical
                .select_list()
                .iter()
                .map(|select_item| select_item.to_project_set_select_item_proto())
                .collect_vec(),
        })
    }
}

impl ToLocalBatch for BatchProjectSet {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}
