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
use risingwave_pb::batch_plan::HopWindowNode;

use super::{
    LogicalHopWindow, PlanBase, PlanRef, PlanTreeNodeUnary, ToBatchProst, ToDistributedBatch,
};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Order, RequiredDist};

/// `BatchHopWindow` implements [`super::LogicalHopWindow`] to evaluate specified expressions on
/// input rows
#[derive(Debug, Clone)]
pub struct BatchHopWindow {
    pub base: PlanBase,
    logical: LogicalHopWindow,
}

impl BatchHopWindow {
    pub fn new(logical: LogicalHopWindow) -> Self {
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
        BatchHopWindow { base, logical }
    }
}

impl fmt::Display for BatchHopWindow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchHopWindow")
    }
}

impl PlanTreeNodeUnary for BatchHopWindow {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { BatchHopWindow }

impl ToDistributedBatch for BatchHopWindow {
    fn to_distributed(&self) -> Result<PlanRef> {
        self.to_distributed_with_required(&Order::any(), &RequiredDist::Any)
    }

    fn to_distributed_with_required(
        &self,
        required_order: &Order,
        required_dist: &RequiredDist,
    ) -> Result<PlanRef> {
        // The hop operator will generate a multiplication of its input rows,
        // so shuffling its input instead of its output will reduce the shuffling data
        // communication.
        // We pass the required dist to its input.
        let input_required = self
            .logical
            .o2i_col_mapping()
            .rewrite_required_distribution(required_dist);
        let new_input = self
            .input()
            .to_distributed_with_required(required_order, &input_required)?;
        let new_logical = self.logical.clone_with_input(new_input);
        let batch_plan = BatchHopWindow::new(new_logical);
        let batch_plan = required_order.enforce_if_not_satisfies(batch_plan.into())?;
        required_dist.enforce_if_not_satisfies(batch_plan, required_order)
    }
}

impl ToBatchProst for BatchHopWindow {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::HopWindow(HopWindowNode {
            time_col: Some(self.logical.time_col.to_proto()),
            window_slide: Some(self.logical.window_slide.into()),
            window_size: Some(self.logical.window_size.into()),
            output_indices: self
                .logical
                .output_indices
                .iter()
                .map(|&x| x as u32)
                .collect(),
        })
    }
}

impl ToLocalBatch for BatchHopWindow {
    fn to_local(&self) -> Result<PlanRef> {
        let new_input = self.input().to_local()?;
        Ok(self.clone_with_input(new_input).into())
    }
}
