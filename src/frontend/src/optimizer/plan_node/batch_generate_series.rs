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
use risingwave_pb::batch_plan::GenerateSeriesNode;

use super::{
    LogicalGenerateSeries, PlanBase, PlanRef, PlanTreeNodeLeaf, ToBatchProst, ToDistributedBatch,
};
use crate::expr::Expr;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone)]
pub struct BatchGenerateSeries {
    pub base: PlanBase,
    logical: LogicalGenerateSeries,
}

impl PlanTreeNodeLeaf for BatchGenerateSeries {}
impl_plan_tree_node_for_leaf!(BatchGenerateSeries);

impl BatchGenerateSeries {
    pub fn new(logical: LogicalGenerateSeries) -> Self {
        Self::with_dist(logical, Distribution::Single)
    }

    pub fn with_dist(logical: LogicalGenerateSeries, dist: Distribution) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any().clone());
        BatchGenerateSeries { base, logical }
    }

    #[must_use]
    pub fn logical(&self) -> &LogicalGenerateSeries {
        &self.logical
    }
}

impl fmt::Display for BatchGenerateSeries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchGenerateSeries")
    }
}

impl ToDistributedBatch for BatchGenerateSeries {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.logical().clone(), Distribution::Single).into())
    }
}

impl ToBatchProst for BatchGenerateSeries {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::GenerateSeries(GenerateSeriesNode {
            start: Some(self.logical.start.to_expr_proto()),
            stop: Some(self.logical.stop.to_expr_proto()),
            step: Some(self.logical.step.to_expr_proto()),
        })
    }
}

impl ToLocalBatch for BatchGenerateSeries {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.logical().clone(), Distribution::Single).into())
    }
}
