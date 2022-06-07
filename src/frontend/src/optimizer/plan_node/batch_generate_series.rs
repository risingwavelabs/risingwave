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
use risingwave_pb::batch_plan::GenerateSeriesNode;

use super::{PlanBase, PlanRef, PlanTreeNodeLeaf, ToBatchProst, ToDistributedBatch};
use crate::expr::Expr;
use crate::optimizer::plan_node::logical_generate_series::LogicalSeries;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order};

#[derive(Debug, Clone)]
pub struct BatchSeries {
    pub base: PlanBase,
    logical: LogicalSeries,
}

impl PlanTreeNodeLeaf for BatchSeries {}
impl_plan_tree_node_for_leaf!(BatchSeries);

impl BatchSeries {
    pub fn new(logical: LogicalSeries) -> Self {
        Self::with_dist(logical, Distribution::Single)
    }

    pub fn with_dist(logical: LogicalSeries, dist: Distribution) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any().clone());
        BatchSeries { base, logical }
    }

    #[must_use]
    pub fn logical(&self) -> &LogicalSeries {
        &self.logical
    }
}

impl fmt::Display for BatchSeries {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "BatchGenerateSeries")
    }
}

impl ToDistributedBatch for BatchSeries {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.logical().clone(), Distribution::Single).into())
    }
}

impl ToBatchProst for BatchSeries {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::GenerateSeries(GenerateSeriesNode {
            series_type: self.logical.series_type.clone() as i32,
            args: self
                .logical
                .args
                .iter()
                .map(|c| c.to_expr_proto())
                .collect_vec(),
            data_type: Some(self.logical.data_type.to_protobuf()),
        })
    }
}

impl ToLocalBatch for BatchSeries {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(self.logical().clone(), Distribution::Single).into())
    }
}
