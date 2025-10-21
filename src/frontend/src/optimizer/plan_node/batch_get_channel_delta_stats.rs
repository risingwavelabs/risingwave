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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, PlanTreeNodeLeaf, ToBatchPb,
    ToDistributedBatch,
};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, Order};

/// `BatchGetChannelDeltaStats` represents a batch plan node that retrieves channel statistics
/// from the dashboard API. It has no inputs and returns channel stats data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchGetChannelDeltaStats {
    pub base: PlanBase<Batch>,
    pub at_time: Option<u64>,
    pub time_offset: Option<u64>,
}

impl PlanTreeNodeLeaf for BatchGetChannelDeltaStats {}
impl_plan_tree_node_for_leaf! { Batch, BatchGetChannelDeltaStats }

impl BatchGetChannelDeltaStats {
    pub fn new(
        ctx: crate::OptimizerContextRef,
        schema: Schema,
        at_time: Option<u64>,
        time_offset: Option<u64>,
    ) -> Self {
        Self::with_dist(ctx, schema, at_time, time_offset, Distribution::Single)
    }

    pub fn with_dist(
        ctx: crate::OptimizerContextRef,
        schema: Schema,
        at_time: Option<u64>,
        time_offset: Option<u64>,
        dist: Distribution,
    ) -> Self {
        let base = PlanBase::new_batch(ctx, schema, dist, Order::any());
        Self {
            base,
            at_time,
            time_offset,
        }
    }

    /// Get the `at_time` parameter
    pub fn at_time(&self) -> Option<u64> {
        self.at_time
    }

    /// Get the `time_offset` parameter
    pub fn time_offset(&self) -> Option<u64> {
        self.time_offset
    }
}

impl Distill for BatchGetChannelDeltaStats {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![
            ("at_time", Pretty::debug(&self.at_time)),
            ("time_offset", Pretty::debug(&self.time_offset)),
        ];
        childless_record("BatchGetChannelDeltaStats", fields)
    }
}

impl ToDistributedBatch for BatchGetChannelDeltaStats {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(
            self.base.ctx(),
            self.base.schema().clone(),
            self.at_time,
            self.time_offset,
            Distribution::Single,
        )
        .into())
    }
}

impl ToBatchPb for BatchGetChannelDeltaStats {
    fn to_batch_prost_body(&self) -> NodeBody {
        use risingwave_pb::batch_plan::GetChannelDeltaStatsNode;

        NodeBody::GetChannelDeltaStats(GetChannelDeltaStatsNode {
            at_time: self.at_time,
            time_offset: self.time_offset,
        })
    }
}

impl ToLocalBatch for BatchGetChannelDeltaStats {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(
            self.base.ctx(),
            self.base.schema().clone(),
            self.at_time,
            self.time_offset,
            Distribution::Single,
        )
        .into())
    }
}

impl ExprRewritable<Batch> for BatchGetChannelDeltaStats {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn crate::expr::ExprRewriter) -> PlanRef {
        self.clone().into()
    }
}

impl crate::optimizer::plan_node::expr_visitable::ExprVisitable for BatchGetChannelDeltaStats {
    fn visit_exprs(&self, _v: &mut dyn crate::expr::ExprVisitor) {
        // No expressions to visit
    }
}
