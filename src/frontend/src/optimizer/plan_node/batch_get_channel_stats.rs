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

/// `BatchGetChannelStats` represents a batch plan node that retrieves channel statistics
/// from the dashboard API. It has no inputs and returns channel stats data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchGetChannelStats {
    pub base: PlanBase<Batch>,
    pub at_time: Option<u64>,
    pub time_offset: u64,
}

impl PlanTreeNodeLeaf for BatchGetChannelStats {}
impl_plan_tree_node_for_leaf! { Batch, BatchGetChannelStats }

impl BatchGetChannelStats {
    pub fn new(
        ctx: crate::OptimizerContextRef,
        schema: Schema,
        at_time: Option<u64>,
        time_offset: u64,
    ) -> Self {
        Self::with_dist(ctx, schema, at_time, time_offset, Distribution::Single)
    }

    pub fn with_dist(
        ctx: crate::OptimizerContextRef,
        schema: Schema,
        at_time: Option<u64>,
        time_offset: u64,
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
    pub fn time_offset(&self) -> u64 {
        self.time_offset
    }
}

impl Distill for BatchGetChannelStats {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![
            ("at_time", Pretty::debug(&self.at_time)),
            ("time_offset", Pretty::debug(&self.time_offset)),
        ];
        childless_record("BatchGetChannelStats", fields)
    }
}

impl ToDistributedBatch for BatchGetChannelStats {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(
            self.base.ctx().clone(),
            self.base.schema().clone(),
            self.at_time,
            self.time_offset,
            Distribution::Single,
        )
        .into())
    }
}

impl ToBatchPb for BatchGetChannelStats {
    fn to_batch_prost_body(&self) -> NodeBody {
        // TODO: In a real implementation, this would create a proper protobuf message
        // For now, we'll use a placeholder that indicates this is a channel stats node
        // This would need to be properly implemented based on the actual protobuf schema
        unimplemented!("Protobuf serialization not yet implemented for BatchGetChannelStats")
    }
}

impl ToLocalBatch for BatchGetChannelStats {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::with_dist(
            self.base.ctx().clone(),
            self.base.schema().clone(),
            self.at_time,
            self.time_offset,
            Distribution::Single,
        )
        .into())
    }
}

impl ExprRewritable<Batch> for BatchGetChannelStats {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _r: &mut dyn crate::expr::ExprRewriter) -> PlanRef {
        self.clone().into()
    }
}

impl crate::optimizer::plan_node::expr_visitable::ExprVisitable for BatchGetChannelStats {
    fn visit_exprs(&self, _v: &mut dyn crate::expr::ExprVisitor) {
        // No expressions to visit
    }
}
