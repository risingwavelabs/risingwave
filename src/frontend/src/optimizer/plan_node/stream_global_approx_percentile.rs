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

use fixedbitset::FixedBitSet;
use pretty_xmlish::XmlNode;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanRef, PhysicalPlanRef};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::utils::{childless_record, watermark_pretty, Distill};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanAggCall, PlanBase, PlanTreeNodeUnary, Stream, StreamHopWindow,
    StreamKeyedMerge, StreamNode,
};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::PlanRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamGlobalApproxPercentile {
    pub base: PlanBase<Stream>,
    input: PlanRef,
}

impl StreamGlobalApproxPercentile {
    pub fn new(input: PlanRef, approx_percentile_agg_call: &PlanAggCall) -> Self {
        let schema = Schema::new(vec![Field::new("approx_percentile", DataType::Float64)]);
        let watermark_columns = FixedBitSet::with_capacity(1);
        let base = PlanBase::new_stream(
            input.ctx(),
            schema,
            Some(vec![0]),
            input.functional_dependency().clone(),
            Distribution::Single,
            input.append_only(),
            input.emit_on_window_close(),
            watermark_columns,
            input.columns_monotonicity().clone(),
        );
        Self { base, input }
    }
}

impl Distill for StreamGlobalApproxPercentile {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("StreamGlobalApproxPercentile", vec![])
    }
}

impl PlanTreeNodeUnary for StreamGlobalApproxPercentile {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self {
            base: self.base.clone(),
            input,
        }
    }
}

impl_plan_tree_node_for_unary! {StreamGlobalApproxPercentile}

impl StreamNode for StreamGlobalApproxPercentile {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        todo!()
    }
}

impl ExprRewritable for StreamGlobalApproxPercentile {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) -> PlanRef {
        unimplemented!()
    }
}

impl ExprVisitable for StreamGlobalApproxPercentile {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {}
}
