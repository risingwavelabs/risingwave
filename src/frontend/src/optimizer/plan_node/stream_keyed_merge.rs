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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::HopWindowNode;

use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::utils::{childless_record, Distill};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanTreeNodeBinary, Stream, StreamHopWindow,
    StreamLocalApproxPercentile, StreamNode,
};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::PlanRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamKeyedMerge {
    pub base: PlanBase<Stream>,
    pub lhs_input: PlanRef,
    pub rhs_input: PlanRef,
    /// Maps input from the lhs to the output.
    pub lhs_mapping: ColIndexMapping,
    /// Maps input from the rhs to the output.
    pub rhs_mapping: ColIndexMapping,
}

impl StreamKeyedMerge {
    pub fn new(
        lhs_input: PlanRef,
        rhs_input: PlanRef,
        lhs_mapping: ColIndexMapping,
        rhs_mapping: ColIndexMapping,
    ) -> Self {
        let base = PlanBase::new_stream(
            lhs_input.ctx(),
            lhs_input.schema().clone(),
            lhs_input.stream_key().map(|k| k.to_vec()),
            lhs_input.functional_dependency().clone(),
            lhs_input.distribution().clone(),
            lhs_input.append_only(),
            lhs_input.emit_on_window_close(),
            lhs_input.watermark_columns().clone(),
            lhs_input.columns_monotonicity().clone(),
        );
        Self {
            base,
            lhs_input,
            rhs_input,
            lhs_mapping,
            rhs_mapping,
        }
    }
}

impl Distill for StreamKeyedMerge {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut out = Vec::with_capacity(2);
        out.push(("lhs_col_mapping", Pretty::debug(&self.lhs_mapping)));
        out.push(("rhs_col_mapping", Pretty::debug(&self.rhs_mapping)));
        childless_record("StreamKeyedMerge", out)
    }
}

impl PlanTreeNodeBinary for StreamKeyedMerge {
    fn left(&self) -> PlanRef {
        self.lhs_input.clone()
    }

    fn right(&self) -> PlanRef {
        self.rhs_input.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self {
            base: self.base.clone(),
            lhs_input: left,
            rhs_input: right,
            lhs_mapping: self.lhs_mapping.clone(),
            rhs_mapping: self.rhs_mapping.clone(),
        }
    }
}

impl_plan_tree_node_for_binary! { StreamKeyedMerge }

impl StreamNode for StreamKeyedMerge {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        todo!()
    }
}

impl ExprRewritable for StreamKeyedMerge {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) -> PlanRef {
        unimplemented!()
    }
}

impl ExprVisitable for StreamKeyedMerge {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        unimplemented!()
    }
}
