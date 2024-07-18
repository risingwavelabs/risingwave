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

use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::HopWindowNode;

use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::Distill;
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanTreeNodeBinary, Stream, StreamHopWindow,
    StreamLocalApproxPercentile, StreamNode,
};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::PlanRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamKeyedMerge {
    pub lhs_input: PlanRef,
    pub rhs_input: PlanRef,
    pub base: PlanBase<Stream>,
}

impl StreamKeyedMerge {
    pub fn new(lhs_input: PlanRef, rhs_input: PlanRef) -> Self {
        Self {
            lhs_input,
            rhs_input,
            base: todo!(),
        }
    }
}

impl Distill for StreamKeyedMerge {
    fn distill<'a>(&self) -> XmlNode<'a> {
        todo!()
    }
}

impl PlanTreeNodeBinary for StreamKeyedMerge {
    fn left(&self) -> PlanRef {
        todo!()
    }

    fn right(&self) -> PlanRef {
        todo!()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        todo!()
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
        todo!()
    }

    fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) -> PlanRef {
        todo!()
    }
}

impl ExprVisitable for StreamKeyedMerge {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        todo!()
    }
}
