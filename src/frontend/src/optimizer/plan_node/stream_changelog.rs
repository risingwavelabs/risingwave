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

use risingwave_pb::stream_plan::ChangeLogNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::expr_visitable::ExprVisitable;
use super::stream::StreamPlanRef;
use super::stream::prelude::PhysicalPlanRef;
use super::utils::impl_distill_by_unit;
use super::{ExprRewritable, PlanBase, PlanTreeNodeUnary, Stream, StreamNode, generic};
use crate::PlanRef;
use crate::optimizer::property::MonotonicityMap;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamChangeLog {
    pub base: PlanBase<Stream>,
    core: generic::ChangeLog<PlanRef>,
}

impl StreamChangeLog {
    pub fn new(core: generic::ChangeLog<PlanRef>) -> Self {
        let input = core.input.clone();
        let dist = input.distribution().clone();
        // Filter executor won't change the append-only behavior of the stream.
        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            // The changelog will convert all delete/update to insert, so it must be true here.
            true,
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            MonotonicityMap::new(), // TODO: derive monotonicity
        );
        StreamChangeLog { base, core }
    }
}

impl PlanTreeNodeUnary for StreamChangeLog {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { StreamChangeLog }
impl_distill_by_unit!(StreamChangeLog, core, "StreamChangeLog");

impl StreamNode for StreamChangeLog {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::Changelog(Box::new(ChangeLogNode {
            need_op: self.core.need_op,
        }))
    }
}

impl ExprRewritable for StreamChangeLog {}

impl ExprVisitable for StreamChangeLog {}
