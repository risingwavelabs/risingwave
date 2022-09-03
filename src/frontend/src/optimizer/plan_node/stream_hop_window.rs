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

use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::HopWindowNode;

use super::{LogicalHopWindow, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// [`StreamHopWindow`] represents a hop window table function.
#[derive(Debug, Clone)]
pub struct StreamHopWindow {
    pub base: PlanBase,
    logical: LogicalHopWindow,
}

impl StreamHopWindow {
    pub fn new(logical: LogicalHopWindow) -> Self {
        let ctx = logical.base.ctx.clone();
        let pk_indices = logical.base.logical_pk.to_vec();
        let input = logical.input();

        let i2o = logical.i2o_col_mapping();
        let dist = i2o.rewrite_provided_distribution(input.distribution());

        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            pk_indices,
            logical.functional_dependency().clone(),
            dist,
            logical.input().append_only(),
        );
        Self { base, logical }
    }
}

impl fmt::Display for StreamHopWindow {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.logical.fmt_with_name(f, "StreamHopWindow")
    }
}

impl PlanTreeNodeUnary for StreamHopWindow {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! {StreamHopWindow}

impl ToStreamProst for StreamHopWindow {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        ProstStreamNode::HopWindow(HopWindowNode {
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
