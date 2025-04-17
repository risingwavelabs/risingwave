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

use risingwave_pb::stream_plan::ExpandNode;
use risingwave_pb::stream_plan::expand_node::Subset;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::impl_distill_by_unit;
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode, generic};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, MonotonicityMap};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamExpand {
    pub base: PlanBase<Stream>,
    core: generic::Expand<PlanRef>,
}

impl StreamExpand {
    pub fn new(core: generic::Expand<PlanRef>) -> Self {
        let input = core.input.clone();
        let input_len = input.schema().len();

        let dist = match input.distribution() {
            Distribution::Single => Distribution::Single,
            Distribution::SomeShard
            | Distribution::HashShard(_)
            | Distribution::UpstreamHashShard(_, _) => Distribution::SomeShard,
            Distribution::Broadcast => unreachable!(),
        };

        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            input.append_only(),
            input.emit_on_window_close(),
            input.watermark_columns().right_shift_clone(input_len),
            MonotonicityMap::new(),
        );
        StreamExpand { base, core }
    }

    pub fn column_subsets(&self) -> &[Vec<usize>] {
        &self.core.column_subsets
    }
}

impl PlanTreeNodeUnary for StreamExpand {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
    }
}

impl_plan_tree_node_for_unary! { StreamExpand }
impl_distill_by_unit!(StreamExpand, core, "StreamExpand");

impl StreamNode for StreamExpand {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::Expand(Box::new(ExpandNode {
            column_subsets: self
                .column_subsets()
                .iter()
                .map(|subset| subset_to_protobuf(subset))
                .collect(),
        }))
    }
}

fn subset_to_protobuf(subset: &[usize]) -> Subset {
    let column_indices = subset.iter().map(|key| *key as u32).collect();
    Subset { column_indices }
}

impl ExprRewritable for StreamExpand {}

impl ExprVisitable for StreamExpand {}
