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

use super::{LogicalTopN, PlanBase, PlanRef, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamTopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone)]
pub struct StreamTopN {
    pub base: PlanBase,
    logical: LogicalTopN,
}

impl StreamTopN {
    pub fn new(logical: LogicalTopN) -> Self {
        let ctx = logical.base.ctx.clone();
        let dist = match logical.input().distribution() {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };

        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.input().logical_pk().to_vec(),
            logical.functional_dependency().clone(),
            dist,
            false,
        );
        StreamTopN { base, logical }
    }
}

impl fmt::Display for StreamTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.input().append_only() {
            self.logical.fmt_with_name(f, "StreamAppendOnlyTopN")
        } else {
            self.logical.fmt_with_name(f, "StreamTopN")
        }
    }
}

impl PlanTreeNodeUnary for StreamTopN {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input))
    }
}

impl_plan_tree_node_for_unary! { StreamTopN }

impl ToStreamProst for StreamTopN {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;
        let topn_node = TopNNode {
            limit: self.logical.limit() as u64,
            offset: self.logical.offset() as u64,
            table: Some(
                self.logical
                    .infer_internal_table_catalog(None)
                    .with_id(state.gen_table_id_wrapped())
                    .to_state_table_prost(),
            ),
        };
        if self.input().append_only() {
            ProstStreamNode::AppendOnlyTopN(topn_node)
        } else {
            ProstStreamNode::TopN(topn_node)
        }
    }
}
