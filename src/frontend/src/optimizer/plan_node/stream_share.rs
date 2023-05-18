// Copyright 2023 RisingWave Labs
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

use std::fmt;

use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::PbStreamNode;

use super::{generic, ExprRewritable, PlanRef, PlanTreeNodeUnary, StreamExchange, StreamNode};
use crate::optimizer::plan_node::{LogicalShare, PlanBase, PlanTreeNode};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamShare` will be translated into an `ExchangeNode` based on its distribution finally.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamShare {
    pub base: PlanBase,
    logical: generic::Share<PlanRef>,
}

impl StreamShare {
    pub fn new(logical: generic::Share<PlanRef>) -> Self {
        let input = logical.input.borrow().0.clone();
        let dist = input.distribution().clone();
        // Filter executor won't change the append-only behavior of the stream.
        let base = PlanBase::new_stream_with_logical(
            &logical,
            dist,
            input.append_only(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
        );
        StreamShare { base, logical }
    }
}

impl fmt::Display for StreamShare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        LogicalShare::fmt_with_name(&self.base, f, "StreamShare")
    }
}

impl PlanTreeNodeUnary for StreamShare {
    fn input(&self) -> PlanRef {
        self.logical.input.borrow().clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let logical = self.logical.clone();
        logical.replace_input(input);
        Self::new(logical)
    }
}

impl StreamShare {
    pub fn replace_input(&self, plan: PlanRef) {
        self.logical.replace_input(plan);
    }
}

impl_plan_tree_node_for_unary! { StreamShare }

impl StreamNode for StreamShare {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamShare {
    pub fn adhoc_to_stream_prost(&self, state: &mut BuildFragmentGraphState) -> PbStreamNode {
        let operator_id = self.base.id.0 as u32;

        match state.get_share_stream_node(operator_id) {
            None => {
                let node_body =
                    StreamExchange::new_no_shuffle(self.input()).to_stream_prost_body(state);

                let input = self
                    .inputs()
                    .into_iter()
                    .map(|plan| plan.to_stream_prost(state))
                    .collect();

                let stream_node = PbStreamNode {
                    input,
                    identity: format!("{}", self),
                    node_body: Some(node_body),
                    operator_id: self.id().0 as _,
                    stream_key: self.logical_pk().iter().map(|x| *x as u32).collect(),
                    fields: self.schema().to_prost(),
                    append_only: self.append_only(),
                };

                state.add_share_stream_node(operator_id, stream_node.clone());
                stream_node
            }

            Some(stream_node) => stream_node.clone(),
        }
    }
}

impl ExprRewritable for StreamShare {}
