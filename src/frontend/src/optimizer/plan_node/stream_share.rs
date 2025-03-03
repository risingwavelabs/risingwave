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

use std::cell::RefCell;

use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::PbStreamNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::Distill;
use super::{ExprRewritable, PlanRef, PlanTreeNodeUnary, StreamExchange, StreamNode, generic};
use crate::Explain;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{LogicalShare, PlanBase, PlanTreeNode};
use crate::scheduler::SchedulerResult;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamShare` will be translated into an `ExchangeNode` based on its distribution finally.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamShare {
    pub base: PlanBase<Stream>,
    core: generic::Share<PlanRef>,
}

impl StreamShare {
    pub fn new(core: generic::Share<PlanRef>) -> Self {
        let base = {
            let input = core.input.borrow();
            let dist = input.distribution().clone();
            // Filter executor won't change the append-only behavior of the stream.
            PlanBase::new_stream_with_core(
                &core,
                dist,
                input.append_only(),
                input.emit_on_window_close(),
                input.watermark_columns().clone(),
                input.columns_monotonicity().clone(),
            )
        };

        StreamShare { base, core }
    }

    pub fn new_from_input(input: PlanRef) -> Self {
        let core = generic::Share {
            input: RefCell::new(input),
        };
        Self::new(core)
    }
}

impl Distill for StreamShare {
    fn distill<'a>(&self) -> XmlNode<'a> {
        LogicalShare::pretty_fields(&self.base, "StreamShare")
    }
}

impl PlanTreeNodeUnary for StreamShare {
    fn input(&self) -> PlanRef {
        self.core.input.borrow().clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let core = self.core.clone();
        core.replace_input(input);
        Self::new(core)
    }
}

impl StreamShare {
    pub fn replace_input(&self, plan: PlanRef) {
        self.core.replace_input(plan);
    }
}

impl_plan_tree_node_for_unary! { StreamShare }

impl StreamNode for StreamShare {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        unreachable!(
            "stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead."
        )
    }
}

impl StreamShare {
    pub fn adhoc_to_stream_prost(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> SchedulerResult<PbStreamNode> {
        let operator_id = self.base.id().0 as u32;

        match state.get_share_stream_node(operator_id) {
            None => {
                let node_body =
                    StreamExchange::new_no_shuffle(self.input()).to_stream_prost_body(state);

                let input = self
                    .inputs()
                    .into_iter()
                    .map(|plan| plan.to_stream_prost(state))
                    .try_collect()?;

                let stream_node = PbStreamNode {
                    input,
                    identity: self.distill_to_string(),
                    node_body: Some(node_body),
                    operator_id: self.id().0 as _,
                    stream_key: self
                        .stream_key()
                        .unwrap_or_else(|| panic!("should always have a stream key in the stream plan but not, sub plan: {}",
                       PlanRef::from(self.clone()).explain_to_string()))
                        .iter()
                        .map(|x| *x as u32)
                        .collect(),
                    fields: self.schema().to_prost(),
                    append_only: self.append_only(),
                };

                state.add_share_stream_node(operator_id, stream_node.clone());
                Ok(stream_node)
            }

            Some(stream_node) => Ok(stream_node.clone()),
        }
    }
}

impl ExprRewritable for StreamShare {}

impl ExprVisitable for StreamShare {}
