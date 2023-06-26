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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::utils::{childless_record, Distill};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamRowIdGen {
    pub base: PlanBase,
    input: PlanRef,
    row_id_index: usize,
}

impl StreamRowIdGen {
    pub fn new(input: PlanRef, row_id_index: usize) -> Self {
        let distribution = if input.append_only() {
            // remove exchange for append only source
            Distribution::HashShard(vec![row_id_index])
        } else {
            input.distribution().clone()
        };

        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            input.functional_dependency().clone(),
            distribution,
            input.append_only(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
        );
        Self {
            base,
            input,
            row_id_index,
        }
    }
}

impl fmt::Display for StreamRowIdGen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "StreamRowIdGen {{ row_id_index: {} }}",
            self.row_id_index
        )
    }
}
impl Distill for StreamRowIdGen {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = vec![("row_id_index", Pretty::debug(&self.row_id_index))];
        childless_record("StreamRowIdGen", fields)
    }
}

impl PlanTreeNodeUnary for StreamRowIdGen {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.row_id_index)
    }
}

impl_plan_tree_node_for_unary! {StreamRowIdGen}

impl StreamNode for StreamRowIdGen {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        PbNodeBody::RowIdGen(RowIdGenNode {
            row_id_index: self.row_id_index as _,
        })
    }
}

impl ExprRewritable for StreamRowIdGen {}
