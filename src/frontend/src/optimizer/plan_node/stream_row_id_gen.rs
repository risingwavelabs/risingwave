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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use super::utils::{childless_record, Distill};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamRowIdGen {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    row_id_index: usize,
    custom_distribution: bool,
}

impl StreamRowIdGen {
    pub fn new(input: PlanRef, row_id_index: usize) -> Self {
        if input.append_only() {
            // remove exchange for append only source
            return Self::new_with_dist_helper(
                input,
                row_id_index,
                Some(Distribution::HashShard(vec![row_id_index])),
            );
        }

        Self::new_with_dist_helper(input, row_id_index, None)
    }

    /// Create a new `StreamRowIdGen` with a custom distribution.
    pub fn new_with_dist(input: PlanRef, row_id_index: usize, distribution: Distribution) -> Self {
        Self::new_with_dist_helper(input, row_id_index, Some(distribution))
    }

    fn new_with_dist_helper(
        input: PlanRef,
        row_id_index: usize,
        distribution: Option<Distribution>,
    ) -> StreamRowIdGen {
        let custom_distribution = distribution.is_some();
        let dist = distribution.unwrap_or(input.distribution().clone());
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.stream_key().map(|v| v.to_vec()),
            input.functional_dependency().clone(),
            dist,
            input.append_only(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
        );
        Self {
            base,
            input,
            row_id_index,
            custom_distribution,
        }
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
        if self.custom_distribution {
            return Self::new_with_dist(input, self.row_id_index, self.base.dist.clone());
        }

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
