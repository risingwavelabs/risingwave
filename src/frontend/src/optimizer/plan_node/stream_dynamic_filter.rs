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

use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::DynamicFilterNode;

use crate::expr::Expr;
use crate::optimizer::plan_node::{PlanBase, PlanTreeNodeBinary, ToStreamProst};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

#[derive(Clone, Debug)]
pub struct StreamDynamicFilter {
    pub base: PlanBase,
    /// The predicate (formed with exactly one of < , <=, >, >=)
    predicate: Condition,
    // dist_key_l: Distribution,
    left_index: usize,
    left: PlanRef,
    right: PlanRef,
}

impl StreamDynamicFilter {
    pub fn new(left_index: usize, predicate: Condition, left: PlanRef, right: PlanRef) -> Self {
        // TODO: derive from input
        let base = PlanBase::new_stream(
            left.ctx(),
            left.schema().clone(),
            left.pk_indices().to_vec(),
            left.distribution().clone(),
            false, /* we can have a new abstraction for append only and monotonically increasing
                    * in the future */
        );
        Self {
            base,
            predicate,
            left_index,
            left,
            right,
        }
    }
}

impl fmt::Display for StreamDynamicFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamDynamicFilter {{ predicate: {} }}", self.predicate)
    }
}

impl PlanTreeNodeBinary for StreamDynamicFilter {
    fn left(&self) -> PlanRef {
        self.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(self.left_index, self.predicate.clone(), left, right)
    }
}

impl_plan_tree_node_for_binary! { StreamDynamicFilter }

impl ToStreamProst for StreamDynamicFilter {
    fn to_stream_prost_body(&self) -> NodeBody {
        NodeBody::DynamicFilter(DynamicFilterNode {
            left_key: self.left_index as u32,
            condition: self
                .predicate
                .as_expr_unless_true()
                .map(|x| x.to_expr_proto()),
        })
    }
}
