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

use super::{LogicalTopN, PlanBase, PlanTreeNodeUnary, ToStreamProst};
use crate::optimizer::property::{Distribution, OrderDisplay};
use crate::PlanRef;

#[derive(Debug, Clone)]
pub struct StreamGroupTopN {
    pub base: PlanBase,
    logical: LogicalTopN,
    group_key: Vec<usize>,
}

impl StreamGroupTopN {
    pub fn new(logical: LogicalTopN, group_key: Vec<usize>) -> Self {
        let input = logical.input();
        let dist = match input.distribution() {
            Distribution::HashShard(_) => Distribution::HashShard(group_key.clone()),
            Distribution::UpstreamHashShard(_) => {
                Distribution::UpstreamHashShard(group_key.clone())
            }
            _ => input.distribution().clone(),
        };
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            input.functional_dependency().clone(),
            dist,
            false,
        );
        StreamGroupTopN {
            base,
            logical,
            group_key,
        }
    }
}

impl ToStreamProst for StreamGroupTopN {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        use risingwave_pb::stream_plan::*;
        let group_key = self.group_key.iter().map(|idx| *idx as u32).collect();

        if self.logical.limit() == 0 {
            panic!("topN's limit shouldn't be 0.");
        }
        let group_topn_node = GroupTopNNode {
            limit: self.logical.limit() as u64,
            offset: self.logical.offset() as u64,
            group_key,
            table: Some(
                self.logical
                    .infer_internal_table_catalog(Some(&self.group_key))
                    .to_internal_table_prost(),
            ),
        };

        ProstStreamNode::GroupTopN(group_topn_node)
    }
}

impl fmt::Display for StreamGroupTopN {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut builder = f.debug_struct("StreamGroupTopN");
        let input = self.input();
        let input_schema = input.schema();
        builder.field(
            "order",
            &format!(
                "{}",
                OrderDisplay {
                    order: self.logical.topn_order(),
                    input_schema
                }
            ),
        );
        builder
            .field("limit", &format_args!("{}", self.logical.limit()))
            .field("offset", &format_args!("{}", self.logical.offset()))
            .field("group_key", &format_args!("{:?}", self.group_key))
            .finish()
    }
}

impl_plan_tree_node_for_unary! { StreamGroupTopN }

impl PlanTreeNodeUnary for StreamGroupTopN {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.logical.clone_with_input(input), self.group_key.clone())
    }
}
