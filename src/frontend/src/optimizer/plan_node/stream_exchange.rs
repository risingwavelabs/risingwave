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
use risingwave_pb::stream_plan::{DispatchStrategy, DispatcherType, ExchangeNode};

use super::{PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::property::{Distribution, DistributionDisplay};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamExchange` imposes a particular distribution on its input
/// without changing its content.
#[derive(Debug, Clone)]
pub struct StreamExchange {
    pub base: PlanBase,
    input: PlanRef,
}

impl StreamExchange {
    pub fn new(input: PlanRef, dist: Distribution) -> Self {
        let ctx = input.ctx();
        let pk_indices = input.logical_pk().to_vec();
        // Dispatch executor won't change the append-only behavior of the stream.
        let base = PlanBase::new_stream(
            ctx,
            input.schema().clone(),
            pk_indices,
            input.functional_dependency().clone(),
            dist,
            input.append_only(),
        );
        StreamExchange { base, input }
    }
}

impl fmt::Display for StreamExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamExchange");
        builder
            .field(
                "dist",
                &format_args!(
                    "{:?}",
                    DistributionDisplay {
                        distribution: &self.base.dist,
                        input_schema: self.input.schema()
                    }
                ),
            )
            .finish()
    }
}

impl PlanTreeNodeUnary for StreamExchange {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.distribution().clone())
    }
}
impl_plan_tree_node_for_unary! {StreamExchange}

impl StreamNode for StreamExchange {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
        NodeBody::Exchange(ExchangeNode {
            strategy: Some(DispatchStrategy {
                r#type: match &self.base.dist {
                    Distribution::HashShard(_) => DispatcherType::Hash,
                    Distribution::Single => DispatcherType::Simple,
                    Distribution::Broadcast => DispatcherType::Broadcast,
                    _ => panic!("Do not allow Any or AnyShard in serialization process"),
                } as i32,
                column_indices: match &self.base.dist {
                    Distribution::HashShard(keys) => keys.iter().map(|num| *num as u32).collect(),
                    _ => vec![],
                },
            }),
        })
    }
}
