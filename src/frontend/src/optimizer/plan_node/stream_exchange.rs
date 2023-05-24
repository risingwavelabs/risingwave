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

use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{DispatchStrategy, DispatcherType, ExchangeNode};

use super::stream::StreamPlanRef;
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::property::{Distribution, DistributionDisplay};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamExchange` imposes a particular distribution on its input
/// without changing its content.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamExchange {
    pub base: PlanBase,
    input: PlanRef,
    no_shuffle: bool,
}

impl StreamExchange {
    pub fn new(input: PlanRef, dist: Distribution) -> Self {
        // Dispatch executor won't change the append-only behavior of the stream.
        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.logical_pk().to_vec(),
            input.functional_dependency().clone(),
            dist,
            input.append_only(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
        );
        StreamExchange {
            base,
            input,
            no_shuffle: false,
        }
    }

    pub fn new_no_shuffle(input: PlanRef) -> Self {
        let ctx = input.ctx();
        let pk_indices = input.logical_pk().to_vec();
        // Dispatch executor won't change the append-only behavior of the stream.
        let base = PlanBase::new_stream(
            ctx,
            input.schema().clone(),
            pk_indices,
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.append_only(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
        );
        StreamExchange {
            base,
            input,
            no_shuffle: true,
        }
    }

    pub fn no_shuffle(&self) -> bool {
        self.no_shuffle
    }
}

impl fmt::Display for StreamExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = if self.no_shuffle {
            f.debug_struct("StreamNoShuffleExchange")
        } else {
            f.debug_struct("StreamExchange")
        };

        builder
            .field(
                "dist",
                &DistributionDisplay {
                    distribution: &self.base.dist,
                    input_schema: self.input.schema(),
                },
            )
            .finish()
    }
}

impl PlanTreeNodeUnary for StreamExchange {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        if self.no_shuffle {
            Self::new_no_shuffle(input)
        } else {
            Self::new(input, self.distribution().clone())
        }
    }
}
impl_plan_tree_node_for_unary! {StreamExchange}

impl StreamNode for StreamExchange {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
        NodeBody::Exchange(ExchangeNode {
            strategy: if self.no_shuffle {
                Some(DispatchStrategy {
                    r#type: DispatcherType::NoShuffle as i32,
                    dist_key_indices: vec![],
                    output_indices: (0..self.schema().len() as u32).collect(),
                })
            } else {
                Some(DispatchStrategy {
                    r#type: match &self.base.dist {
                        Distribution::HashShard(_) => DispatcherType::Hash,
                        Distribution::Single => DispatcherType::Simple,
                        Distribution::Broadcast => DispatcherType::Broadcast,
                        _ => panic!("Do not allow Any or AnyShard in serialization process"),
                    } as i32,
                    dist_key_indices: match &self.base.dist {
                        Distribution::HashShard(keys) => {
                            keys.iter().map(|num| *num as u32).collect()
                        }
                        _ => vec![],
                    },
                    output_indices: (0..self.schema().len() as u32).collect(),
                })
            },
        })
    }
}

impl ExprRewritable for StreamExchange {}
