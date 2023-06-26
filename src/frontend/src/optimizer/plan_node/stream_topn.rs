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

use fixedbitset::FixedBitSet;
use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{DistillUnit, TopNLimit};
use super::utils::{plan_node_name, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::optimizer::property::{Distribution, Order};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamTopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTopN {
    pub base: PlanBase,
    logical: generic::TopN<PlanRef>,
}

impl StreamTopN {
    pub fn new(logical: generic::TopN<PlanRef>) -> Self {
        assert!(logical.group_key.is_empty());
        assert!(logical.limit_attr.limit() > 0);
        let input = &logical.input;
        let dist = match input.distribution() {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };
        let watermark_columns = FixedBitSet::with_capacity(input.schema().len());

        let base =
            PlanBase::new_stream_with_logical(&logical, dist, false, false, watermark_columns);
        StreamTopN { base, logical }
    }

    pub fn limit_attr(&self) -> TopNLimit {
        self.logical.limit_attr
    }

    pub fn offset(&self) -> u64 {
        self.logical.offset
    }

    pub fn topn_order(&self) -> &Order {
        &self.logical.order
    }
}

impl fmt::Display for StreamTopN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = plan_node_name!("StreamTopN",
            { "append_only", self.input().append_only() },
        );
        self.logical
            .fmt_with_name_and_force(f, &name, false)
            .finish()
    }
}
impl Distill for StreamTopN {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let name = plan_node_name!("StreamTopN",
            { "append_only", self.input().append_only() },
        );
        self.logical.distill_with_name(name)
    }
}

impl PlanTreeNodeUnary for StreamTopN {
    fn input(&self) -> PlanRef {
        self.logical.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut logical = self.logical.clone();
        logical.input = input;
        Self::new(logical)
    }
}

impl_plan_tree_node_for_unary! { StreamTopN }

impl StreamNode for StreamTopN {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        let input = self.input();
        let topn_node = TopNNode {
            limit: self.limit_attr().limit(),
            offset: self.offset(),
            with_ties: self.limit_attr().with_ties(),
            table: Some(
                self.logical
                    .infer_internal_table_catalog(
                        input.schema(),
                        input.ctx(),
                        input.logical_pk(),
                        None,
                    )
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            order_by: self.topn_order().to_protobuf(),
        };
        if self.input().append_only() {
            PbNodeBody::AppendOnlyTopN(topn_node)
        } else {
            PbNodeBody::TopN(topn_node)
        }
    }
}
impl ExprRewritable for StreamTopN {}
