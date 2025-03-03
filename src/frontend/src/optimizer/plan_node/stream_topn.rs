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

use pretty_xmlish::XmlNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::generic::{DistillUnit, TopNLimit};
use super::stream::prelude::*;
use super::utils::{Distill, plan_node_name};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode, generic};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, MonotonicityMap, Order, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamTopN` implements [`super::LogicalTopN`] to find the top N elements with a heap
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTopN {
    pub base: PlanBase<Stream>,
    core: generic::TopN<PlanRef>,
}

impl StreamTopN {
    pub fn new(core: generic::TopN<PlanRef>) -> Self {
        assert!(core.group_key.is_empty());
        assert!(core.limit_attr.limit() > 0);
        let input = &core.input;
        let dist = match input.distribution() {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };
        let watermark_columns = WatermarkColumns::new();

        let base = PlanBase::new_stream_with_core(
            &core,
            dist,
            false,
            false,
            watermark_columns,
            MonotonicityMap::new(),
        );
        StreamTopN { base, core }
    }

    pub fn limit_attr(&self) -> TopNLimit {
        self.core.limit_attr
    }

    pub fn offset(&self) -> u64 {
        self.core.offset
    }

    pub fn topn_order(&self) -> &Order {
        &self.core.order
    }
}

impl Distill for StreamTopN {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let name = plan_node_name!("StreamTopN",
            { "append_only", self.input().append_only() },
        );
        self.core.distill_with_name(name)
    }
}

impl PlanTreeNodeUnary for StreamTopN {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::new(core)
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
                self.core
                    .infer_internal_table_catalog(
                        input.schema(),
                        input.ctx(),
                        input.expect_stream_key(),
                        None,
                    )
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            order_by: self.topn_order().to_protobuf(),
        };
        if self.input().append_only() {
            PbNodeBody::AppendOnlyTopN(Box::new(topn_node))
        } else {
            PbNodeBody::TopN(Box::new(topn_node))
        }
    }
}

impl ExprRewritable for StreamTopN {}

impl ExprVisitable for StreamTopN {}
