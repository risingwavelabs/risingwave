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
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

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
        assert!(logical.limit > 0);
        let base = PlanBase::new_logical_with_core(&logical);
        let ctx = base.ctx;
        let input = logical.input.clone();
        let schema = base.schema;
        let dist = match logical.input.distribution() {
            Distribution::Single => Distribution::Single,
            _ => panic!(),
        };
        let watermark_columns = FixedBitSet::with_capacity(schema.len());

        let base = PlanBase::new_stream(
            ctx,
            schema,
            input.logical_pk().to_vec(),
            base.functional_dependency,
            dist,
            false,
            watermark_columns,
        );
        StreamTopN { base, logical }
    }

    pub fn limit(&self) -> u64 {
        self.logical.limit
    }

    pub fn offset(&self) -> u64 {
        self.logical.offset
    }

    pub fn with_ties(&self) -> bool {
        self.logical.with_ties
    }

    pub fn topn_order(&self) -> &Order {
        &self.logical.order
    }
}

impl fmt::Display for StreamTopN {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.input().append_only() {
            self.logical.fmt_with_name(f, "StreamAppendOnlyTopN")
        } else {
            self.logical.fmt_with_name(f, "StreamTopN")
        }
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
        let topn_node = TopNNode {
            limit: self.limit(),
            offset: self.offset(),
            with_ties: self.with_ties(),
            table: Some(
                self.logical
                    .infer_internal_table_catalog(&self.base, None)
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
