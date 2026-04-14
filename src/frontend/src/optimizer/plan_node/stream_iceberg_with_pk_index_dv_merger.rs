// Copyright 2026 RisingWave Labs
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
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_pb::stream_plan::IcebergWithPkIndexDvMergerNode;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use super::stream::prelude::*;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanTreeNodeUnary, Stream, StreamNode, StreamPlanRef as PlanRef,
};
use crate::optimizer::property::RequiredDist;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamIcebergWithPkIndexDvMerger` is the stateless singleton executor for the Iceberg
/// with pk index sink. It merges delete-position messages from the upstream `WriterExecutor`
/// with historical deletion vectors and writes merged DV files.
///
/// This node enforces `Distribution::Single` (singleton) — an `Exchange` node is automatically
/// inserted if the input is not already single-distributed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamIcebergWithPkIndexDvMerger {
    pub base: PlanBase<Stream>,
    pub input: PlanRef,
    pub sink_desc: SinkDesc,
}

impl StreamIcebergWithPkIndexDvMerger {
    pub fn new(input: PlanRef, sink_desc: SinkDesc) -> Self {
        // Enforce singleton distribution — inserts Exchange if needed.
        // `streaming_enforce_if_not_satisfies` is infallible.
        let input = RequiredDist::single()
            .streaming_enforce_if_not_satisfies(input)
            .expect("single distribution enforcement is infallible");

        let base = PlanBase::new_stream(
            input.ctx(),
            input.schema().clone(),
            input.stream_key().map(|keys| keys.to_vec()),
            input.functional_dependency().clone(),
            input.distribution().clone(),
            input.stream_kind(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );
        Self {
            base,
            input,
            sink_desc,
        }
    }
}

impl Distill for StreamIcebergWithPkIndexDvMerger {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("StreamIcebergWithPkIndexDvMerger", vec![])
    }
}

impl PlanTreeNodeUnary<Stream> for StreamIcebergWithPkIndexDvMerger {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.sink_desc.clone())
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamIcebergWithPkIndexDvMerger }

impl StreamNode for StreamIcebergWithPkIndexDvMerger {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> NodeBody {
        NodeBody::IcebergWithPkIndexDvMerger(Box::new(IcebergWithPkIndexDvMergerNode {
            sink_desc: Some(self.sink_desc.to_proto()),
        }))
    }
}

impl ExprRewritable<Stream> for StreamIcebergWithPkIndexDvMerger {}

impl ExprVisitable for StreamIcebergWithPkIndexDvMerger {}
