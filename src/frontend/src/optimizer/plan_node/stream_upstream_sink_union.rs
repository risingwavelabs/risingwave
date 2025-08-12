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
use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::UpstreamSinkUnionNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use crate::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{ExprRewritable, PlanBase, Stream, StreamNode};
use crate::optimizer::property::{
    Distribution, FunctionalDependencySet, MonotonicityMap, WatermarkColumns,
};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamUpstreamSinkUnion {
    pub base: PlanBase<Stream>,
}

impl StreamUpstreamSinkUnion {
    pub fn new(ctx: OptimizerContextRef, schema: Schema, append_only: bool) -> Self {
        let column_num = schema.fields().len();
        // TODO(zyx): Maybe some fields are incorrect.
        let base = PlanBase::new_stream(
            ctx,
            schema,
            Some(vec![]), // stream_key
            FunctionalDependencySet::new(column_num),
            Distribution::SomeShard,
            if append_only {
                StreamKind::AppendOnly
            } else {
                StreamKind::Retract
            },
            false, // emit_on_window_close
            WatermarkColumns::new(),
            MonotonicityMap::new(),
        );
        Self { base }
    }
}

impl Distill for StreamUpstreamSinkUnion {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("StreamUpstreamSinkUnion", vec![])
    }
}

impl_plan_tree_node_for_leaf! { Stream, StreamUpstreamSinkUnion }

impl StreamNode for StreamUpstreamSinkUnion {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::UpstreamSinkUnion(Box::new(UpstreamSinkUnionNode {
            init_upstreams: vec![],
        }))
    }
}

impl ExprRewritable<Stream> for StreamUpstreamSinkUnion {}

impl ExprVisitable for StreamUpstreamSinkUnion {}
