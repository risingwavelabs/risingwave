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
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::ChangeBufferNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::StreamPlanNodeMetadata;
use super::utils::{Distill, TableCatalogBuilder, childless_record};
use super::{
    ExprRewritable, PlanBase, PlanTreeNodeUnary, Stream, StreamNode, StreamPlanRef as PlanRef,
};
use crate::catalog::TableCatalog;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::PhysicalPlanRef;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamChangeBuffer {
    pub base: PlanBase<Stream>,
    input: PlanRef,
}

impl StreamChangeBuffer {
    pub fn new(input: PlanRef) -> Self {
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

        Self { base, input }
    }

    fn build_state_table(&self, state: &mut BuildFragmentGraphState) -> TableCatalog {
        let mut catalog_builder = TableCatalogBuilder::default();
        let input = self.input();
        let input_schema = input.schema();
        let stream_key = input.expect_stream_key();

        debug_assert!(
            !stream_key.is_empty(),
            "change buffer requires a non-empty input stream key"
        );

        input_schema.fields().iter().for_each(|field| {
            catalog_builder.add_column(field);
        });

        // The runtime relies on `iter_uncommitted_log` to already follow the input stream key
        // order, so the internal table primary key must match it exactly.
        stream_key.iter().for_each(|&idx| {
            catalog_builder.add_order_column(idx, OrderType::ascending());
        });

        catalog_builder.set_value_indices((0..input_schema.len()).collect());

        catalog_builder
            .build(
                input.distribution().dist_column_indices().to_vec(),
                stream_key.len(),
            )
            .with_id(state.gen_table_id_wrapped())
    }
}

impl Distill for StreamChangeBuffer {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("StreamChangeBuffer", vec![])
    }
}

impl PlanTreeNodeUnary<Stream> for StreamChangeBuffer {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input)
    }
}

impl_plan_tree_node_for_unary! { Stream, StreamChangeBuffer }

impl StreamNode for StreamChangeBuffer {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::ChangeBuffer(ChangeBufferNode {
            state_table: Some(self.build_state_table(state).to_internal_table_prost()),
        })
    }
}

impl ExprRewritable<Stream> for StreamChangeBuffer {}

impl ExprVisitable for StreamChangeBuffer {}
