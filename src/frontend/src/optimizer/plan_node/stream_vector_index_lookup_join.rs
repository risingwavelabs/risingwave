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
use risingwave_common::bail;
use risingwave_pb::plan_common::PbVectorIndexReaderDesc;
use risingwave_pb::stream_plan::PbVectorIndexLookupJoinNode;

use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{PhysicalPlanRef, VectorIndexLookupJoin};
use crate::optimizer::plan_node::stream::StreamPlanNodeMetadata;
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanTreeNodeUnary, Stream, StreamNode, StreamPlanRef,
};
use crate::optimizer::property::StreamKind;
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamVectorIndexLookupJoin {
    pub base: PlanBase<Stream>,
    pub core: VectorIndexLookupJoin<StreamPlanRef>,
}

impl StreamVectorIndexLookupJoin {
    pub fn new(core: VectorIndexLookupJoin<StreamPlanRef>) -> crate::error::Result<Self> {
        if core.input.stream_kind() != StreamKind::AppendOnly {
            bail!("StreamVectorIndexLookupJoin only support append only input")
        }
        Ok(Self::with_core(core))
    }

    fn with_core(core: VectorIndexLookupJoin<StreamPlanRef>) -> Self {
        assert_eq!(core.input.stream_kind(), StreamKind::AppendOnly);
        let base = PlanBase::new_stream_with_core(
            &core,
            core.input.distribution().clone(),
            core.input.stream_kind(),
            core.input.emit_on_window_close(),
            core.input.watermark_columns().clone(),
            core.input.columns_monotonicity().clone(),
        );
        Self { base, core }
    }
}

impl Distill for StreamVectorIndexLookupJoin {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = self.core.distill();
        childless_record("StreamVectorIndexLookupJoin", fields)
    }
}

impl PlanTreeNodeUnary<Stream> for StreamVectorIndexLookupJoin {
    fn input(&self) -> crate::PlanRef<Stream> {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: crate::PlanRef<Stream>) -> Self {
        let mut core = self.core.clone();
        core.input = input;
        Self::with_core(core)
    }
}

impl_plan_tree_node_for_unary!(Stream, StreamVectorIndexLookupJoin);

impl StreamNode for StreamVectorIndexLookupJoin {
    fn to_stream_prost_body(
        &self,
        _state: &mut BuildFragmentGraphState,
    ) -> risingwave_pb::stream_plan::stream_node::NodeBody {
        risingwave_pb::stream_plan::stream_node::NodeBody::VectorIndexLookupJoin(
            PbVectorIndexLookupJoinNode {
                reader_desc: Some(PbVectorIndexReaderDesc {
                    table_id: self.core.index_table_id,
                    info_column_desc: self
                        .core
                        .info_column_desc
                        .iter()
                        .map(|col| col.to_protobuf())
                        .collect(),
                    top_n: self.core.top_n as _,
                    distance_type: self.core.distance_type as _,
                    hnsw_ef_search: self.core.hnsw_ef_search.unwrap_or(0) as _,
                    info_output_indices: self
                        .core
                        .info_output_indices
                        .iter()
                        .map(|&idx| idx as _)
                        .collect(),
                    include_distance: self.core.include_distance,
                }),
                vector_column_idx: self.core.vector_column_idx as _,
            }
            .into(),
        )
    }
}

impl ExprVisitable for StreamVectorIndexLookupJoin {}

impl ExprRewritable<Stream> for StreamVectorIndexLookupJoin {}
