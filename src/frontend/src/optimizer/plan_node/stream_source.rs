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

use risingwave_pb::stream_plan::source_node::Info;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::SourceNode;

use super::{LogicalSource, PlanBase, StreamNode};
use crate::catalog::source_catalog::SourceCatalogInfo;
use crate::optimizer::property::Distribution;
use crate::stream_fragmenter::BuildFragmentGraphState;

/// [`StreamSource`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone)]
pub struct StreamSource {
    pub base: PlanBase,
    logical: LogicalSource,
}

impl StreamSource {
    pub fn new(logical: LogicalSource) -> Self {
        let base = PlanBase::new_stream(
            logical.ctx(),
            logical.schema().clone(),
            logical.logical_pk().to_vec(),
            logical.functional_dependency().clone(),
            Distribution::SomeShard,
            logical.source_catalog().append_only,
        );
        Self { base, logical }
    }

    pub fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }
}

impl_plan_tree_node_for_leaf! { StreamSource }

impl fmt::Display for StreamSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamSource");
        builder
            .field("source", &self.logical.source_catalog().name)
            .field(
                "columns",
                &format_args!("[{}]", &self.column_names().join(", ")),
            )
            .finish()
    }
}

impl StreamNode for StreamSource {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        let source_catalog = self.logical.source_catalog();
        ProstStreamNode::Source(SourceNode {
            source_id: source_catalog.id,
            column_ids: source_catalog
                .columns
                .iter()
                .map(|c| c.column_id().into())
                .collect(),
            source_type: source_catalog.source_type as i32,
            state_table: Some(
                self.logical
                    .infer_internal_table_catalog()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            info: Some(match &source_catalog.info {
                SourceCatalogInfo::StreamSource(info) => Info::StreamSource(info.to_owned()),
                SourceCatalogInfo::TableSource(info) => Info::TableSource(info.to_owned()),
            }),
        })
    }
}
