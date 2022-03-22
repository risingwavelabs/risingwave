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
//
use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::source_node::SourceType;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;
use risingwave_pb::stream_plan::SourceNode;

use super::{LogicalScan, PlanBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithSchema};

/// [`StreamSource`] represents a table/connector source at the very beginning of the graph.
#[derive(Debug, Clone)]
pub struct StreamSource {
    pub base: PlanBase,
    logical: LogicalScan,
    source_type: SourceType,
}

impl StreamSource {
    pub fn new(logical: LogicalScan, source_type: SourceType) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = PlanBase::new_stream(ctx, logical.schema().clone(), Distribution::any().clone());

        Self {
            logical,
            base,
            source_type,
        }
    }
}

impl WithSchema for StreamSource {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl_plan_tree_node_for_leaf! { StreamSource }

impl fmt::Display for StreamSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamSource {{ source: {}, type: {:?}, columns: [{}] }}",
            self.logical.table_name(),
            self.source_type,
            self.logical.column_names().join(", ")
        )
    }
}

impl ToStreamProst for StreamSource {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::SourceNode(SourceNode {
            // TODO: Refactor this id
            table_ref_id: TableRefId {
                table_id: self.logical.table_id() as i32,
                ..Default::default()
            }
            .into(),
            column_ids: self.logical.columns().iter().map(|c| c.get_id()).collect(),
            source_type: self.source_type as i32,
        })
    }
}
