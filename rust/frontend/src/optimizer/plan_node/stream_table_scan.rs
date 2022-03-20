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
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;
use risingwave_pb::stream_plan::StreamNode as ProstStreamPlan;

use super::{LogicalScan, PlanBase, ToStreamProst};
use crate::catalog::ColumnId;
use crate::optimizer::property::{Distribution, WithSchema};

/// `StreamTableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to MView
/// creation request.
#[derive(Debug, Clone)]
pub struct StreamTableScan {
    pub base: PlanBase,
    logical: LogicalScan,
}

impl StreamTableScan {
    pub fn new(logical: LogicalScan) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = PlanBase::new_stream(
            ctx.clone(),
            logical.schema().clone(),
            Distribution::any().clone(),
        );
        Self { logical, base }
    }

    pub fn table_id(&self) -> u32 {
        self.logical.table_id()
    }

    pub fn table_name(&self) -> &str {
        self.logical.table_name()
    }

    pub fn columns(&self) -> &[ColumnId] {
        self.logical.columns()
    }
}

impl WithSchema for StreamTableScan {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl_plan_tree_node_for_leaf! { StreamTableScan }

impl fmt::Display for StreamTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamTableScan {{ table: {}, columns: [{}] }}",
            self.logical.table_name(),
            self.logical.column_names().join(", ")
        )
    }
}

impl ToStreamProst for StreamTableScan {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamTableScan {
    pub fn adhoc_to_stream_prost(&self) -> ProstStreamPlan {
        ProstStreamPlan {
            input: vec![
                ProstStreamPlan {
                    node: Some(ProstStreamNode::MergeNode(Default::default())),
                    ..Default::default()
                },
                ProstStreamPlan {
                    node: Some(ProstStreamNode::ChainNode(Default::default())),
                    ..Default::default()
                },
            ],
            node: Some(ProstStreamNode::ChainNode(Default::default())),
            ..Default::default()
        }
    }
}
