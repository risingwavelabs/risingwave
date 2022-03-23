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

use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::stream_node::Node as ProstStreamNode;
use risingwave_pb::stream_plan::StreamNode as ProstStreamPlan;

use super::{LogicalScan, PlanBase, ToStreamProst};
use crate::catalog::ColumnId;
use crate::optimizer::property::{Distribution, WithSchema};

/// `StreamTableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to MView
/// creation request.
// TODO: rename to `StreamChain`
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
            ctx,
            logical.schema().clone(),
            vec![0], // TODO
            Distribution::Single,
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
            "StreamTableScan {{ table: {}, columns: [{}], pk_indices: {:?} }}",
            self.logical.table_name(),
            self.logical.column_names().join(", "),
            self.base.pk_indices
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
        use risingwave_pb::plan::*;
        use risingwave_pb::stream_plan::*;

        let batch_plan_node = BatchPlanNode {
            table_ref_id: Some(TableRefId {
                table_id: self.logical.table_id() as i32,
                schema_ref_id: Default::default(),
            }),
            column_descs: self
                .schema()
                .fields()
                .iter()
                .zip_eq(self.logical.columns().iter())
                .zip_eq(self.logical.column_names().iter())
                .map(|((field, column_id), column_name)| ColumnDesc {
                    column_type: Some(field.data_type().to_protobuf()),
                    column_id: column_id.get_id(),
                    name: column_name.clone(),
                    field_descs: vec![],
                    type_name: "".to_string(),
                })
                .collect(),
        };

        ProstStreamPlan {
            input: vec![
                // The merge node should be empty
                ProstStreamPlan {
                    node: Some(ProstStreamNode::MergeNode(Default::default())),
                    ..Default::default()
                },
                ProstStreamPlan {
                    node: Some(ProstStreamNode::BatchPlanNode(batch_plan_node)),
                    ..Default::default()
                },
            ],
            node: Some(ProstStreamNode::ChainNode(Default::default())),
            ..Default::default()
        }
    }
}
