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

use super::{LogicalScan, PlanBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithSchema};

/// `StreamSourceScan` represents a scan from source.
#[derive(Debug, Clone)]
pub struct StreamSourceScan {
    pub base: PlanBase,
    logical: LogicalScan,
}

impl StreamSourceScan {
    pub fn new(logical: LogicalScan) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            vec![0], // TODO
            Distribution::any().clone(),
        );
        Self { logical, base }
    }
}

impl WithSchema for StreamSourceScan {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl_plan_tree_node_for_leaf! { StreamSourceScan }

impl fmt::Display for StreamSourceScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamSourceScan {{ table: {}, columns: [{}] }}",
            self.logical.table_name(),
            self.logical.column_names().join(", ")
        )
    }
}

impl ToStreamProst for StreamSourceScan {
    fn to_stream_prost_body(&self) -> ProstStreamNode {
        ProstStreamNode::MergeNode(Default::default())
    }
}
