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
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::{CellBasedTableDesc, RowSeqScanNode};

use super::{PlanBase, PlanNode, PlanRef, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::LogicalScan;
use crate::optimizer::property::{Distribution, Order, WithSchema};

/// `BatchSeqScan` implements [`super::LogicalScan`] to scan from a row-oriented table
#[derive(Debug, Clone)]
pub struct BatchSeqScan {
    pub base: PlanBase,
    logical: LogicalScan,
}

impl WithSchema for BatchSeqScan {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl BatchSeqScan {
    pub fn new(logical: LogicalScan) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            logical.plan_base().pk_indices.to_vec(),
            Distribution::any().clone(),
            Order::any().clone(),
        );

        Self { logical, base }
    }
}

impl_plan_tree_node_for_leaf! { BatchSeqScan }

impl fmt::Display for BatchSeqScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BatchScan {{ table: {}, columns: [{}] }}",
            self.logical.table_name(),
            self.logical.column_names().join(", ")
        )
    }
}

impl ToDistributedBatch for BatchSeqScan {
    fn to_distributed(&self) -> PlanRef {
        self.clone().into()
    }
}

impl ToBatchProst for BatchSeqScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        // TODO(Bowen): Fix this serialization.
        NodeBody::RowSeqScan(RowSeqScanNode {
            table_desc: Some(CellBasedTableDesc {
                table_id: self.logical.table_id(),
                pk: vec![],
            }),
            ..Default::default()
        })
    }
}
