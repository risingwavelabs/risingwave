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

use risingwave_common::error::Result;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{scan_range, RowSeqScanNode, ScanRange};
use risingwave_pb::plan_common::{CellBasedTableDesc, ColumnDesc as ProstColumnDesc};

use super::{PlanBase, PlanRef, ToBatchProst, ToDistributedBatch};
use crate::optimizer::plan_node::{LogicalScan, ToLocalBatch};
use crate::optimizer::property::{Distribution, Order};

/// `BatchSeqScan` implements [`super::LogicalScan`] to scan from a row-oriented table
#[derive(Debug, Clone)]
pub struct BatchSeqScan {
    pub base: PlanBase,
    logical: LogicalScan,
    scan_range: ScanRange,
}

impl BatchSeqScan {
    pub fn new_inner(logical: LogicalScan, dist: Distribution, scan_range: ScanRange) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = PlanBase::new_batch(ctx, logical.schema().clone(), dist, Order::any().clone());

        Self {
            base,
            logical,
            scan_range,
        }
    }

    pub fn new(logical: LogicalScan, scan_range: ScanRange) -> Self {
        Self::new_inner(logical, Distribution::Single, scan_range)
    }

    pub fn clone_with_dist(&self) -> Self {
        Self::new_inner(
            self.logical.clone(),
            Distribution::SomeShard,
            self.scan_range.clone(),
        )
    }

    /// Get a reference to the batch seq scan's logical.
    #[must_use]
    pub fn logical(&self) -> &LogicalScan {
        &self.logical
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
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchProst for BatchSeqScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_descs = self
            .logical
            .column_descs()
            .iter()
            .map(ProstColumnDesc::from)
            .collect();

        NodeBody::RowSeqScan(RowSeqScanNode {
            table_desc: Some(CellBasedTableDesc {
                table_id: self.logical.table_desc().table_id.into(),
                pk: vec![], // TODO:
            }),
            column_descs,
            scan_range: Some(self.scan_range.clone()),
        })
    }
}

impl ToLocalBatch for BatchSeqScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}
