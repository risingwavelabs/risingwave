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

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_pb::batch_plan::SysRowSeqScanNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::plan_common::PbColumnDesc;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record};
use super::{
    BatchPlanRef as PlanRef, ExprRewritable, PlanBase, ToBatchPb, ToDistributedBatch, generic,
};
use crate::error::Result;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, DistributionDisplay, Order};

/// `BatchSysSeqScan` implements [`super::LogicalSysScan`] to scan from a row-oriented table
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSysSeqScan {
    pub base: PlanBase<Batch>,
    core: generic::SysScan,
}

impl BatchSysSeqScan {
    fn new_inner(core: generic::SysScan) -> Self {
        let base = PlanBase::new_batch_with_core(&core, Distribution::Single, Order::any());

        Self { base, core }
    }

    pub fn new(core: generic::SysScan) -> Self {
        Self::new_inner(core)
    }

    fn clone_with_dist(&self) -> Self {
        Self::new_inner(self.core.clone())
    }

    /// Get a reference to the batch seq scan's logical.
    #[must_use]
    pub fn core(&self) -> &generic::SysScan {
        &self.core
    }
}

impl_plan_tree_node_for_leaf! { Batch, BatchSysSeqScan }

impl Distill for BatchSysSeqScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(4);
        vec.push(("table", Pretty::from(self.core.table.name.clone())));
        vec.push(("columns", self.core.columns_pretty(verbose)));

        if verbose {
            let dist = Pretty::display(&DistributionDisplay {
                distribution: self.distribution(),
                input_schema: self.base.schema(),
            });
            vec.push(("distribution", dist));
        }

        childless_record("BatchSysScan", vec)
    }
}

impl ToDistributedBatch for BatchSysSeqScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchSysSeqScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_descs = self
            .core
            .column_descs()
            .iter()
            .map(PbColumnDesc::from)
            .collect();
        NodeBody::SysRowSeqScan(SysRowSeqScanNode {
            table_id: self.core.table.id.table_id,
            column_descs,
        })
    }
}

impl ToLocalBatch for BatchSysSeqScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::new_inner(self.core.clone()).into())
    }
}

impl ExprRewritable<Batch> for BatchSysSeqScan {}

impl ExprVisitable for BatchSysSeqScan {}
