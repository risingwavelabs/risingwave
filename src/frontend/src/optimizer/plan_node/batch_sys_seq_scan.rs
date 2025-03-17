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
use risingwave_common::util::scan_range::{ScanRange, is_full_range};
use risingwave_pb::batch_plan::SysRowSeqScanNode;
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::plan_common::PbColumnDesc;

use super::batch::prelude::*;
use super::utils::{Distill, childless_record, scan_ranges_as_strs};
use super::{ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch, generic};
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::property::{Distribution, DistributionDisplay, Order};

/// `BatchSysSeqScan` implements [`super::LogicalSysScan`] to scan from a row-oriented table
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSysSeqScan {
    pub base: PlanBase<Batch>,
    core: generic::SysScan,
    scan_ranges: Vec<ScanRange>,
}

impl BatchSysSeqScan {
    fn new_inner(core: generic::SysScan, dist: Distribution, scan_ranges: Vec<ScanRange>) -> Self {
        let order = if scan_ranges.len() > 1 {
            Order::any()
        } else {
            core.get_out_column_index_order()
        };
        let base = PlanBase::new_batch_with_core(&core, dist, order);

        {
            // validate scan_range
            scan_ranges.iter().for_each(|scan_range| {
                assert!(!scan_range.is_full_table_scan());
                let scan_pk_prefix_len = scan_range.eq_conds.len();
                let order_len = core.table_desc.order_column_indices().len();
                assert!(
                    scan_pk_prefix_len < order_len
                        || (scan_pk_prefix_len == order_len && is_full_range(&scan_range.range)),
                    "invalid scan_range",
                );
            })
        }

        Self {
            base,
            core,
            scan_ranges,
        }
    }

    pub fn new(core: generic::SysScan, scan_ranges: Vec<ScanRange>) -> Self {
        // Use `Single` by default, will be updated later with `clone_with_dist`.
        Self::new_inner(core, Distribution::Single, scan_ranges)
    }

    fn clone_with_dist(&self) -> Self {
        Self::new_inner(
            self.core.clone(),
            Distribution::Single,
            self.scan_ranges.clone(),
        )
    }

    /// Get a reference to the batch seq scan's logical.
    #[must_use]
    pub fn core(&self) -> &generic::SysScan {
        &self.core
    }

    pub fn scan_ranges(&self) -> &[ScanRange] {
        &self.scan_ranges
    }
}

impl_plan_tree_node_for_leaf! { BatchSysSeqScan }

impl Distill for BatchSysSeqScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(4);
        vec.push(("table", Pretty::from(self.core.table_name.clone())));
        vec.push(("columns", self.core.columns_pretty(verbose)));

        if !self.scan_ranges.is_empty() {
            let order_names = match verbose {
                true => self.core.order_names_with_table_prefix(),
                false => self.core.order_names(),
            };
            let range_strs = scan_ranges_as_strs(order_names, &self.scan_ranges);
            vec.push((
                "scan_ranges",
                Pretty::Array(range_strs.into_iter().map(Pretty::from).collect()),
            ));
        }

        if verbose {
            let dist = Pretty::display(&DistributionDisplay {
                distribution: self.distribution(),
                input_schema: self.base.schema(),
            });
            vec.push(("distribution", dist));
        }

        childless_record("BatchScan", vec)
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
            table_id: self.core.table_desc.table_id.table_id,
            column_descs,
        })
    }
}

impl ToLocalBatch for BatchSysSeqScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(Self::new_inner(
            self.core.clone(),
            Distribution::Single,
            self.scan_ranges.clone(),
        )
        .into())
    }
}

impl ExprRewritable for BatchSysSeqScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.scan_ranges.clone()).into()
    }
}

impl ExprVisitable for BatchSysSeqScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}
