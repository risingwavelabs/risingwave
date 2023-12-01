// Copyright 2023 RisingWave Labs
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

use std::ops::Bound;

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::error::Result;
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::scan_range::{is_full_range, ScanRange};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::SysRowSeqScanNode;
use risingwave_pb::plan_common::PbColumnDesc;

use super::batch::prelude::*;
use super::utils::{childless_record, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch};
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::ToLocalBatch;
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

    fn scan_ranges_as_strs(&self, verbose: bool) -> Vec<String> {
        let order_names = match verbose {
            true => self.core.order_names_with_table_prefix(),
            false => self.core.order_names(),
        };
        let mut range_strs = vec![];

        let explain_max_range = 20;
        for scan_range in self.scan_ranges.iter().take(explain_max_range) {
            #[expect(clippy::disallowed_methods)]
            let mut range_str = scan_range
                .eq_conds
                .iter()
                .zip(order_names.iter())
                .map(|(v, name)| match v {
                    Some(v) => format!("{} = {:?}", name, v),
                    None => format!("{} IS NULL", name),
                })
                .collect_vec();
            if !is_full_range(&scan_range.range) {
                let i = scan_range.eq_conds.len();
                range_str.push(range_to_string(&order_names[i], &scan_range.range))
            }
            range_strs.push(range_str.join(" AND "));
        }
        if self.scan_ranges.len() > explain_max_range {
            range_strs.push("...".to_string());
        }
        range_strs
    }
}

impl_plan_tree_node_for_leaf! { BatchSysSeqScan }

fn lb_to_string(name: &str, lb: &Bound<ScalarImpl>) -> String {
    let (op, v) = match lb {
        Bound::Included(v) => (">=", v),
        Bound::Excluded(v) => (">", v),
        Bound::Unbounded => unreachable!(),
    };
    format!("{} {} {:?}", name, op, v)
}
fn ub_to_string(name: &str, ub: &Bound<ScalarImpl>) -> String {
    let (op, v) = match ub {
        Bound::Included(v) => ("<=", v),
        Bound::Excluded(v) => ("<", v),
        Bound::Unbounded => unreachable!(),
    };
    format!("{} {} {:?}", name, op, v)
}
fn range_to_string(name: &str, range: &(Bound<ScalarImpl>, Bound<ScalarImpl>)) -> String {
    match (&range.0, &range.1) {
        (Bound::Unbounded, Bound::Unbounded) => unreachable!(),
        (Bound::Unbounded, ub) => ub_to_string(name, ub),
        (lb, Bound::Unbounded) => lb_to_string(name, lb),
        (lb, ub) => {
            format!("{} AND {}", lb_to_string(name, lb), ub_to_string(name, ub))
        }
    }
}

impl Distill for BatchSysSeqScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(4);
        vec.push(("table", Pretty::from(self.core.table_name.clone())));
        vec.push(("columns", self.core.columns_pretty(verbose)));

        if !self.scan_ranges.is_empty() {
            let range_strs = self.scan_ranges_as_strs(verbose);
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
