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
use risingwave_pb::batch_plan::row_seq_scan_node::ChunkSize;
use risingwave_pb::batch_plan::{RowSeqScanNode, SysRowSeqScanNode};
use risingwave_pb::plan_common::PbColumnDesc;

use super::batch::prelude::*;
use super::utils::{childless_record, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanRef, ToBatchPb, ToDistributedBatch};
use crate::catalog::ColumnId;
use crate::expr::ExprRewriter;
use crate::optimizer::plan_node::ToLocalBatch;
use crate::optimizer::property::{Distribution, DistributionDisplay, Order};

/// `BatchSeqScan` implements [`super::LogicalScan`] to scan from a row-oriented table
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BatchSeqScan {
    pub base: PlanBase<Batch>,
    core: generic::Scan,
    scan_ranges: Vec<ScanRange>,
}

impl BatchSeqScan {
    fn new_inner(core: generic::Scan, dist: Distribution, scan_ranges: Vec<ScanRange>) -> Self {
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

    pub fn new(core: generic::Scan, scan_ranges: Vec<ScanRange>) -> Self {
        // Use `Single` by default, will be updated later with `clone_with_dist`.
        Self::new_inner(core, Distribution::Single, scan_ranges)
    }

    fn clone_with_dist(&self) -> Self {
        Self::new_inner(
            self.core.clone(),
            if self.core.is_sys_table() {
                Distribution::Single
            } else {
                match self.core.distribution_key() {
                    None => Distribution::SomeShard,
                    Some(distribution_key) => {
                        if distribution_key.is_empty() {
                            Distribution::Single
                        } else {
                            // For other batch operators, `HashShard` is a simple hashing, i.e.,
                            // `target_shard = hash(dist_key) % shard_num`
                            //
                            // But MV is actually sharded by consistent hashing, i.e.,
                            // `target_shard = vnode_mapping.map(hash(dist_key) % vnode_num)`
                            //
                            // They are incompatible, so we just specify its distribution as
                            // `SomeShard` to force an exchange is
                            // inserted.
                            Distribution::UpstreamHashShard(
                                distribution_key,
                                self.core.table_desc.table_id,
                            )
                        }
                    }
                }
            },
            self.scan_ranges.clone(),
        )
    }

    /// Get a reference to the batch seq scan's logical.
    #[must_use]
    pub fn core(&self) -> &generic::Scan {
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

impl_plan_tree_node_for_leaf! { BatchSeqScan }

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

impl Distill for BatchSeqScan {
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

impl ToDistributedBatch for BatchSeqScan {
    fn to_distributed(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}

impl ToBatchPb for BatchSeqScan {
    fn to_batch_prost_body(&self) -> NodeBody {
        let column_descs = self
            .core
            .column_descs()
            .iter()
            .map(PbColumnDesc::from)
            .collect();

        if self.core.is_sys_table() {
            NodeBody::SysRowSeqScan(SysRowSeqScanNode {
                table_id: self.core.table_desc.table_id.table_id,
                column_descs,
            })
        } else {
            NodeBody::RowSeqScan(RowSeqScanNode {
                table_desc: Some(self.core.table_desc.to_protobuf()),
                column_ids: self
                    .core
                    .output_column_ids()
                    .iter()
                    .map(ColumnId::get_id)
                    .collect(),
                scan_ranges: self.scan_ranges.iter().map(|r| r.to_protobuf()).collect(),
                // To be filled by the scheduler.
                vnode_bitmap: None,
                ordered: !self.order().is_any(),
                chunk_size: self
                    .core
                    .chunk_size
                    .map(|chunk_size| ChunkSize { chunk_size }),
            })
        }
    }
}

impl ToLocalBatch for BatchSeqScan {
    fn to_local(&self) -> Result<PlanRef> {
        let dist = if self.core.is_sys_table() {
            Distribution::Single
        } else if let Some(distribution_key) = self.core.distribution_key()
            && !distribution_key.is_empty()
        {
            Distribution::UpstreamHashShard(distribution_key, self.core.table_desc.table_id)
        } else {
            // NOTE(kwannoel): This is a hack to force an exchange to always be inserted before
            // scan.
            Distribution::SomeShard
        };
        Ok(Self::new_inner(self.core.clone(), dist, self.scan_ranges.clone()).into())
    }
}

impl ExprRewritable for BatchSeqScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core, self.scan_ranges.clone()).into()
    }
}
