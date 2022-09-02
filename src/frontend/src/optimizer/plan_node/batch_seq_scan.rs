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
use std::ops::Bound;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_common::types::ScalarImpl;
use risingwave_common::util::scan_range::{is_full_range, ScanRange};
use risingwave_pb::batch_plan::plan_node::NodeBody;
use risingwave_pb::batch_plan::{RowSeqScanNode, SysRowSeqScanNode};
use risingwave_pb::plan_common::ColumnDesc as ProstColumnDesc;

use super::{PlanBase, PlanRef, ToBatchProst, ToDistributedBatch};
use crate::catalog::ColumnId;
use crate::optimizer::plan_node::{LogicalScan, ToLocalBatch};
use crate::optimizer::property::{Distribution, DistributionDisplay};

/// `BatchSeqScan` implements [`super::LogicalScan`] to scan from a row-oriented table
#[derive(Debug, Clone)]
pub struct BatchSeqScan {
    pub base: PlanBase,
    logical: LogicalScan,
    scan_ranges: Vec<ScanRange>,
}

impl BatchSeqScan {
    fn new_inner(logical: LogicalScan, dist: Distribution, scan_ranges: Vec<ScanRange>) -> Self {
        let ctx = logical.base.ctx.clone();
        let base = PlanBase::new_batch(
            ctx,
            logical.schema().clone(),
            dist,
            logical.get_out_column_index_order(),
        );

        {
            // validate scan_range
            scan_ranges.iter().for_each(|scan_range| {
                assert!(!scan_range.is_full_table_scan());
                let scan_pk_prefix_len = scan_range.eq_conds.len();
                let order_len = logical.table_desc().order_column_indices().len();
                assert!(
                    scan_pk_prefix_len < order_len
                        || (scan_pk_prefix_len == order_len && is_full_range(&scan_range.range)),
                    "invalid scan_range",
                );
            })
        }

        Self {
            base,
            logical,
            scan_ranges,
        }
    }

    pub fn new(logical: LogicalScan, scan_ranges: Vec<ScanRange>) -> Self {
        // Use `Single` by default, will be updated later with `clone_with_dist`.
        Self::new_inner(logical, Distribution::Single, scan_ranges)
    }

    fn clone_with_dist(&self) -> Self {
        Self::new_inner(
            self.logical.clone(),
            if self.logical.is_sys_table() {
                Distribution::Single
            } else {
                match self.logical.distribution_key() {
                    None => Distribution::SomeShard,
                    Some(distribution_key) => {
                        // FIXME: Should be `Single` if distribution_key.is_empty().
                        // Currently the task will be scheduled to frontend under local mode, which
                        // is unimplemented yet. Enable this when it's done.

                        // For other batch operators, `HashShard` is a simple hashing, i.e.,
                        // `target_shard = hash(dist_key) % shard_num`
                        //
                        // But MV is actually sharded by consistent hashing, i.e.,
                        // `target_shard = vnode_mapping.map(hash(dist_key) % vnode_num)`
                        //
                        // They are incompatible, so we just specify its distribution as `SomeShard`
                        // to force an exchange is inserted.
                        Distribution::UpstreamHashShard(distribution_key)
                    }
                }
            },
            self.scan_ranges.clone(),
        )
    }

    /// Get a reference to the batch seq scan's logical.
    #[must_use]
    pub fn logical(&self) -> &LogicalScan {
        &self.logical
    }

    pub fn scan_ranges(&self) -> &[ScanRange] {
        &self.scan_ranges
    }
}

impl_plan_tree_node_for_leaf! { BatchSeqScan }

impl fmt::Display for BatchSeqScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
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

        let verbose = self.base.ctx.is_explain_verbose();

        write!(
            f,
            "BatchScan {{ table: {}, columns: [{}]",
            self.logical.table_name(),
            match verbose {
                true => self.logical.column_names_with_table_prefix(),
                false => self.logical.column_names(),
            }
            .join(", "),
        )?;

        if !self.scan_ranges.is_empty() {
            let order_names = match verbose {
                true => self.logical.order_names_with_table_prefix(),
                false => self.logical.order_names(),
            };
            let mut range_strs = vec![];
            for scan_range in &self.scan_ranges {
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
                range_strs.push(range_str.join(", "));
            }
            write!(f, ", scan_ranges: [{}]", range_strs.join(" OR "))?;
        }

        if verbose {
            write!(
                f,
                ", distribution: {}",
                &DistributionDisplay {
                    distribution: self.distribution(),
                    input_schema: &self.base.schema,
                }
            )?;
        }

        write!(f, " }}")
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

        if self.logical.is_sys_table() {
            NodeBody::SysRowSeqScan(SysRowSeqScanNode {
                table_name: self.logical.table_name().to_string(),
                column_descs,
            })
        } else {
            NodeBody::RowSeqScan(RowSeqScanNode {
                table_desc: Some(self.logical.table_desc().to_protobuf()),
                column_ids: self
                    .logical
                    .output_column_ids()
                    .iter()
                    .map(ColumnId::get_id)
                    .collect(),
                scan_ranges: self.scan_ranges.iter().map(|r| r.to_protobuf()).collect(),
                // To be filled by the scheduler.
                vnode_bitmap: None,
            })
        }
    }
}

impl ToLocalBatch for BatchSeqScan {
    fn to_local(&self) -> Result<PlanRef> {
        Ok(self.clone_with_dist().into())
    }
}
