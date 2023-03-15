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

use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::Field;
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::{ChainType, StreamNode as ProstStreamPlan};

use super::{ExprRewritable, LogicalScan, PlanBase, PlanNodeId, PlanRef, StreamNode};
use crate::catalog::ColumnId;
use crate::expr::ExprRewriter;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::property::{Distribution, DistributionDisplay};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamIndexScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request. Compared with `StreamTableScan`, it will reorder columns, and the chain node
/// doesn't allow rearrange.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamIndexScan {
    pub base: PlanBase,
    logical: LogicalScan,
    batch_plan_id: PlanNodeId,
    chain_type: ChainType,
}

impl StreamIndexScan {
    pub fn new(logical: LogicalScan, chain_type: ChainType) -> Self {
        let ctx = logical.base.ctx.clone();

        let distribution = {
            let distribution_key = logical
                .distribution_key()
                .expect("distribution key of stream chain must exist in output columns");
            if distribution_key.is_empty() {
                Distribution::Single
            } else {
                // See also `BatchSeqScan::clone_with_dist`.
                Distribution::UpstreamHashShard(distribution_key, logical.table_desc().table_id)
            }
        };

        let batch_plan_id = ctx.next_plan_node_id();
        // TODO: derive from input
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.base.logical_pk.clone(),
            logical.functional_dependency().clone(),
            distribution,
            false, // TODO: determine the `append-only` field of table scan
            logical.watermark_columns(),
        );
        Self {
            base,
            logical,
            batch_plan_id,
            chain_type,
        }
    }

    pub fn table_name(&self) -> &str {
        self.logical.table_name()
    }

    pub fn logical(&self) -> &LogicalScan {
        &self.logical
    }
}

impl_plan_tree_node_for_leaf! { StreamIndexScan }

impl fmt::Display for StreamIndexScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("StreamIndexScan");

        let v = match verbose {
            false => self.logical.column_names(),
            true => self.logical.column_names_with_table_prefix(),
        }
        .join(", ");
        builder
            .field("index", &format_args!("{}", self.logical.table_name()))
            .field("columns", &format_args!("[{}]", v));

        if verbose {
            builder.field(
                "pk",
                &IndicesDisplay {
                    indices: self.logical_pk(),
                    input_schema: &self.base.schema,
                },
            );
            builder.field(
                "dist",
                &DistributionDisplay {
                    distribution: self.distribution(),
                    input_schema: &self.base.schema,
                },
            );
        }

        builder.finish()
    }
}

impl StreamNode for StreamIndexScan {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        unreachable!("stream index scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamIndexScan {
    // TODO: this method is almost the same as `StreamTableScan::adhoc_to_stream_prost`, we should
    // avoid duplication.
    pub fn adhoc_to_stream_prost(&self) -> ProstStreamPlan {
        use risingwave_pb::plan_common::Field as ProstField;
        use risingwave_pb::stream_plan::*;

        let stream_key = self.base.logical_pk.iter().map(|x| *x as u32).collect_vec();

        // The required columns from the table (both scan and upstream).
        let upstream_column_ids = match self.chain_type {
            // For backfill, we additionally need the primary key columns.
            ChainType::Backfill => self.logical.output_and_pk_column_ids(),
            ChainType::Chain | ChainType::Rearrange | ChainType::UpstreamOnly => {
                self.logical.output_column_ids()
            }
            ChainType::ChainUnspecified => unreachable!(),
        }
        .iter()
        .map(ColumnId::get_id)
        .collect_vec();

        // The schema of the upstream table (both scan and upstream).
        let upstream_schema = upstream_column_ids
            .iter()
            .map(|&id| {
                let col = self
                    .logical
                    .table_desc()
                    .columns
                    .iter()
                    .find(|c| c.column_id.get_id() == id)
                    .unwrap();
                Field::from(col).to_prost()
            })
            .collect_vec();

        let output_indices = self
            .logical
            .output_column_ids()
            .iter()
            .map(|i| {
                upstream_column_ids
                    .iter()
                    .position(|&x| x == i.get_id())
                    .unwrap() as u32
            })
            .collect_vec();

        let batch_plan_node = BatchPlanNode {
            table_desc: Some(self.logical.table_desc().to_protobuf()),
            column_ids: upstream_column_ids.clone(),
        };

        ProstStreamPlan {
            fields: self.schema().to_prost(),
            input: vec![
                // The merge node should be empty
                ProstStreamPlan {
                    node_body: Some(ProstStreamNode::Merge(Default::default())),
                    identity: "Upstream".into(),
                    fields: upstream_schema.clone(),
                    stream_key: vec![], // not used
                    ..Default::default()
                },
                ProstStreamPlan {
                    node_body: Some(ProstStreamNode::BatchPlan(batch_plan_node)),
                    operator_id: self.batch_plan_id.0 as u64,
                    identity: "BatchPlanNode".into(),
                    input: vec![],
                    fields: upstream_schema.clone(),
                    stream_key: vec![], // not used
                    append_only: true,
                },
            ],
            node_body: Some(ProstStreamNode::Chain(ChainNode {
                table_id: self.logical.table_desc().table_id.table_id,
                chain_type: self.chain_type as i32,
                // The fields from upstream
                upstream_fields: self
                    .logical
                    .table_desc()
                    .columns
                    .iter()
                    .map(|x| ProstField {
                        data_type: Some(x.data_type.to_protobuf()),
                        name: x.name.clone(),
                    })
                    .collect(),
                output_indices,
                upstream_column_ids,
                is_singleton: false,
                table_desc: Some(self.logical.table_desc().to_protobuf()),
            })),
            stream_key,
            operator_id: self.base.id.0 as u64,
            identity: format!("{}", self),
            append_only: self.append_only(),
        }
    }
}

impl ExprRewritable for StreamIndexScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new(
            self.logical
                .rewrite_exprs(r)
                .as_logical_scan()
                .unwrap()
                .clone(),
            self.chain_type,
        )
        .into()
    }
}
