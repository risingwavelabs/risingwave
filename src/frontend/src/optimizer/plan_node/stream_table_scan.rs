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

use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::catalog::{Field, TableDesc};
use risingwave_pb::stream_plan::stream_node::NodeBody as ProstStreamNode;
use risingwave_pb::stream_plan::{ChainType, StreamNode as ProstStreamPlan};

use super::{ExprRewritable, LogicalScan, PlanBase, PlanNodeId, PlanRef, StreamNode};
use crate::catalog::ColumnId;
use crate::expr::ExprRewriter;
use crate::optimizer::plan_node::utils::IndicesDisplay;
use crate::optimizer::property::{Distribution, DistributionDisplay};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamTableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTableScan {
    pub base: PlanBase,
    logical: LogicalScan,
    batch_plan_id: PlanNodeId,
    chain_type: ChainType,
}

impl StreamTableScan {
    pub fn new(logical: LogicalScan) -> Self {
        Self::new_with_chain_type(logical, ChainType::Backfill)
    }

    pub fn new_with_chain_type(logical: LogicalScan, chain_type: ChainType) -> Self {
        let ctx = logical.base.ctx.clone();

        let batch_plan_id = ctx.next_plan_node_id();

        let distribution = {
            match logical.distribution_key() {
                Some(distribution_key) => {
                    if distribution_key.is_empty() {
                        Distribution::Single
                    } else {
                        // See also `BatchSeqScan::clone_with_dist`.
                        Distribution::UpstreamHashShard(
                            distribution_key,
                            logical.table_desc().table_id,
                        )
                    }
                }
                None => Distribution::SomeShard,
            }
        };
        let base = PlanBase::new_stream(
            ctx,
            logical.schema().clone(),
            logical.base.logical_pk.clone(),
            logical.functional_dependency().clone(),
            distribution,
            logical.table_desc().append_only,
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

    pub fn to_index_scan(
        &self,
        index_name: &str,
        index_table_desc: Rc<TableDesc>,
        primary_to_secondary_mapping: &BTreeMap<usize, usize>,
        chain_type: ChainType,
    ) -> StreamTableScan {
        let logical_index_scan =
            self.logical
                .to_index_scan(index_name, index_table_desc, primary_to_secondary_mapping);
        logical_index_scan
            .distribution_key()
            .expect("distribution key of stream chain must exist in output columns");
        StreamTableScan::new_with_chain_type(logical_index_scan, chain_type)
    }

    pub fn chain_type(&self) -> ChainType {
        self.chain_type
    }
}

impl_plan_tree_node_for_leaf! { StreamTableScan }

impl fmt::Display for StreamTableScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = f.debug_struct("StreamTableScan");

        let v = match verbose {
            false => self.logical.column_names(),
            true => self.logical.column_names_with_table_prefix(),
        }
        .join(", ");
        builder
            .field("table", &format_args!("{}", self.logical.table_name()))
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

impl StreamNode for StreamTableScan {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> ProstStreamNode {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamTableScan {
    pub fn adhoc_to_stream_prost(&self) -> ProstStreamPlan {
        use risingwave_pb::plan_common::Field as ProstField;
        use risingwave_pb::stream_plan::*;

        let batch_plan_node = BatchPlanNode {
            table_desc: Some(self.logical.table_desc().to_protobuf()),
            column_ids: self
                .logical
                .output_column_ids()
                .iter()
                .map(ColumnId::get_id)
                .collect(),
        };

        let stream_key = self.logical_pk().iter().map(|x| *x as u32).collect_vec();

        ProstStreamPlan {
            fields: self.schema().to_prost(),
            input: vec![
                // The merge node should be empty
                ProstStreamPlan {
                    node_body: Some(ProstStreamNode::Merge(Default::default())),
                    identity: "Upstream".into(),
                    fields: self
                        .logical
                        .table_desc()
                        .columns
                        .iter()
                        .map(|c| Field::from(c).to_prost())
                        .collect(),
                    stream_key: self
                        .logical
                        .table_desc()
                        .stream_key
                        .iter()
                        .map(|i| *i as _)
                        .collect(),
                    ..Default::default()
                },
                ProstStreamPlan {
                    node_body: Some(ProstStreamNode::BatchPlan(batch_plan_node)),
                    operator_id: self.batch_plan_id.0 as u64,
                    identity: "BatchPlanNode".into(),
                    stream_key: stream_key.clone(),
                    fields: self.schema().to_prost(),
                    input: vec![],
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
                // The column indices need to be forwarded to the downstream
                upstream_column_indices: self
                    .logical
                    .output_column_indices()
                    .iter()
                    .map(|&i| i as _)
                    .collect(),
                is_singleton: *self.distribution() == Distribution::Single,
                // The table desc used by backfill executor
                table_desc: Some(self.logical.table_desc().to_protobuf()),
            })),
            stream_key,
            operator_id: self.base.id.0 as u64,
            identity: {
                let s = format!("{}", self);
                s.replace("StreamTableScan", "Chain")
            },
            append_only: self.append_only(),
        }
    }
}

impl ExprRewritable for StreamTableScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new_with_chain_type(
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
