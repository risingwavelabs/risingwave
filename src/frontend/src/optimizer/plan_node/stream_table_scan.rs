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

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::catalog::{Field, TableDesc};
use risingwave_common::types::DataType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::{ChainType, PbStreamNode};

use super::{generic, ExprRewritable, PlanBase, PlanNodeId, PlanRef, StreamNode};
use crate::catalog::ColumnId;
use crate::expr::{ExprRewriter, FunctionCall};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::utils::{IndicesDisplay, TableCatalogBuilder};
use crate::optimizer::property::{Distribution, DistributionDisplay};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::TableCatalog;

/// `StreamTableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTableScan {
    pub base: PlanBase,
    logical: generic::Scan,
    batch_plan_id: PlanNodeId,
    chain_type: ChainType,
}

impl StreamTableScan {
    pub fn new(logical: generic::Scan) -> Self {
        Self::new_with_chain_type(logical, ChainType::Backfill)
    }

    pub fn new_with_chain_type(logical: generic::Scan, chain_type: ChainType) -> Self {
        let batch_plan_id = logical.ctx.next_plan_node_id();

        let distribution = {
            match logical.distribution_key() {
                Some(distribution_key) => {
                    if distribution_key.is_empty() {
                        Distribution::Single
                    } else {
                        // See also `BatchSeqScan::clone_with_dist`.
                        Distribution::UpstreamHashShard(
                            distribution_key,
                            logical.table_desc.table_id,
                        )
                    }
                }
                None => Distribution::SomeShard,
            }
        };
        let base = PlanBase::new_stream_with_logical(
            &logical,
            distribution,
            logical.table_desc.append_only,
            false,
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
        &self.logical.table_name
    }

    pub fn logical(&self) -> &generic::Scan {
        &self.logical
    }

    pub fn to_index_scan(
        &self,
        index_name: &str,
        index_table_desc: Rc<TableDesc>,
        primary_to_secondary_mapping: &BTreeMap<usize, usize>,
        function_mapping: &HashMap<FunctionCall, usize>,
        chain_type: ChainType,
    ) -> StreamTableScan {
        let logical_index_scan = self.logical.to_index_scan(
            index_name,
            index_table_desc,
            primary_to_secondary_mapping,
            function_mapping,
        );
        logical_index_scan
            .distribution_key()
            .expect("distribution key of stream chain must exist in output columns");
        StreamTableScan::new_with_chain_type(logical_index_scan, chain_type)
    }

    pub fn chain_type(&self) -> ChainType {
        self.chain_type
    }

    // Build catalog for backfill state
    pub fn build_backfill_state_catalog(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> TableCatalog {
        let properties = self.ctx().with_options().internal_table_subset();
        let mut catalog_builder = TableCatalogBuilder::new(properties);
        let schema = self.base.schema();

        for pos in &self.base.logical_pk {
            let field = &schema[*pos];
            catalog_builder.add_column(field);
        }

        catalog_builder.add_column(&Field::with_name(
            DataType::Boolean,
            format!("{}_backfill_finished", self.table_name()),
        ));

        catalog_builder
            .build(vec![], 0)
            .with_id(state.gen_table_id_wrapped())
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
            .field("table", &format_args!("{}", self.logical.table_name))
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
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamTableScan {
    pub fn adhoc_to_stream_prost(&self, state: &mut BuildFragmentGraphState) -> PbStreamNode {
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
                    .table_desc
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
            table_desc: Some(self.logical.table_desc.to_protobuf()),
            column_ids: upstream_column_ids.clone(),
        };

        let catalog = self.build_backfill_state_catalog(state);
        let catalog = catalog.to_internal_table_prost();

        PbStreamNode {
            fields: self.schema().to_prost(),
            input: vec![
                // The merge node body will be filled by the `ActorBuilder` on the meta service.
                PbStreamNode {
                    node_body: Some(PbNodeBody::Merge(Default::default())),
                    identity: "Upstream".into(),
                    fields: upstream_schema.clone(),
                    stream_key: vec![], // not used
                    ..Default::default()
                },
                PbStreamNode {
                    node_body: Some(PbNodeBody::BatchPlan(batch_plan_node)),
                    operator_id: self.batch_plan_id.0 as u64,
                    identity: "BatchPlanNode".into(),
                    fields: upstream_schema,
                    stream_key: vec![], // not used
                    input: vec![],
                    append_only: true,
                },
            ],
            node_body: Some(PbNodeBody::Chain(ChainNode {
                table_id: self.logical.table_desc.table_id.table_id,
                chain_type: self.chain_type as i32,
                // The column indices need to be forwarded to the downstream
                output_indices,
                upstream_column_ids,
                // The table desc used by backfill executor
                table_desc: Some(self.logical.table_desc.to_protobuf()),
                state_table: Some(catalog),
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
        let mut logical = self.logical.clone();
        logical.rewrite_exprs(r);
        Self::new_with_chain_type(logical, self.chain_type).into()
    }
}
