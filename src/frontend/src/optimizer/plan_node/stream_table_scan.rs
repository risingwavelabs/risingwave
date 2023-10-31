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
use std::rc::Rc;

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, Field, TableDesc};
use risingwave_common::hash::VirtualNode;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::{ChainType, PbStreamNode};

use super::stream::prelude::*;
use super::utils::{childless_record, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanNodeId, PlanRef, StreamNode};
use crate::catalog::ColumnId;
use crate::expr::{ExprRewriter, FunctionCall};
use crate::handler::create_source::debezium_cdc_source_schema;
use crate::optimizer::plan_node::utils::{IndicesDisplay, TableCatalogBuilder};
use crate::optimizer::property::{Distribution, DistributionDisplay};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{Explain, TableCatalog};

/// `StreamTableScan` is a virtual plan node to represent a stream table scan. It will be converted
/// to chain + merge node (for upstream materialize) + batch table scan when converting to `MView`
/// creation request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamTableScan {
    pub base: PlanBase<Stream>,
    core: generic::Scan,
    batch_plan_id: PlanNodeId,
    chain_type: ChainType,
}

impl StreamTableScan {
    pub fn new(core: generic::Scan) -> Self {
        Self::new_with_chain_type(core, ChainType::Backfill)
    }

    pub fn new_with_chain_type(core: generic::Scan, chain_type: ChainType) -> Self {
        let batch_plan_id = core.ctx.next_plan_node_id();

        // TODO: correctly derive the distribution for cdc backfill
        let distribution = {
            match core.distribution_key() {
                Some(distribution_key) => {
                    if distribution_key.is_empty() {
                        Distribution::Single
                    } else {
                        // See also `BatchSeqScan::clone_with_dist`.
                        Distribution::UpstreamHashShard(distribution_key, core.table_desc.table_id)
                    }
                }
                None => Distribution::SomeShard,
            }
        };

        let base = PlanBase::new_stream_with_core(
            &core,
            distribution,
            core.append_only(),
            false,
            core.watermark_columns(),
        );
        Self {
            base,
            core,
            batch_plan_id,
            chain_type,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn core(&self) -> &generic::Scan {
        &self.core
    }

    pub fn to_index_scan(
        &self,
        index_name: &str,
        index_table_desc: Rc<TableDesc>,
        primary_to_secondary_mapping: &BTreeMap<usize, usize>,
        function_mapping: &HashMap<FunctionCall, usize>,
        chain_type: ChainType,
    ) -> StreamTableScan {
        let logical_index_scan = self.core.to_index_scan(
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

    /// Build catalog for backfill state
    ///
    /// Schema: | vnode | pk ... | `backfill_finished` | `row_count` |
    ///
    /// key:    | vnode |
    /// value:  | pk ... | `backfill_finished` | `row_count` |
    ///
    /// When we update the backfill progress,
    /// we update it for all vnodes.
    ///
    /// `pk` refers to the upstream pk which we use to track the backfill progress.
    ///
    /// `vnode` is the corresponding vnode of the upstream's distribution key.
    ///         It should also match the vnode of the backfill executor.
    ///
    /// `backfill_finished` is a boolean which just indicates if backfill is done.
    ///
    /// `row_count` is a count of rows which indicates the # of rows per executor.
    ///             We used to track this in memory.
    ///             But for backfill persistence we have to also persist it.
    ///
    /// FIXME(kwannoel):
    /// - Across all vnodes, the values are the same.
    /// - e.g. | vnode | pk ...  | `backfill_finished` | `row_count` |
    ///        | 1002 | Int64(1) | t                   | 10          |
    ///        | 1003 | Int64(1) | t                   | 10          |
    ///        | 1003 | Int64(1) | t                   | 10          |
    /// Eventually we should track progress per vnode, to support scaling with both mview and
    /// the corresponding `no_shuffle_backfill`.
    /// However this is not high priority, since we are working on supporting arrangement backfill,
    /// which already has this capability.
    pub fn build_backfill_state_catalog(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> TableCatalog {
        let properties = self.ctx().with_options().internal_table_subset();
        let mut catalog_builder = TableCatalogBuilder::new(properties);
        let upstream_schema = &self.core.get_table_columns();

        // We use vnode as primary key in state table.
        // If `Distribution::Single`, vnode will just be `VirtualNode::default()`.
        catalog_builder.add_column(&Field::with_name(VirtualNode::RW_TYPE, "vnode"));
        catalog_builder.add_order_column(0, OrderType::ascending());

        // pk columns
        for col_order in self.core.primary_key() {
            let col = &upstream_schema[col_order.column_index];
            catalog_builder.add_column(&Field::from(col));
        }

        // `backfill_finished` column
        catalog_builder.add_column(&Field::with_name(
            DataType::Boolean,
            format!("{}_backfill_finished", self.table_name()),
        ));

        // `row_count` column
        catalog_builder.add_column(&Field::with_name(
            DataType::Int64,
            format!("{}_row_count", self.table_name()),
        ));

        // Reuse the state store pk (vnode) as the vnode as well.
        catalog_builder.set_vnode_col_idx(0);
        catalog_builder.set_dist_key_in_pk(vec![0]);

        let num_of_columns = catalog_builder.columns().len();
        catalog_builder.set_value_indices((1..num_of_columns).collect_vec());

        catalog_builder
            .build(vec![0], 1)
            .with_id(state.gen_table_id_wrapped())
    }
}

impl_plan_tree_node_for_leaf! { StreamTableScan }

impl Distill for StreamTableScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(4);
        vec.push(("table", Pretty::from(self.core.table_name.clone())));
        vec.push(("columns", self.core.columns_pretty(verbose)));

        if verbose {
            let pk = IndicesDisplay {
                indices: self.stream_key().unwrap_or_default(),
                schema: self.base.schema(),
            };
            vec.push(("pk", pk.distill()));
            let dist = Pretty::display(&DistributionDisplay {
                distribution: self.distribution(),
                input_schema: self.base.schema(),
            });
            vec.push(("dist", dist));
        }

        childless_record("StreamTableScan", vec)
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

        let stream_key = self
            .stream_key()
            .unwrap_or_else(|| {
                panic!(
                    "should always have a stream key in the stream plan but not, sub plan: {}",
                    PlanRef::from(self.clone()).explain_to_string()
                )
            })
            .iter()
            .map(|x| *x as u32)
            .collect_vec();

        // A flag to mark whether the upstream is a cdc source job
        let cdc_upstream = matches!(self.chain_type, ChainType::CdcBackfill);

        // The required columns from the table (both scan and upstream).
        let upstream_column_ids = match self.chain_type {
            // For backfill, we additionally need the primary key columns.
            ChainType::Backfill | ChainType::CdcBackfill => self.core.output_and_pk_column_ids(),
            ChainType::Chain | ChainType::Rearrange | ChainType::UpstreamOnly => {
                self.core.output_column_ids()
            }
            ChainType::ChainUnspecified => unreachable!(),
        }
        .iter()
        .map(ColumnId::get_id)
        .collect_vec();

        // The schema of the snapshot read stream
        let snapshot_schema = upstream_column_ids
            .iter()
            .map(|&id| {
                let col = self
                    .core
                    .get_table_columns()
                    .iter()
                    .find(|c| c.column_id.get_id() == id)
                    .unwrap();
                Field::from(col).to_prost()
            })
            .collect_vec();

        // The schema of the shared cdc source upstream is different from snapshot,
        // refer to `debezium_cdc_source_schema()` for details.
        let upstream_schema = if cdc_upstream {
            let mut columns = debezium_cdc_source_schema();
            columns.push(ColumnCatalog::row_id_column());
            columns
                .into_iter()
                .map(|c| Field::from(c.column_desc).to_prost())
                .collect_vec()
        } else {
            snapshot_schema.clone()
        };

        let output_indices = self
            .core
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
            table_desc: if cdc_upstream {
                None
            } else {
                Some(self.core.table_desc.to_protobuf())
            },
            column_ids: upstream_column_ids.clone(),
        };

        let catalog = self
            .build_backfill_state_catalog(state)
            .to_internal_table_prost();

        let node_body = if cdc_upstream {
            // don't need batch plan for cdc source
            PbNodeBody::Chain(ChainNode {
                table_id: self.core.cdc_table_desc.table_id.table_id,
                chain_type: self.chain_type as i32,
                // The column indices need to be forwarded to the downstream
                output_indices,
                upstream_column_ids,
                // The table desc used by backfill executor
                state_table: Some(catalog),
                rate_limit: None,
                cdc_table_desc: Some(self.core.cdc_table_desc.to_protobuf()),
                ..Default::default()
            })
        } else {
            PbNodeBody::Chain(ChainNode {
                table_id: self.core.table_desc.table_id.table_id,
                chain_type: self.chain_type as i32,
                // The column indices need to be forwarded to the downstream
                output_indices,
                upstream_column_ids,
                // The table desc used by backfill executor
                table_desc: Some(self.core.table_desc.to_protobuf()),
                state_table: Some(catalog),
                rate_limit: self
                    .base
                    .ctx()
                    .session_ctx()
                    .config()
                    .get_streaming_rate_limit(),
                ..Default::default()
            })
        };

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
                    fields: snapshot_schema,
                    stream_key: vec![], // not used
                    input: vec![],
                    append_only: true,
                },
            ],

            node_body: Some(node_body),
            stream_key,
            operator_id: self.base.id().0 as u64,
            identity: {
                let s = self.distill_to_string();
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
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new_with_chain_type(core, self.chain_type).into()
    }
}
