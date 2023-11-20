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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::PbStreamNode;

use super::stream::prelude::*;
use super::utils::{childless_record, Distill};
use super::{generic, ExprRewritable, PlanBase, PlanNodeId, PlanRef, StreamNode};
use crate::catalog::ColumnId;
use crate::expr::ExprRewriter;
use crate::handler::create_source::debezium_cdc_source_schema;
use crate::optimizer::plan_node::utils::{IndicesDisplay, TableCatalogBuilder};
use crate::optimizer::property::{Distribution, DistributionDisplay};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{Explain, TableCatalog};

/// `StreamCdcTableScan` is a virtual plan node to represent a stream cdc table scan.
/// It will be converted to cdc backfill + merge node (for upstream source)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamCdcTableScan {
    pub base: PlanBase<Stream>,
    core: generic::CdcScan,
    batch_plan_id: PlanNodeId,
}

impl StreamCdcTableScan {
    pub fn new(core: generic::CdcScan) -> Self {
        let batch_plan_id = core.ctx.next_plan_node_id();
        let distribution = Distribution::SomeShard;
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
        }
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn core(&self) -> &generic::CdcScan {
        &self.core
    }

    /// Build catalog for cdc backfill state
    /// Right now we only persist whether the backfill is finished and the corresponding cdc offset
    /// schema: | `split_id` | `pk...` | `backfill_finished` | `row_count` | `cdc_offset` |
    pub fn build_backfill_state_catalog(
        &self,
        state: &mut BuildFragmentGraphState,
    ) -> TableCatalog {
        let properties = self.ctx().with_options().internal_table_subset();
        let mut catalog_builder = TableCatalogBuilder::new(properties);
        let upstream_schema = &self.core.get_table_columns();

        // Use `split_id` as primary key in state table.
        // Currently we only support single split for cdc backfill.
        catalog_builder.add_column(&Field::with_name(DataType::Varchar, "split_id"));
        catalog_builder.add_order_column(0, OrderType::ascending());

        // pk columns
        for col_order in self.core.primary_key() {
            let col = &upstream_schema[col_order.column_index];
            catalog_builder.add_column(&Field::from(col));
        }

        catalog_builder.add_column(&Field::with_name(DataType::Boolean, "backfill_finished"));

        // `row_count` column, the number of rows read from snapshot
        catalog_builder.add_column(&Field::with_name(DataType::Int64, "row_count"));

        // The offset is only for observability, not for recovery right now
        catalog_builder.add_column(&Field::with_name(DataType::Jsonb, "cdc_offset"));

        // leave dist key empty, since the cdc backfill executor is singleton
        catalog_builder
            .build(vec![], 1)
            .with_id(state.gen_table_id_wrapped())
    }
}

impl_plan_tree_node_for_leaf! { StreamCdcTableScan }

impl Distill for StreamCdcTableScan {
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

        childless_record("StreamCdcTableScan", vec)
    }
}

impl StreamNode for StreamCdcTableScan {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        unreachable!("stream scan cannot be converted into a prost body -- call `adhoc_to_stream_prost` instead.")
    }
}

impl StreamCdcTableScan {
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

        // The required columns from the table (both scan and upstream).
        let upstream_column_ids = self
            .core
            .output_and_pk_column_ids()
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
        let upstream_schema = {
            let mut columns = debezium_cdc_source_schema();
            columns.push(ColumnCatalog::row_id_column());
            columns
                .into_iter()
                .map(|c| Field::from(c.column_desc).to_prost())
                .collect_vec()
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
            table_desc: None,
            column_ids: upstream_column_ids.clone(),
        };

        let catalog = self
            .build_backfill_state_catalog(state)
            .to_internal_table_prost();

        // We need to pass the id of upstream source job here
        let upstream_source_id = self.core.cdc_table_desc.source_id.table_id;
        let node_body = PbNodeBody::StreamScan(StreamScanNode {
            table_id: upstream_source_id,
            // The column indices need to be forwarded to the downstream
            output_indices,
            upstream_column_ids,
            // The table desc used by backfill executor
            state_table: Some(catalog),
            rate_limit: None,
            cdc_table_desc: Some(self.core.cdc_table_desc.to_protobuf()),
            ..Default::default()
        });

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
            ],

            node_body: Some(node_body),
            stream_key,
            operator_id: self.base.id().0 as u64,
            identity: self.distill_to_string(),
            append_only: self.append_only(),
        }
    }
}

impl ExprRewritable for StreamCdcTableScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let core = self.core.clone();
        core.rewrite_exprs(r);
        Self::new(core).into()
    }
}
