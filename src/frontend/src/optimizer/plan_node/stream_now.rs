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

use pretty_xmlish::XmlNode;
use risingwave_common::types::Datum;
use risingwave_common::util::value_encoding::DatumToProtoExt;
use risingwave_pb::stream_plan::now_node::PbMode as PbNowMode;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{PbNowModeGenerateSeries, PbNowModeUpdateCurrent, PbNowNode};

use super::generic::{GenericPlanNode, Mode};
use super::stream::prelude::*;
use super::utils::{Distill, TableCatalogBuilder, childless_record};
use super::{ExprRewritable, PlanBase, StreamNode, generic};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::property::{Distribution, Monotonicity, MonotonicityMap, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamNow {
    pub base: PlanBase<Stream>,
    core: generic::Now,
}

impl StreamNow {
    pub fn new(core: generic::Now) -> Self {
        let mut watermark_columns = WatermarkColumns::new();
        watermark_columns.insert(0, core.ctx().next_watermark_group_id()); // `StreamNow` generates watermark messages

        let mut columns_monotonicity = MonotonicityMap::new();
        columns_monotonicity.insert(0, Monotonicity::NonDecreasing);

        let base = PlanBase::new_stream_with_core(
            &core,
            Distribution::Single,
            core.mode.is_generate_series(), // append only
            core.mode.is_generate_series(), // emit on window close
            watermark_columns,
            columns_monotonicity,
        );
        Self { base, core }
    }
}

impl Distill for StreamNow {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let vec = if self.base.ctx().is_explain_verbose() {
            vec![("output", column_names_pretty(self.schema()))]
        } else {
            vec![]
        };

        childless_record("StreamNow", vec)
    }
}

impl_plan_tree_node_for_leaf! { StreamNow }

impl StreamNode for StreamNow {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        let schema = self.base.schema();
        let dist_keys = self.base.distribution().dist_column_indices().to_vec();
        let mut internal_table_catalog_builder = TableCatalogBuilder::default();
        schema.fields().iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });

        let table_catalog = internal_table_catalog_builder
            .build(dist_keys, 0)
            .with_id(state.gen_table_id_wrapped());
        NodeBody::Now(Box::new(PbNowNode {
            state_table: Some(table_catalog.to_internal_table_prost()),
            mode: Some(match &self.core.mode {
                Mode::UpdateCurrent => PbNowMode::UpdateCurrent(PbNowModeUpdateCurrent {}),
                Mode::GenerateSeries {
                    start_timestamp,
                    interval,
                } => PbNowMode::GenerateSeries(PbNowModeGenerateSeries {
                    start_timestamp: Some(Datum::Some((*start_timestamp).into()).to_protobuf()),
                    interval: Some(Datum::Some((*interval).into()).to_protobuf()),
                }),
            }),
        }))
    }
}

impl ExprRewritable for StreamNow {}

impl ExprVisitable for StreamNow {}
