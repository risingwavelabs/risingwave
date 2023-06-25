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

use fixedbitset::FixedBitSet;
use pretty_xmlish::XmlNode;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::NowNode;

use super::generic::GenericPlanRef;
use super::stream::StreamPlanRef;
use super::utils::{childless_record, formatter_debug_plan_node, Distill, TableCatalogBuilder};
use super::{ExprRewritable, LogicalNow, PlanBase, StreamNode};
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::property::{Distribution, FunctionalDependencySet};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::OptimizerContextRef;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamNow {
    pub base: PlanBase,
}

impl StreamNow {
    pub fn new(_logical: LogicalNow, ctx: OptimizerContextRef) -> Self {
        let schema = Schema::new(vec![Field {
            data_type: DataType::Timestamptz,
            name: String::from("now"),
            sub_fields: vec![],
            type_name: String::default(),
        }]);
        let mut watermark_columns = FixedBitSet::with_capacity(1);
        watermark_columns.set(0, true);
        let base = PlanBase::new_stream(
            ctx,
            schema,
            vec![],
            FunctionalDependencySet::default(),
            Distribution::Single,
            false,
            false, // TODO(rc): derive EOWC property from input
            watermark_columns,
        );
        Self { base }
    }
}

impl Distill for StreamNow {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let vec = if self.base.ctx.is_explain_verbose() {
            vec![("output", column_names_pretty(self.schema()))]
        } else {
            vec![]
        };

        childless_record("StreamNow", vec)
    }
}
impl fmt::Display for StreamNow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let mut builder = formatter_debug_plan_node!(f, "StreamNow");

        if verbose {
            // For now, output all columns from the left side. Make it explicit here.
            builder.field("output", &self.schema().names_str());
        }

        builder.finish()
    }
}

impl_plan_tree_node_for_leaf! { StreamNow }

impl StreamNode for StreamNow {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> NodeBody {
        let schema = self.base.schema();
        let dist_keys = self.base.distribution().dist_column_indices().to_vec();
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(self.base.ctx().with_options().internal_table_subset());
        schema.fields().iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });

        let table_catalog = internal_table_catalog_builder
            .build(dist_keys, 0)
            .with_id(state.gen_table_id_wrapped());
        NodeBody::Now(NowNode {
            state_table: Some(table_catalog.to_internal_table_prost()),
        })
    }
}

impl ExprRewritable for StreamNow {}
