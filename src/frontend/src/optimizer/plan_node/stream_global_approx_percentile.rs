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
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::GlobalApproxPercentileNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use crate::PlanRef;
use crate::expr::{ExprRewriter, ExprVisitor, Literal};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::utils::{Distill, TableCatalogBuilder, childless_record};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanAggCall, PlanBase, PlanTreeNodeUnary, Stream, StreamNode,
};
use crate::optimizer::property::{Distribution, FunctionalDependencySet, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamGlobalApproxPercentile {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    /// Quantile
    quantile: Literal,
    /// Used to compute the exponent bucket base.
    relative_error: Literal,
}

impl StreamGlobalApproxPercentile {
    pub fn new(input: PlanRef, approx_percentile_agg_call: &PlanAggCall) -> Self {
        let schema = Schema::new(vec![Field::with_name(
            DataType::Float64,
            "approx_percentile",
        )]);
        let functional_dependency = FunctionalDependencySet::with_key(1, &[]);
        let watermark_columns = WatermarkColumns::new();
        let base = PlanBase::new_stream(
            input.ctx(),
            schema,
            Some(vec![]),
            functional_dependency,
            Distribution::Single,
            input.append_only(),
            input.emit_on_window_close(),
            watermark_columns,
            input.columns_monotonicity().clone(),
        );
        Self {
            base,
            input,
            quantile: approx_percentile_agg_call.direct_args[0].clone(),
            relative_error: approx_percentile_agg_call.direct_args[1].clone(),
        }
    }
}

impl Distill for StreamGlobalApproxPercentile {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let out = vec![
            ("quantile", Pretty::debug(&self.quantile)),
            ("relative_error", Pretty::debug(&self.relative_error)),
        ];
        childless_record("StreamGlobalApproxPercentile", out)
    }
}

impl PlanTreeNodeUnary for StreamGlobalApproxPercentile {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self {
            base: self.base.clone(),
            input,
            quantile: self.quantile.clone(),
            relative_error: self.relative_error.clone(),
        }
    }
}

impl_plan_tree_node_for_unary! {StreamGlobalApproxPercentile}

impl StreamNode for StreamGlobalApproxPercentile {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let relative_error = self.relative_error.get_data().as_ref().unwrap();
        let relative_error = relative_error.as_float64().into_inner();
        let base = (1.0 + relative_error) / (1.0 - relative_error);
        let quantile = self.quantile.get_data().as_ref().unwrap();
        let quantile = quantile.as_float64().into_inner();

        // setup table: bucket_id->count
        let mut bucket_table_builder = TableCatalogBuilder::default();
        bucket_table_builder.add_column(&Field::with_name(DataType::Int16, "sign"));
        bucket_table_builder.add_column(&Field::with_name(DataType::Int32, "bucket_id"));
        bucket_table_builder.add_column(&Field::with_name(DataType::Int64, "count"));
        bucket_table_builder.add_order_column(0, OrderType::ascending()); // sign
        bucket_table_builder.add_order_column(1, OrderType::ascending()); // bucket_id

        // setup table: total_count
        let mut count_table_builder = TableCatalogBuilder::default();
        count_table_builder.add_column(&Field::with_name(DataType::Int64, "total_count"));

        let body = GlobalApproxPercentileNode {
            base,
            quantile,
            bucket_state_table: Some(
                bucket_table_builder
                    .build(vec![], 0)
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            count_state_table: Some(
                count_table_builder
                    .build(vec![], 0)
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
        };
        PbNodeBody::GlobalApproxPercentile(Box::new(body))
    }
}

impl ExprRewritable for StreamGlobalApproxPercentile {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) -> PlanRef {
        unimplemented!()
    }
}

impl ExprVisitable for StreamGlobalApproxPercentile {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {}
}
