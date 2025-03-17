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
use risingwave_pb::stream_plan::LocalApproxPercentileNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use crate::PlanRef;
use crate::expr::{ExprRewriter, ExprVisitor, InputRef, InputRefDisplay, Literal};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanRef, PhysicalPlanRef};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::utils::{Distill, childless_record, watermark_pretty};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanAggCall, PlanBase, PlanTreeNodeUnary, Stream, StreamNode,
};
use crate::optimizer::property::{FunctionalDependencySet, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

// Does not contain `core` because no other plan nodes share
// common fields and schema, even GlobalApproxPercentile.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamLocalApproxPercentile {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    quantile: Literal,
    relative_error: Literal,
    percentile_col: InputRef,
}

impl StreamLocalApproxPercentile {
    pub fn new(input: PlanRef, approx_percentile_agg_call: &PlanAggCall) -> Self {
        let schema = Schema::new(vec![
            Field::with_name(DataType::Int16, "sign"),
            Field::with_name(DataType::Int32, "bucket_id"),
            Field::with_name(DataType::Int32, "count"),
        ]);
        // TODO(kwannoel): derive watermark columns?
        let watermark_columns = WatermarkColumns::new();
        let functional_dependency = FunctionalDependencySet::with_key(3, &[]);
        let base = PlanBase::new_stream(
            input.ctx(),
            schema,
            input.stream_key().map(|k| k.to_vec()),
            functional_dependency,
            input.distribution().clone(),
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
            percentile_col: approx_percentile_agg_call.inputs[0].clone(),
        }
    }
}

impl Distill for StreamLocalApproxPercentile {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut out = Vec::with_capacity(5);
        out.push((
            "percentile_col",
            Pretty::display(&InputRefDisplay {
                input_ref: &self.percentile_col,
                input_schema: self.input.schema(),
            }),
        ));
        out.push(("quantile", Pretty::debug(&self.quantile)));
        out.push(("relative_error", Pretty::debug(&self.relative_error)));
        if let Some(ow) = watermark_pretty(self.base.watermark_columns(), self.schema()) {
            out.push(("output_watermarks", ow));
        }
        childless_record("StreamLocalApproxPercentile", out)
    }
}

impl PlanTreeNodeUnary for StreamLocalApproxPercentile {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self {
            base: self.base.clone(),
            input,
            quantile: self.quantile.clone(),
            relative_error: self.relative_error.clone(),
            percentile_col: self.percentile_col.clone(),
        }
    }
}

impl_plan_tree_node_for_unary! {StreamLocalApproxPercentile}

impl StreamNode for StreamLocalApproxPercentile {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        let relative_error = self.relative_error.get_data().as_ref().unwrap();
        let relative_error = relative_error.as_float64().into_inner();
        let base = (1.0 + relative_error) / (1.0 - relative_error);
        let percentile_index = self.percentile_col.index() as u32;
        let body = LocalApproxPercentileNode {
            base,
            percentile_index,
        };
        PbNodeBody::LocalApproxPercentile(Box::new(body))
    }
}

impl ExprRewritable for StreamLocalApproxPercentile {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) -> PlanRef {
        unimplemented!()
    }
}

impl ExprVisitable for StreamLocalApproxPercentile {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {}
}
