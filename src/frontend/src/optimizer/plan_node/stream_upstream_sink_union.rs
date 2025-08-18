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

use educe::Educe;
use pretty_xmlish::XmlNode;
use risingwave_common::catalog::Schema;
use risingwave_pb::stream_plan::UpstreamSinkUnionNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::stream::prelude::*;
use crate::OptimizerContextRef;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{ExprRewritable, PlanBase, Stream, StreamNode};
use crate::optimizer::property::{
    Distribution, FunctionalDependencySet, MonotonicityDerivation, MonotonicityMap,
    WatermarkColumns, analyze_monotonicity,
};
use crate::stream_fragmenter::BuildFragmentGraphState;

#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
struct UpstreamSinkUnionInner {
    #[educe(PartialEq(ignore), Hash(ignore))]
    ctx: OptimizerContextRef,
    schema: Schema,
    append_only: bool,
    // `generated_column` is used to generate the `watermark_columns` field and affects the derivation of subsequent
    // operators. Since the project operator of `generated_column` is actually on the sink-fragment, not on the table,
    // only expr can be retained here to determine the non-decreasing column.
    generated_column_exprs: Option<Vec<ExprImpl>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamUpstreamSinkUnion {
    pub base: PlanBase<Stream>,
    inner: UpstreamSinkUnionInner,
}

impl StreamUpstreamSinkUnion {
    pub fn new(
        ctx: OptimizerContextRef,
        schema: &Schema,
        append_only: bool,
        generated_column_exprs: Option<Vec<ExprImpl>>,
    ) -> Self {
        let inner = UpstreamSinkUnionInner {
            ctx,
            schema: schema.clone(),
            append_only,
            generated_column_exprs,
        };

        Self::new_inner(inner)
    }

    fn new_inner(inner: UpstreamSinkUnionInner) -> Self {
        let mut out_watermark_columns = WatermarkColumns::new();
        let mut out_monotonicity_map = MonotonicityMap::new();
        if let Some(ref generated_column_exprs) = inner.generated_column_exprs {
            for (expr_idx, expr) in generated_column_exprs.iter().enumerate() {
                if let MonotonicityDerivation::Inherent(monotonicity) = analyze_monotonicity(expr) {
                    out_monotonicity_map.insert(expr_idx, monotonicity);
                    if monotonicity.is_non_decreasing() && !monotonicity.is_constant() {
                        out_watermark_columns.insert(expr_idx, inner.ctx.next_watermark_group_id());
                    }
                }
            }
        }

        // TODO(zyx): Maybe some fields are incorrect.
        let base = PlanBase::new_stream(
            inner.ctx.clone(),
            inner.schema.clone(),
            Some(vec![]), // stream_key
            FunctionalDependencySet::new(inner.schema.fields().len()),
            Distribution::SomeShard,
            if inner.append_only {
                StreamKind::AppendOnly
            } else {
                StreamKind::Retract
            },
            false, // emit_on_window_close
            out_watermark_columns,
            out_monotonicity_map,
        );

        Self { base, inner }
    }
}

impl Distill for StreamUpstreamSinkUnion {
    fn distill<'a>(&self) -> XmlNode<'a> {
        childless_record("StreamUpstreamSinkUnion", vec![])
    }
}

impl_plan_tree_node_for_leaf! { Stream, StreamUpstreamSinkUnion }

impl StreamNode for StreamUpstreamSinkUnion {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::UpstreamSinkUnion(Box::new(UpstreamSinkUnionNode {
            init_upstreams: vec![],
        }))
    }
}

impl ExprRewritable<Stream> for StreamUpstreamSinkUnion {
    fn has_rewritable_expr(&self) -> bool {
        self.inner.generated_column_exprs.is_some()
    }

    fn rewrite_exprs(&self, _r: &mut dyn crate::expr::ExprRewriter) -> super::PlanRef<Stream> {
        let mut inner = self.inner.clone();
        inner.generated_column_exprs = inner.generated_column_exprs.map(|exprs| {
            exprs
                .into_iter()
                .map(|expr| _r.rewrite_expr(expr))
                .collect()
        });
        Self::new_inner(inner).into()
    }
}

impl ExprVisitable for StreamUpstreamSinkUnion {
    fn visit_exprs(&self, v: &mut dyn crate::expr::ExprVisitor) {
        if let Some(exprs) = self.inner.generated_column_exprs.as_ref() {
            exprs.iter().for_each(|expr| {
                v.visit_expr(expr);
            });
        }
    }
}
