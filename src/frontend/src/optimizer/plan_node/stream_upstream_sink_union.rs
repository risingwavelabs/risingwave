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
use crate::optimizer::plan_node::utils::{Distill, childless_record, watermark_pretty};
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
    stream_key: Option<Vec<usize>>,
    dist: Distribution,
    stream_kind: StreamKind,
    // `generated_column` is used to generate the `watermark_columns` field and affects the derivation of subsequent
    // operators. Since the project operator of `generated_column` is actually on the sink-fragment, not on the table,
    // only expr can be retained here to determine the `watermark_columns`.
    generated_column_exprs: Option<Vec<ExprImpl>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamUpstreamSinkUnion {
    pub base: PlanBase<Stream>,
    pub generated_column_exprs: Option<Vec<ExprImpl>>,
}

impl StreamUpstreamSinkUnion {
    pub fn new(
        ctx: OptimizerContextRef,
        schema: &Schema,
        stream_key: Option<&[usize]>,
        dist: Distribution,
        append_only: bool,
        user_defined_pk: bool,
        generated_column_exprs: Option<Vec<ExprImpl>>,
    ) -> Self {
        // For upstream sink creating, we require that if the table doesn't define pk or the table is `append_only`, the
        // upstream sink must be `append_only`.
        let stream_kind = if append_only || !user_defined_pk {
            StreamKind::AppendOnly
        } else {
            StreamKind::Upsert
        };
        let inner = UpstreamSinkUnionInner {
            ctx,
            schema: schema.clone(),
            stream_key: stream_key.map(|keys| keys.to_vec()),
            dist,
            stream_kind,
            generated_column_exprs,
        };

        Self::new_inner(inner)
    }

    fn new_inner(inner: UpstreamSinkUnionInner) -> Self {
        let mut out_watermark_columns = WatermarkColumns::new();
        let mut out_monotonicity_map = MonotonicityMap::new();
        // reference `StreamProject::new_inner()`
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

        let field_num = inner.schema.fields().len();
        let base = PlanBase::new_stream(
            inner.ctx,
            inner.schema,
            inner.stream_key, // maybe incorrect
            FunctionalDependencySet::new(field_num),
            inner.dist,
            inner.stream_kind,
            false, // emit_on_window_close
            out_watermark_columns,
            out_monotonicity_map,
        );

        Self {
            base,
            generated_column_exprs: inner.generated_column_exprs,
        }
    }

    fn rebuild_inner(&self) -> UpstreamSinkUnionInner {
        UpstreamSinkUnionInner {
            ctx: self.base.ctx(),
            schema: self.base.schema().clone(),
            stream_key: self.base.stream_key().map(|keys| keys.to_vec()),
            dist: self.base.distribution().clone(),
            stream_kind: self.base.stream_kind(),
            generated_column_exprs: self.generated_column_exprs.clone(),
        }
    }
}

impl Distill for StreamUpstreamSinkUnion {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::new();
        if verbose && let Some(ow) = watermark_pretty(self.watermark_columns(), self.schema()) {
            vec.push(("output_watermarks", ow));
        }
        childless_record("StreamUpstreamSinkUnion", vec)
    }
}

impl_plan_tree_node_for_leaf! { Stream, StreamUpstreamSinkUnion }

impl StreamNode for StreamUpstreamSinkUnion {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::UpstreamSinkUnion(Box::new(UpstreamSinkUnionNode {
            // When the table is created, there are no upstreams, so this is empty. When upstream sinks are created
            // later, the actual upstreams in executor will increase, and during recovery, it will be filled with the
            // actual upstream infos.
            init_upstreams: vec![],
        }))
    }
}

impl ExprRewritable<Stream> for StreamUpstreamSinkUnion {
    fn has_rewritable_expr(&self) -> bool {
        self.generated_column_exprs.is_some()
    }

    fn rewrite_exprs(&self, r: &mut dyn crate::expr::ExprRewriter) -> super::PlanRef<Stream> {
        let mut inner = self.rebuild_inner();
        inner.generated_column_exprs = inner
            .generated_column_exprs
            .map(|exprs| exprs.into_iter().map(|expr| r.rewrite_expr(expr)).collect());
        Self::new_inner(inner).into()
    }
}

impl ExprVisitable for StreamUpstreamSinkUnion {
    fn visit_exprs(&self, v: &mut dyn crate::expr::ExprVisitor) {
        if let Some(exprs) = self.generated_column_exprs.as_ref() {
            exprs.iter().for_each(|expr| {
                v.visit_expr(expr);
            });
        }
    }
}
