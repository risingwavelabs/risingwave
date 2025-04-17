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

use anyhow::anyhow;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use crate::PlanRef;
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::{GenericPlanRef, PhysicalPlanRef};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::utils::{Distill, childless_record};
use crate::optimizer::plan_node::{
    ExprRewritable, PlanBase, PlanTreeNodeBinary, Stream, StreamNode,
};
use crate::optimizer::property::{FunctionalDependencySet, WatermarkColumns};
use crate::stream_fragmenter::BuildFragmentGraphState;

/// `StreamRowMerge` is used for merging two streams with the same stream key and distribution.
/// It will buffer the outputs from its input streams until we receive a barrier.
/// On receiving a barrier, it will `Project` their outputs according
/// to the provided `lhs_mapping` and `rhs_mapping`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamRowMerge {
    pub base: PlanBase<Stream>,
    pub lhs_input: PlanRef,
    pub rhs_input: PlanRef,
    /// Maps input from the lhs to the output.
    pub lhs_mapping: ColIndexMapping,
    /// Maps input from the rhs to the output.
    pub rhs_mapping: ColIndexMapping,
}

impl StreamRowMerge {
    pub fn new(
        lhs_input: PlanRef,
        rhs_input: PlanRef,
        lhs_mapping: ColIndexMapping,
        rhs_mapping: ColIndexMapping,
    ) -> Result<Self> {
        assert_eq!(lhs_mapping.target_size(), rhs_mapping.target_size());
        assert_eq!(lhs_input.distribution(), rhs_input.distribution());
        assert_eq!(lhs_input.stream_key(), rhs_input.stream_key());
        let functional_dependency =
            FunctionalDependencySet::with_key(lhs_mapping.target_size(), &[]);
        let mut schema_fields = Vec::with_capacity(lhs_mapping.target_size());
        let o2i_lhs = lhs_mapping
            .inverse()
            .ok_or_else(|| anyhow!("lhs_mapping should be invertible"))?;
        let o2i_rhs = rhs_mapping
            .inverse()
            .ok_or_else(|| anyhow!("rhs_mapping should be invertible"))?;
        for output_idx in 0..lhs_mapping.target_size() {
            if let Some(lhs_idx) = o2i_lhs.try_map(output_idx) {
                schema_fields.push(lhs_input.schema().fields()[lhs_idx].clone());
            } else if let Some(rhs_idx) = o2i_rhs.try_map(output_idx) {
                schema_fields.push(rhs_input.schema().fields()[rhs_idx].clone());
            } else {
                bail!(
                    "output index {} not found in either lhs or rhs mapping",
                    output_idx
                );
            }
        }
        let schema = Schema::new(schema_fields);
        assert!(!schema.is_empty());
        let watermark_columns = WatermarkColumns::new();

        let base = PlanBase::new_stream(
            lhs_input.ctx(),
            schema,
            lhs_input.stream_key().map(|k| k.to_vec()),
            functional_dependency,
            lhs_input.distribution().clone(),
            lhs_input.append_only(),
            lhs_input.emit_on_window_close(),
            watermark_columns,
            lhs_input.columns_monotonicity().clone(),
        );
        Ok(Self {
            base,
            lhs_input,
            rhs_input,
            lhs_mapping,
            rhs_mapping,
        })
    }
}

impl Distill for StreamRowMerge {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let mut out = Vec::with_capacity(1);

        if self.base.ctx().is_explain_verbose() {
            let f = |t| Pretty::debug(&t);
            let e = Pretty::Array(self.base.schema().fields().iter().map(f).collect());
            out = vec![("output", e)];
        }
        childless_record("StreamRowMerge", out)
    }
}

impl PlanTreeNodeBinary for StreamRowMerge {
    fn left(&self) -> PlanRef {
        self.lhs_input.clone()
    }

    fn right(&self) -> PlanRef {
        self.rhs_input.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self {
            base: self.base.clone(),
            lhs_input: left,
            rhs_input: right,
            lhs_mapping: self.lhs_mapping.clone(),
            rhs_mapping: self.rhs_mapping.clone(),
        }
    }
}

impl_plan_tree_node_for_binary! { StreamRowMerge }

impl StreamNode for StreamRowMerge {
    fn to_stream_prost_body(&self, _state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::RowMerge(Box::new(risingwave_pb::stream_plan::RowMergeNode {
            lhs_mapping: Some(self.lhs_mapping.to_protobuf()),
            rhs_mapping: Some(self.rhs_mapping.to_protobuf()),
        }))
    }
}

impl ExprRewritable for StreamRowMerge {
    fn has_rewritable_expr(&self) -> bool {
        false
    }

    fn rewrite_exprs(&self, _rewriter: &mut dyn ExprRewriter) -> PlanRef {
        unimplemented!()
    }
}

impl ExprVisitable for StreamRowMerge {
    fn visit_exprs(&self, _v: &mut dyn ExprVisitor) {}
}
