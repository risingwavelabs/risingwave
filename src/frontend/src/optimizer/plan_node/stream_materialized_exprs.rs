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

use std::collections::BTreeMap;

use pretty_xmlish::Pretty;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::MaterializedExprsNode;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;

use super::expr_visitable::ExprVisitable;
use super::generic::{AliasedExpr, GenericPlanRef, PhysicalPlanRef};
use super::stream::StreamPlanRef;
use super::utils::{Distill, TableCatalogBuilder, childless_record, watermark_pretty};
use super::{ExprRewritable, PlanBase, PlanRef, PlanTreeNodeUnary, Stream, StreamNode};
use crate::catalog::TableCatalog;
use crate::expr::{Expr, ExprDisplay, ExprImpl, ExprRewriter, ExprVisitor, collect_input_refs};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt};

/// `StreamMaterializedExprs` materializes the results of a set of expressions.
/// The expressions are evaluated once and the results are stored in a state table,
/// avoiding re-evaluation for delete operations.
/// Particularly useful for expensive or non-deterministic expressions like UDF calls.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamMaterializedExprs {
    pub base: PlanBase<Stream>,
    input: PlanRef,
    exprs: Vec<ExprImpl>,
    /// Mapping from expr index to field name. May not contain all exprs.
    field_names: BTreeMap<usize, String>,
    state_clean_col_idx: Option<usize>,
}

impl Distill for StreamMaterializedExprs {
    fn distill<'a>(&self) -> pretty_xmlish::XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();

        let schema = self.schema();
        let mut vec = vec![{
            let f = |t| Pretty::debug(&t);
            let e = Pretty::Array(self.exprs_for_display(schema).iter().map(f).collect());
            ("exprs", e)
        }];
        if let Some(display_output_watermarks) = watermark_pretty(self.watermark_columns(), schema)
        {
            vec.push(("output_watermarks", display_output_watermarks));
        }
        if verbose && self.state_clean_col_idx.is_some() {
            vec.push((
                "state_clean_col_idx",
                Pretty::display(&self.state_clean_col_idx.unwrap()),
            ));
        }

        childless_record("StreamMaterializedExprs", vec)
    }
}

impl StreamMaterializedExprs {
    /// Creates a new `StreamMaterializedExprs` node.
    pub fn new(input: PlanRef, exprs: Vec<ExprImpl>, field_names: BTreeMap<usize, String>) -> Self {
        let input_watermark_cols = input.watermark_columns();

        // Determine if we have a watermark column for state cleaning
        let state_clean_col_idx = if !input_watermark_cols.is_empty() {
            let (idx, _) = input_watermark_cols
                .iter()
                .next()
                .expect("Expected at least one watermark column");
            Some(idx)
        } else {
            None
        };

        // Create a functional dependency set that includes dependencies from UDF inputs to outputs
        let input_len = input.schema().len();
        let output_len = input_len + exprs.len();

        // First, rewrite existing functional dependencies from input
        let mapping = ColIndexMapping::identity_or_none(input_len, output_len);
        let mut fd_set =
            mapping.rewrite_functional_dependency_set(input.functional_dependency().clone());

        // Then, add dependencies from UDF parameters to UDF outputs
        for (i, expr) in exprs.iter().enumerate() {
            let output_idx = input_len + i;

            // Create a dependency from all input references in the expression to the output
            let input_refs = collect_input_refs(input_len, std::iter::once(expr));
            let input_indices: Vec<_> = input_refs.ones().collect();

            if !input_indices.is_empty() {
                fd_set.add_functional_dependency_by_column_indices(&input_indices, &[output_idx]);
            }
        }

        let base = PlanBase::new_stream(
            input.ctx(),
            Self::derive_schema(&input, &exprs, &field_names),
            input.stream_key().map(|v| v.to_vec()),
            fd_set,
            input.distribution().clone(),
            input.append_only(),
            input.emit_on_window_close(),
            input.watermark_columns().clone(),
            input.columns_monotonicity().clone(),
        );

        Self {
            base,
            input,
            exprs,
            field_names,
            state_clean_col_idx,
        }
    }

    fn derive_schema(
        input: &PlanRef,
        exprs: &[ExprImpl],
        field_names: &BTreeMap<usize, String>,
    ) -> Schema {
        let ctx = input.ctx();
        let input_schema = input.schema();
        let mut all_fields = input_schema.fields.clone();
        all_fields.reserve(exprs.len());

        for (i, expr) in exprs.iter().enumerate() {
            let field_name = match field_names.get(&i) {
                Some(name) => name.clone(),
                None => format!("$expr{}", ctx.next_expr_display_id()),
            };
            let field = Field::with_name(expr.return_type(), field_name);
            all_fields.push(field);
        }

        Schema::new(all_fields)
    }

    fn exprs_for_display<'a>(&'a self, schema: &Schema) -> Vec<AliasedExpr<'a>> {
        let input_schema_len = self.input.schema().len();

        self.exprs
            .iter()
            .zip_eq_fast(schema.fields().iter().skip(input_schema_len))
            .map(|(expr, field)| AliasedExpr {
                expr: ExprDisplay {
                    expr,
                    input_schema: self.input.schema(),
                },
                alias: Some(field.name.clone()),
            })
            .collect()
    }

    /// Builds a state table catalog for `StreamMaterializedExprs`
    fn build_state_table(&self) -> TableCatalog {
        let mut catalog_builder = TableCatalogBuilder::default();
        let dist_keys = self.distribution().dist_column_indices().to_vec();

        // Add all columns
        self.schema().fields().iter().for_each(|field| {
            catalog_builder.add_column(field);
        });

        // Add table PK columns
        let mut pk_indices = self
            .input
            .stream_key()
            .expect("Expected stream key")
            .to_vec();
        let clean_wtmk_in_pk = if let Some(idx) = self.state_clean_col_idx {
            if let Some(pos) = pk_indices.iter().position(|&x| x == idx) {
                Some(pos)
            } else {
                pk_indices.push(idx);
                Some(pk_indices.len() - 1)
            }
        } else {
            None
        };

        pk_indices.iter().for_each(|idx| {
            catalog_builder.add_order_column(*idx, OrderType::ascending());
        });

        let read_prefix_len_hint = pk_indices.len();
        let mut catalog = catalog_builder.build(dist_keys, read_prefix_len_hint);

        if let Some(idx) = clean_wtmk_in_pk {
            catalog.clean_watermark_index_in_pk = Some(idx);
            catalog.cleaned_by_watermark = true;
        }

        catalog
    }
}

impl PlanTreeNodeUnary for StreamMaterializedExprs {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.exprs.clone(), self.field_names.clone())
    }
}
impl_plan_tree_node_for_unary! { StreamMaterializedExprs }

impl StreamNode for StreamMaterializedExprs {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        PbNodeBody::MaterializedExprs(Box::new(MaterializedExprsNode {
            exprs: self.exprs.iter().map(|expr| expr.to_expr_proto()).collect(),
            state_table: Some(
                self.build_state_table()
                    .with_id(state.gen_table_id_wrapped())
                    .to_internal_table_prost(),
            ),
            state_clean_col_idx: self.state_clean_col_idx.map(|idx| idx as u32),
        }))
    }
}

impl ExprRewritable for StreamMaterializedExprs {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let new_exprs = self
            .exprs
            .iter()
            .map(|e| r.rewrite_expr(e.clone()))
            .collect();
        Self::new(self.input.clone(), new_exprs, self.field_names.clone()).into()
    }
}

impl ExprVisitable for StreamMaterializedExprs {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.exprs.iter().for_each(|e| v.visit_expr(e));
    }
}
