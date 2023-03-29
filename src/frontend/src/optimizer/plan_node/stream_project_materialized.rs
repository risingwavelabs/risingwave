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
use itertools::Itertools;
use risingwave_common::catalog::{FieldDisplay, Schema};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::sort_util::OrderType;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::ProjectMaterializedNode;

use super::generic::GenericPlanRef;
use super::stream::StreamPlanRef;
use super::utils::TableCatalogBuilder;
use super::{ExprRewritable, LogicalProject, PlanBase, PlanRef, PlanTreeNodeUnary, StreamNode};
use crate::expr::{try_derive_watermark, Expr, ExprImpl, ExprRewriter};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::ColIndexMappingRewriteExt;

/// `StreamProjectMaterialized` implements [`super::LogicalProject`] to evaluate specified
/// expressions on input rows. and additionally, materializes the `materialize_columns`
/// which is a concatenation of the node's PK with the non-pk `forced_materialize_columns`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamProjectMaterialized {
    pub base: PlanBase,
    logical: LogicalProject,
    /// All the watermark derivations, (input_column_index, output_column_index). And the
    /// derivation expression is the project's expression itself.
    watermark_derivations: Vec<(usize, usize)>,
    /// Materialize columns will contain all the
    materialize_columns: Vec<usize>,
}

impl fmt::Display for StreamProjectMaterialized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("StreamProjectMaterialized");
        self.logical.fmt_fields_with_builder(&mut builder);
        if !self.watermark_derivations.is_empty() {
            builder.field(
                "output_watermarks",
                &self
                    .watermark_derivations
                    .iter()
                    .map(|(_, idx)| FieldDisplay(self.schema().fields.get(*idx).unwrap()))
                    .collect_vec(),
            );
        };
        builder.finish()
    }
}

impl StreamProjectMaterialized {
    pub fn new(logical: LogicalProject, forced_materialize_columns: Vec<usize>) -> Self {
        let ctx = logical.base.ctx.clone();
        let input = logical.input();
        let pk_indices = logical.base.logical_pk.to_vec();
        let schema = logical.schema().clone();

        let distribution = logical
            .i2o_col_mapping()
            .rewrite_provided_distribution(input.distribution());

        let mut watermark_derivations = vec![];
        let mut watermark_columns = FixedBitSet::with_capacity(schema.len());
        for (expr_idx, expr) in logical.exprs().iter().enumerate() {
            if let Some(input_idx) = try_derive_watermark(expr) {
                if input.watermark_columns().contains(input_idx) {
                    watermark_derivations.push((input_idx, expr_idx));
                    watermark_columns.insert(expr_idx);
                }
            }
        }
        let mut materialize_columns = pk_indices.clone();
        for idx in forced_materialize_columns {
            if !materialize_columns.contains(&idx) {
                materialize_columns.push(idx);
            }
        }
        // Project executor won't change the append-only behavior of the stream, so it depends on
        // input's `append_only`.
        let base = PlanBase::new_stream(
            ctx,
            schema,
            pk_indices,
            logical.functional_dependency().clone(),
            distribution,
            logical.input().append_only(),
            watermark_columns,
        );
        StreamProjectMaterialized {
            base,
            logical,
            watermark_derivations,
            materialize_columns,
        }
    }

    pub fn as_logical(&self) -> &LogicalProject {
        &self.logical
    }

    pub fn exprs(&self) -> &Vec<ExprImpl> {
        self.logical.exprs()
    }
}

impl PlanTreeNodeUnary for StreamProjectMaterialized {
    fn input(&self) -> PlanRef {
        self.logical.input()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(
            self.logical.clone_with_input(input),
            self.materialize_columns.clone(),
        )
    }
}
impl_plan_tree_node_for_unary! {StreamProjectMaterialized}

impl StreamNode for StreamProjectMaterialized {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        // This is an artificial schema: [logical_pk, non_pk_materialize_columns]
        let schema = Schema {
            fields: self
                .materialize_columns
                .iter()
                .map(|i| self.schema().fields[*i].clone())
                .collect(),
        };
        let mut index = vec![None; self.schema().len()];
        self.logical
            .logical_pk()
            .iter()
            .enumerate()
            .for_each(|(idx, pk)| {
                index[*pk] = Some(idx);
            });
        let col_to_schema_mapping = ColIndexMapping::new(index);
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(self.base.ctx().with_options().internal_table_subset());
        schema.fields().iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        (0..self.logical.logical_pk().len()).for_each(|i| {
            internal_table_catalog_builder.add_order_column(i, OrderType::ascending())
        });
        let dist_keys = self
            .base
            .distribution()
            .dist_column_indices()
            .iter()
            // Distribution keys must be contained in logical_pk
            .map(|i| col_to_schema_mapping.map(*i))
            .collect();

        let table_catalog = internal_table_catalog_builder
            .build(dist_keys, 0)
            .with_id(state.gen_table_id_wrapped());

        PbNodeBody::ProjectMaterialized(ProjectMaterializedNode {
            select_list: self
                .logical
                .exprs()
                .iter()
                .map(|x| x.to_expr_proto())
                .collect(),
            watermark_input_key: self
                .watermark_derivations
                .iter()
                .map(|(x, _)| *x as u32)
                .collect(),
            watermark_output_key: self
                .watermark_derivations
                .iter()
                .map(|(_, y)| *y as u32)
                .collect(),
            materialize_columns_key: self.materialize_columns.iter().map(|y| *y as u32).collect(),
            table: Some(table_catalog.to_internal_table_prost()),
        })
    }
}

impl ExprRewritable for StreamProjectMaterialized {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        Self::new(
            self.logical
                .rewrite_exprs(r)
                .as_logical_project()
                .unwrap()
                .clone(),
            self.materialize_columns.clone(),
        )
        .into()
    }
}
