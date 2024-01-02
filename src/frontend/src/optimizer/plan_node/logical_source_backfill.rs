// Copyright 2024 RisingWave Labs
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

use std::ops::Bound;
use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::parser::additional_columns::add_partition_offset_cols;

use super::generic::{GenericPlanRef, SourceNodeKind};
use super::stream_watermark_filter::StreamWatermarkFilter;
use super::utils::{childless_record, Distill};
use super::{
    generic, BatchProject, BatchSource, ColPrunable, ExprRewritable, Logical, LogicalFilter,
    LogicalProject, LogicalSource, PlanBase, PlanRef, PredicatePushdown, StreamProject,
    StreamRowIdGen, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::expr::{ExprImpl, ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamSourceBackfill,
    ToStreamContext,
};
use crate::optimizer::property::Distribution::HashShard;
use crate::utils::{ColIndexMapping, Condition};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalSourceBackfill {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,

    /// Expressions to output. This field presents and will be turned to a `Project` when
    /// converting to a physical plan, only if there are generated columns.
    output_exprs: Option<Vec<ExprImpl>>,
    /// When there are generated columns, the `StreamRowIdGen`'s row_id_index is different from
    /// the one in `core`. So we store the one in `output_exprs` here.
    output_row_id_index: Option<usize>,
}

impl LogicalSourceBackfill {
    pub fn new(source_catalog: Rc<SourceCatalog>, ctx: OptimizerContextRef) -> Result<Self> {
        let mut column_catalog = source_catalog.columns.clone();
        let row_id_index = source_catalog.row_id_index;

        let kafka_timestamp_range = (Bound::Unbounded, Bound::Unbounded);

        let (columns_exist, additional_columns) =
            add_partition_offset_cols(&column_catalog, &source_catalog.connector_name());
        for (existed, mut c) in columns_exist.into_iter().zip_eq_fast(additional_columns) {
            c.is_hidden = true;
            if !existed {
                column_catalog.push(c);
            }
        }
        let core = generic::Source {
            catalog: Some(source_catalog),
            column_catalog,
            row_id_index,
            // FIXME: this field is not useful for backfill.
            kind: SourceNodeKind::CreateMViewOrBatch,
            ctx,
            kafka_timestamp_range,
        };

        let base = PlanBase::new_logical_with_core(&core);

        let output_exprs =
            LogicalSource::derive_output_exprs_from_generated_columns(&core.column_catalog)?;
        let (core, output_row_id_index) = core.exclude_generated_columns();

        Ok(LogicalSourceBackfill {
            base,
            core,
            output_exprs,
            output_row_id_index,
        })
    }

    pub fn source_catalog(&self) -> Rc<SourceCatalog> {
        self.core
            .catalog
            .clone()
            .expect("source catalog should exist for LogicalSourceBackfill")
    }
}

impl_plan_tree_node_for_leaf! {LogicalSourceBackfill}
impl Distill for LogicalSourceBackfill {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let src = Pretty::from(self.source_catalog().name.clone());
        let time = Pretty::debug(&self.core.kafka_timestamp_range);
        let fields = vec![
            ("source", src),
            ("columns", column_names_pretty(self.schema())),
            ("time_range", time),
        ];

        childless_record("LogicalSourceBackfill", fields)
    }
}

impl ColPrunable for LogicalSourceBackfill {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl ExprRewritable for LogicalSourceBackfill {
    fn has_rewritable_expr(&self) -> bool {
        self.output_exprs.is_some()
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut output_exprs = self.output_exprs.clone();

        for expr in output_exprs.iter_mut().flatten() {
            *expr = r.rewrite_expr(expr.clone());
        }

        Self {
            output_exprs,
            ..self.clone()
        }
        .into()
    }
}

impl ExprVisitable for LogicalSourceBackfill {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.output_exprs
            .iter()
            .flatten()
            .for_each(|e| v.visit_expr(e));
    }
}

impl PredicatePushdown for LogicalSourceBackfill {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalSourceBackfill {
    fn to_batch(&self) -> Result<PlanRef> {
        let mut plan: PlanRef = BatchSource::new(self.core.clone()).into();

        if let Some(exprs) = &self.output_exprs {
            let logical_project = generic::Project::new(exprs.to_vec(), plan);
            plan = BatchProject::new(logical_project).into();
        }

        Ok(plan)
    }
}

impl ToStream for LogicalSourceBackfill {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let mut plan = StreamSourceBackfill::new(self.core.clone()).into();

        if let Some(exprs) = &self.output_exprs {
            let logical_project = generic::Project::new(exprs.to_vec(), plan);
            plan = StreamProject::new(logical_project).into();
        }

        let catalog = self.source_catalog();
        if !catalog.watermark_descs.is_empty() {
            plan = StreamWatermarkFilter::new(plan, catalog.watermark_descs.clone()).into();
        }

        if let Some(row_id_index) = self.output_row_id_index {
            plan = StreamRowIdGen::new_with_dist(plan, row_id_index, HashShard(vec![row_id_index]))
                .into();
        }
        Ok(plan)
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        Ok((
            self.clone().into(),
            ColIndexMapping::identity(self.schema().len()),
        ))
    }
}
