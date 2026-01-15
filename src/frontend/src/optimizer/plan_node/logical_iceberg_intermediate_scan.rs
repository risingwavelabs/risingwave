// Copyright 2026 RisingWave Labs
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
use iceberg::expr::Predicate;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, PlanBase, PredicatePushdown,
    ToBatch, ToStream, generic,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    ColumnPruningContext, LogicalFilter, LogicalProject, LogicalSource, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition, to_iceberg_predicate};

/// `LogicalIcebergIntermediateScan` is an intermediate plan node used during optimization
/// of Iceberg scans. It accumulates predicates and column pruning information before
/// being converted to the final `LogicalIcebergScan` with delete file anti-joins.
///
/// This node is introduced to reduce the number of Iceberg metadata reads. Instead of
/// reading metadata when creating `LogicalIcebergScan`, we defer the metadata read
/// until all optimizations (predicate pushdown, column pruning) are applied.
///
/// The optimization flow is:
/// 1. `LogicalSource` (iceberg) -> `LogicalIcebergIntermediateScan`
/// 2. Predicate pushdown and column pruning are applied to `LogicalIcebergIntermediateScan`
/// 3. `LogicalIcebergIntermediateScan` -> `LogicalIcebergScan` (with anti-joins for delete files)
#[derive(Debug, Clone, PartialEq, Educe)]
#[educe(Hash)]
pub struct LogicalIcebergIntermediateScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,
    #[educe(Hash(ignore))]
    pub predicate: Predicate,
    pub output_columns: Vec<String>,
    pub time_travel_info: IcebergTimeTravelInfo,
}

impl Eq for LogicalIcebergIntermediateScan {}

impl LogicalIcebergIntermediateScan {
    pub fn new(logical_source: &LogicalSource, time_travel: IcebergTimeTravelInfo) -> Self {
        assert!(logical_source.core.is_iceberg_connector());

        let core = logical_source.core.clone();
        let base = PlanBase::new_logical_with_core(&core);
        let output_column = core
            .column_catalog
            .iter()
            .map(|c| c.column_desc.name.clone())
            .collect();

        assert!(logical_source.output_exprs.is_none());

        LogicalIcebergIntermediateScan {
            base,
            core,
            predicate: Predicate::AlwaysTrue,
            output_columns: output_column,
            time_travel_info: time_travel,
        }
    }

    pub fn source_catalog(&self) -> Option<&SourceCatalog> {
        self.core.catalog.as_deref()
    }

    pub fn clone_with_predicate(&self, predicate: Predicate) -> Self {
        let new_predicate = self.predicate.clone().and(predicate);
        LogicalIcebergIntermediateScan {
            predicate: new_predicate,
            ..self.clone()
        }
    }

    pub fn clone_with_required_cols(&self, required_cols: &[usize]) -> Self {
        assert!(!required_cols.is_empty());

        let mut core = self.core.clone();
        let mut has_row_id = false;
        core.column_catalog = required_cols
            .iter()
            .map(|idx| {
                if Some(*idx) == core.row_id_index {
                    has_row_id = true;
                }
                core.column_catalog[*idx].clone()
            })
            .collect();
        if !has_row_id {
            core.row_id_index = None;
        }
        let base = PlanBase::new_logical_with_core(&core);

        let new_output_column = required_cols
            .iter()
            .map(|&i| self.output_columns[i].clone())
            .collect();

        LogicalIcebergIntermediateScan {
            base,
            core,
            predicate: self.predicate.clone(),
            output_columns: new_output_column,
            time_travel_info: self.time_travel_info.clone(),
        }
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalIcebergIntermediateScan }

impl Distill for LogicalIcebergIntermediateScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut fields = Vec::with_capacity(if verbose { 4 } else { 2 });

        if let Some(catalog) = self.source_catalog() {
            fields.push(("source", Pretty::from(catalog.name.clone())));
        } else {
            fields.push(("source", Pretty::from("unknown")));
        }
        fields.push(("columns", column_names_pretty(self.schema())));

        if verbose {
            fields.push(("predicate", Pretty::debug(&self.predicate)));
            fields.push(("output_columns", Pretty::debug(&self.output_columns)));
        }

        childless_record("LogicalIcebergIntermediateScan", fields)
    }
}

impl ColPrunable for LogicalIcebergIntermediateScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        if required_cols.is_empty() {
            // If required_cols is empty, we use the first column of iceberg to avoid the empty schema.
            LogicalProject::new(self.clone_with_required_cols(&[0]).into(), vec![]).into()
        } else {
            self.clone_with_required_cols(required_cols).into()
        }
    }
}

impl ExprRewritable<Logical> for LogicalIcebergIntermediateScan {}

impl ExprVisitable for LogicalIcebergIntermediateScan {}

impl PredicatePushdown for LogicalIcebergIntermediateScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let (iceberg_predicate, upper_conditions) =
            to_iceberg_predicate(predicate, self.schema().fields());
        let plan = self.clone_with_predicate(iceberg_predicate).into();
        if upper_conditions.always_true() {
            plan
        } else {
            LogicalFilter::create(plan, upper_conditions)
        }
    }
}

impl ToBatch for LogicalIcebergIntermediateScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        // This should not be called directly. The intermediate scan should be
        // converted to LogicalIcebergScan first via the materialization rule.
        Err(crate::error::ErrorCode::InternalError(
            "LogicalIcebergIntermediateScan should be converted to LogicalIcebergScan before to_batch".to_owned()
        )
        .into())
    }
}

impl ToStream for LogicalIcebergIntermediateScan {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        unreachable!("LogicalIcebergIntermediateScan is only for batch queries")
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!("LogicalIcebergIntermediateScan is only for batch queries")
    }
}
