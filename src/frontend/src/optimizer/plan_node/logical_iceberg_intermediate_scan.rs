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

use std::collections::HashMap;

use educe::Educe;
use iceberg::expr::Predicate;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::types::DataType;
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
use crate::utils::{
    ColIndexMapping, Condition, ExtractIcebergPredicateResult, extract_iceberg_predicate,
};

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
    // iceberg_predicate and risingwave_condition are same expression but in different forms. They will be added together during predicate pushdown.
    #[educe(Hash(ignore))]
    pub iceberg_predicate: Predicate,
    #[educe(Hash(ignore))]
    pub risingwave_condition: Condition,
    #[educe(Hash(ignore))]
    pub output_column_mapping: ColIndexMapping,
    pub time_travel_info: IcebergTimeTravelInfo,
    /// For Iceberg engine tables: maps source column name → target Hummock `DataType`.
    /// This remapping is applied to the output schema so that the intermediate scan's
    /// output types match the Hummock table types, avoiding unnecessary double casts
    /// when the storage selection rule rewrites to a Hummock `LogicalScan`.
    /// Empty for non-engine-table Iceberg sources.
    #[educe(Hash(ignore))]
    pub table_column_type_mapping: HashMap<String, DataType>,
}

impl Eq for LogicalIcebergIntermediateScan {}

impl LogicalIcebergIntermediateScan {
    pub fn new(
        logical_source: &LogicalSource,
        time_travel_info: IcebergTimeTravelInfo,
        table_column_type_mapping: HashMap<String, DataType>,
    ) -> Self {
        assert!(logical_source.core.is_iceberg_connector());

        let mut core = logical_source.core.clone();
        // Apply type remapping: change the source column types to Hummock table types
        // so that the output schema has Hummock types.
        for col in &mut core.column_catalog {
            if let Some(target_type) = table_column_type_mapping.get(col.name()) {
                col.column_desc.data_type = target_type.clone();
            }
        }
        let output_column_mapping = ColIndexMapping::identity(core.column_catalog.len());
        let base = PlanBase::new_logical_with_core(&core);
        assert!(logical_source.output_exprs.is_none());
        LogicalIcebergIntermediateScan {
            base,
            core,
            iceberg_predicate: Predicate::AlwaysTrue,
            risingwave_condition: Condition::true_cond(),
            output_column_mapping,
            time_travel_info,
            table_column_type_mapping,
        }
    }

    pub fn source_catalog(&self) -> Option<&SourceCatalog> {
        self.core.catalog.as_deref()
    }

    pub fn output_columns(&self) -> impl ExactSizeIterator<Item = &str> {
        self.core.column_catalog.iter().map(|c| c.name.as_str())
    }

    pub fn add_predicate(
        &self,
        iceberg_predicate: Predicate,
        extracted_condition: Condition,
    ) -> Self {
        let new_predicate = self.iceberg_predicate.clone().and(iceberg_predicate);
        let extracted_condition =
            extracted_condition.rewrite_expr(&mut self.output_column_mapping.clone());
        let new_condition = self.risingwave_condition.clone().and(extracted_condition);
        LogicalIcebergIntermediateScan {
            iceberg_predicate: new_predicate,
            risingwave_condition: new_condition,
            ..self.clone()
        }
    }

    /// Returns true if this intermediate scan has type remapping for Iceberg engine tables.
    pub fn has_type_mapping(&self) -> bool {
        !self.table_column_type_mapping.is_empty()
    }

    pub fn clone_with_required_cols(&self, required_cols: &[usize]) -> Self {
        assert!(!required_cols.is_empty());

        let mut core = self.core.clone();
        core.column_catalog = required_cols
            .iter()
            .map(|idx| core.column_catalog[*idx].clone())
            .collect();
        core.row_id_index = required_cols
            .iter()
            .position(|idx| Some(*idx) == self.core.row_id_index);

        let base = PlanBase::new_logical_with_core(&core);

        let map = required_cols
            .iter()
            .map(|&idx| Some(self.output_column_mapping.map(idx)))
            .collect();
        let new_output_column_mapping =
            ColIndexMapping::new(map, self.output_column_mapping.target_size());

        LogicalIcebergIntermediateScan {
            base,
            core,
            iceberg_predicate: self.iceberg_predicate.clone(),
            risingwave_condition: self.risingwave_condition.clone(),
            output_column_mapping: new_output_column_mapping,
            time_travel_info: self.time_travel_info.clone(),
            table_column_type_mapping: self.table_column_type_mapping.clone(),
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
            fields.push(("predicate", Pretty::debug(&self.iceberg_predicate)));
            fields.push((
                "output_column",
                Pretty::debug(&self.output_columns().collect_vec()),
            ));
            fields.push(("time_travel_info", Pretty::debug(&self.time_travel_info)));
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
        let ExtractIcebergPredicateResult {
            iceberg_predicate,
            extracted_condition,
            remaining_condition,
        } = extract_iceberg_predicate(predicate, self.schema().fields());
        let plan = self
            .add_predicate(iceberg_predicate, extracted_condition)
            .into();
        if remaining_condition.always_true() {
            plan
        } else {
            LogicalFilter::create(plan, remaining_condition)
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
