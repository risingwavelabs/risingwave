// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::rc::Rc;

use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::Result;

use super::generic::GenericPlanNode;
use super::{
    generic, BatchSource, ColPrunable, LogicalFilter, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, StreamDml, StreamRowIdGen, StreamSource, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::ColumnId;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::FunctionalDependencySet;
use crate::utils::{ColIndexMapping, Condition};
use crate::TableCatalog;

/// `LogicalSource` returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct LogicalSource {
    pub base: PlanBase,
    pub core: generic::Source,
}

impl LogicalSource {
    pub fn new(
        source_catalog: Option<Rc<SourceCatalog>>,
        column_descs: Vec<ColumnDesc>,
        pk_col_ids: Vec<ColumnId>,
        row_id_index: Option<usize>,
        enable_dml: bool,
        ctx: OptimizerContextRef,
    ) -> Self {
        let core = generic::Source {
            catalog: source_catalog,
            column_descs,
            pk_col_ids,
            row_id_index,
            enable_dml,
        };

        let schema = core.schema();
        let pk_indices = core.logical_pk();

        let (functional_dependency, pk_indices) = match pk_indices {
            Some(pk_indices) => (
                FunctionalDependencySet::with_key(schema.len(), &pk_indices),
                pk_indices,
            ),
            None => (FunctionalDependencySet::new(schema.len()), vec![]),
        };

        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);

        LogicalSource { base, core }
    }

    pub(super) fn column_names(&self) -> Vec<String> {
        self.schema()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }

    pub fn infer_internal_table_catalog(&self) -> TableCatalog {
        generic::Source::infer_internal_table_catalog(&self.base)
    }
}

impl_plan_tree_node_for_leaf! {LogicalSource}

impl fmt::Display for LogicalSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(catalog) = self.source_catalog() {
            write!(
                f,
                "LogicalSource {{ source: {}, columns: [{}] }}",
                catalog.name,
                self.column_names().join(", ")
            )
        } else {
            write!(f, "LogicalSource")
        }
    }
}

impl ColPrunable for LogicalSource {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl PredicatePushdown for LogicalSource {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalSource {
    fn to_batch(&self) -> Result<PlanRef> {
        Ok(BatchSource::new(self.clone()).into())
    }
}

impl ToStream for LogicalSource {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let mut plan: PlanRef = StreamSource::new(self.clone()).into();
        if self.core.enable_dml {
            plan = StreamDml::new(plan, self.core.column_descs.clone()).into();
        }
        if let Some(row_id_index) = self.core.row_id_index {
            plan = StreamRowIdGen::new(plan, row_id_index).into();
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
