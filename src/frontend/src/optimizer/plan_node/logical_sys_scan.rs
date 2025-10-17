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

use std::sync::Arc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::ColumnDesc;

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, LogicalPlanRef as PlanRef, PlanBase, PredicatePushdown,
    ToBatch, ToStream, generic,
};
use crate::catalog::system_catalog::SystemTableCatalog;
use crate::error::Result;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    BatchSysSeqScan, ColumnPruningContext, LogicalFilter, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::Cardinality;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalSysScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalSysScan {
    pub base: PlanBase<Logical>,
    core: generic::SysScan,
}

impl From<generic::SysScan> for LogicalSysScan {
    fn from(core: generic::SysScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl From<generic::SysScan> for PlanRef {
    fn from(core: generic::SysScan) -> Self {
        LogicalSysScan::from(core).into()
    }
}

impl LogicalSysScan {
    /// Create a [`LogicalSysScan`] node. Used by planner.
    pub fn create(
        table: Arc<SystemTableCatalog>,
        ctx: OptimizerContextRef,
        table_cardinality: Cardinality,
    ) -> Self {
        generic::SysScan::new(
            (0..table.columns.len()).collect(),
            table,
            ctx,
            table_cardinality,
        )
        .into()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table.name
    }

    /// The cardinality of the table **without** applying the predicate.
    pub fn table_cardinality(&self) -> Cardinality {
        self.core.table_cardinality
    }

    /// Get a reference to the logical scan's table desc.
    pub fn table(&self) -> &Arc<SystemTableCatalog> {
        &self.core.table
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.core.column_descs()
    }

    fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        generic::SysScan::new_inner(
            output_col_idx,
            self.table().clone(),
            self.base.ctx(),
            self.table_cardinality(),
        )
        .into()
    }

    fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }
}

impl_plan_tree_node_for_leaf! { Logical, LogicalSysScan}

impl Distill for LogicalSysScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(5);
        vec.push(("table", Pretty::from(self.table_name().to_owned())));
        vec.push(("output_columns", self.core.columns_pretty(verbose)));

        if self.table_cardinality() != Cardinality::unknown() {
            vec.push(("cardinality", Pretty::display(&self.table_cardinality())));
        }

        childless_record("LogicalSysScan", vec)
    }
}

impl ColPrunable for LogicalSysScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let output_col_idx: Vec<usize> = required_cols
            .iter()
            .map(|i| self.output_col_idx()[*i])
            .collect();
        assert!(
            output_col_idx
                .iter()
                .all(|i| self.output_col_idx().contains(i))
        );

        self.clone_with_output_indices(output_col_idx).into()
    }
}

impl ExprRewritable<Logical> for LogicalSysScan {}

impl ExprVisitable for LogicalSysScan {}

impl PredicatePushdown for LogicalSysScan {
    // TODO(kwannoel): Unify this with logical_scan.
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        LogicalFilter::create(self.clone().into(), predicate)
    }
}

impl ToBatch for LogicalSysScan {
    fn to_batch(&self) -> Result<crate::optimizer::plan_node::BatchPlanRef> {
        Ok(BatchSysSeqScan::new(self.core.clone()).into())
    }
}

impl ToStream for LogicalSysScan {
    fn to_stream(
        &self,
        _ctx: &mut ToStreamContext,
    ) -> Result<crate::optimizer::plan_node::StreamPlanRef> {
        bail_not_implemented!("streaming on system table");
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail_not_implemented!("streaming on system table");
    }
}
