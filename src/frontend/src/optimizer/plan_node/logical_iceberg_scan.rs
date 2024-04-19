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

use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};

use super::generic::GenericPlanRef;
use super::utils::{childless_record, Distill};
use super::{
    generic, ColPrunable, ExprRewritable, Logical, LogicalProject, PlanBase, PlanRef,
    PredicatePushdown, ToBatch, ToStream,
};
use crate::catalog::source_catalog::SourceCatalog;
use crate::error::Result;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::utils::column_names_pretty;
use crate::optimizer::plan_node::{
    BatchIcebergScan, ColumnPruningContext, LogicalFilter, LogicalSource, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalIcebergScan` is only used by batch queries. At the beginning of the batch query optimization, `LogicalSource` with a iceberg property would be converted into a `LogicalIcebergScan`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalIcebergScan {
    pub base: PlanBase<Logical>,
    pub core: generic::Source,
}

impl LogicalIcebergScan {
    pub fn new(logical_source: &LogicalSource) -> Self {
        assert!(logical_source.core.is_iceberg_connector());

        let core = logical_source.core.clone();
        let base = PlanBase::new_logical_with_core(&core);

        assert!(logical_source.output_exprs.is_none());

        LogicalIcebergScan { base, core }
    }

    pub fn source_catalog(&self) -> Option<Rc<SourceCatalog>> {
        self.core.catalog.clone()
    }
}

impl_plan_tree_node_for_leaf! {LogicalIcebergScan}
impl Distill for LogicalIcebergScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let fields = if let Some(catalog) = self.source_catalog() {
            let src = Pretty::from(catalog.name.clone());
            vec![
                ("source", src),
                ("columns", column_names_pretty(self.schema())),
            ]
        } else {
            vec![]
        };
        childless_record("LogicalIcebergScan", fields)
    }
}

impl ColPrunable for LogicalIcebergScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        // TODO: support column pruning for iceberg scan
        let mapping = ColIndexMapping::with_remaining_columns(required_cols, self.schema().len());
        LogicalProject::with_mapping(self.clone().into(), mapping).into()
    }
}

impl ExprRewritable for LogicalIcebergScan {}

impl ExprVisitable for LogicalIcebergScan {}

impl PredicatePushdown for LogicalIcebergScan {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // No pushdown.
        LogicalFilter::create(self.clone().into(), predicate).into()
    }
}

impl ToBatch for LogicalIcebergScan {
    fn to_batch(&self) -> Result<PlanRef> {
        let plan: PlanRef = BatchIcebergScan::new(self.core.clone()).into();
        Ok(plan)
    }
}

impl ToStream for LogicalIcebergScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        unreachable!()
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        unreachable!()
    }
}
