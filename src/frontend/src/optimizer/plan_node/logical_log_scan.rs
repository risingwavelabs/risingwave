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
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{CdcTableDesc, ColumnDesc, TableDesc};

use super::batch_log_seq_scan::BatchLogSeqScan;
use super::generic::GenericPlanRef;
use super::utils::{childless_record, Distill};
use super::{
    generic, ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef, PredicatePushdown, ToBatch,
    ToStream,
};
use crate::catalog::ColumnId;
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamCdcTableScan,
    ToStreamContext,
};
use crate::optimizer::property::Order;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalCdcScan` reads rows of a table from an external upstream database
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalLogScan {
    pub base: PlanBase<Logical>,
    core: generic::LogScan,
}

impl From<generic::LogScan> for LogicalLogScan {
    fn from(core: generic::LogScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl From<generic::LogScan> for PlanRef {
    fn from(core: generic::LogScan) -> Self {
        LogicalLogScan::from(core).into()
    }
}

impl LogicalLogScan {
    pub fn create(
        table_name: String, // explain-only
        table_desc: Rc<TableDesc>,
        ctx: OptimizerContextRef,
    ) -> Self {
        generic::LogScan::new(
            table_name,
            (0..table_desc.columns.len()).collect(),
            table_desc,
            ctx,
        )
        .into()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn cdc_table_desc(&self) -> &TableDesc {
        self.core.table_desc.as_ref()
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.core.column_descs()
    }

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.core.output_column_ids()
    }

    pub fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        generic::LogScan::new(
            self.table_name().to_string(),
            output_col_idx,
            self.core.table_desc.clone(),
            self.base.ctx().clone(),
        )
        .into()
    }

    pub fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }
}

impl_plan_tree_node_for_leaf! {LogicalLogScan}

impl Distill for LogicalLogScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(5);
        vec.push(("table", Pretty::from(self.table_name().to_owned())));
        let key_is_columns = true;
        let key = if key_is_columns {
            "columns"
        } else {
            "output_columns"
        };
        vec.push((key, self.core.columns_pretty(verbose)));
        if !key_is_columns {
            vec.push((
                "required_columns",
                Pretty::Array(
                    self.output_col_idx()
                        .iter()
                        .map(|i| {
                            let col_name = &self.cdc_table_desc().columns[*i].name;
                            Pretty::from(if verbose {
                                format!("{}.{}", self.table_name(), col_name)
                            } else {
                                col_name.to_string()
                            })
                        })
                        .collect(),
                ),
            ));
        }

        childless_record("LogicalCdcScan", vec)
    }
}

impl ColPrunable for LogicalLogScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let output_col_idx: Vec<usize> = required_cols
            .iter()
            .map(|i| self.output_col_idx()[*i])
            .collect();
        assert!(output_col_idx
            .iter()
            .all(|i| self.output_col_idx().contains(i)));

        self.clone_with_output_indices(output_col_idx).into()
    }
}

impl ExprRewritable for LogicalLogScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
    }
}

impl ExprVisitable for LogicalLogScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalLogScan {
    fn predicate_pushdown(
        &self,
        _predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        self.clone().into()
    }
}

impl ToBatch for LogicalLogScan {
    fn to_batch(&self) -> Result<PlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        required_order
            .enforce_if_not_satisfies(BatchLogSeqScan::new(self.core.clone(), vec![]).into())
    }
}

impl ToStream for LogicalLogScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        bail_not_implemented!("streaming on log table");
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail_not_implemented!("streaming on log table");
    }
}
