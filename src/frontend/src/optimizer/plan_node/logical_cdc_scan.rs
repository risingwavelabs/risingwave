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

use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{CdcTableDesc, ColumnDesc};

use super::generic::GenericPlanRef;
use super::utils::{Distill, childless_record};
use super::{
    ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef, PredicatePushdown, ToBatch, ToStream,
    generic,
};
use crate::catalog::ColumnId;
use crate::error::Result;
use crate::expr::{ExprRewriter, ExprVisitor};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::generic::CdcScanOptions;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamCdcTableScan,
    ToStreamContext,
};
use crate::optimizer::property::Order;
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalCdcScan` reads rows of a table from an external upstream database
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalCdcScan {
    pub base: PlanBase<Logical>,
    core: generic::CdcScan,
}

impl From<generic::CdcScan> for LogicalCdcScan {
    fn from(core: generic::CdcScan) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

impl From<generic::CdcScan> for PlanRef {
    fn from(core: generic::CdcScan) -> Self {
        LogicalCdcScan::from(core).into()
    }
}

impl LogicalCdcScan {
    pub fn create(
        table_name: String, // explain-only
        cdc_table_desc: Rc<CdcTableDesc>,
        ctx: OptimizerContextRef,
        options: CdcScanOptions,
    ) -> Self {
        generic::CdcScan::new(
            table_name,
            (0..cdc_table_desc.columns.len()).collect(),
            cdc_table_desc,
            ctx,
            options,
        )
        .into()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn cdc_table_desc(&self) -> &CdcTableDesc {
        self.core.cdc_table_desc.as_ref()
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
        generic::CdcScan::new(
            self.table_name().to_owned(),
            output_col_idx,
            self.core.cdc_table_desc.clone(),
            self.base.ctx().clone(),
            self.core.options.clone(),
        )
        .into()
    }

    pub fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }
}

impl_plan_tree_node_for_leaf! {LogicalCdcScan}

impl Distill for LogicalCdcScan {
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
                                col_name.clone()
                            })
                        })
                        .collect(),
                ),
            ));
        }

        childless_record("LogicalCdcScan", vec)
    }
}

impl ColPrunable for LogicalCdcScan {
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

impl ExprRewritable for LogicalCdcScan {
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

impl ExprVisitable for LogicalCdcScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalCdcScan {
    fn predicate_pushdown(
        &self,
        _predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        self.clone().into()
    }
}

impl ToBatch for LogicalCdcScan {
    fn to_batch(&self) -> Result<PlanRef> {
        unreachable!()
    }

    fn to_batch_with_order_required(&self, _required_order: &Order) -> Result<PlanRef> {
        unreachable!()
    }
}

impl ToStream for LogicalCdcScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        Ok(StreamCdcTableScan::new(self.core.clone()).into())
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
