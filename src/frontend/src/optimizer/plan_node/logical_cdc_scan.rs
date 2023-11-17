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

use std::rc::Rc;

use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{CdcTableDesc, ColumnDesc};
use risingwave_common::error::Result;

use super::generic::GenericPlanRef;
use super::utils::{childless_record, Distill};
use super::{
    generic, ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef, PredicatePushdown, ToBatch,
    ToStream,
};
use crate::catalog::ColumnId;
use crate::expr::ExprRewriter;
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::{
    ColumnPruningContext, PredicatePushdownContext, RewriteStreamContext, StreamCdcTableScan,
    ToStreamContext,
};
use crate::optimizer::property::{Cardinality, Order};
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

/// `LogicalCdcScan` returns contents of a table or other equivalent object
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
    ) -> Self {
        generic::CdcScan::new(
            table_name,
            (0..cdc_table_desc.columns.len()).collect(),
            cdc_table_desc,
            ctx,
        )
        .into()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn for_system_time_as_of_proctime(&self) -> bool {
        self.core.for_system_time_as_of_proctime
    }

    /// The cardinality of the table **without** applying the predicate.
    pub fn table_cardinality(&self) -> Cardinality {
        self.core.table_cardinality
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

    /// Get the logical scan's filter predicate
    pub fn predicate(&self) -> &Condition {
        &self.core.predicate
    }

    pub fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        generic::CdcScan::new_inner(
            self.table_name().to_string(),
            output_col_idx,
            self.core.table_desc.clone(),
            self.core.cdc_table_desc.clone(),
            vec![],
            self.base.ctx().clone(),
            self.predicate().clone(),
            self.for_system_time_as_of_proctime(),
            self.table_cardinality(),
        )
        .into()
    }

    pub fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }

    pub fn required_col_idx(&self) -> &Vec<usize> {
        &self.core.required_col_idx
    }
}

impl_plan_tree_node_for_leaf! {LogicalCdcScan}

impl Distill for LogicalCdcScan {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let verbose = self.base.ctx().is_explain_verbose();
        let mut vec = Vec::with_capacity(5);
        vec.push(("table", Pretty::from(self.table_name().to_owned())));
        let key_is_columns =
            self.predicate().always_true() || self.output_col_idx() == self.required_col_idx();
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
                    self.required_col_idx()
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

        if !self.predicate().always_true() {
            let input_schema = self.core.fields_pretty_schema();
            vec.push((
                "predicate",
                Pretty::display(&ConditionDisplay {
                    condition: self.predicate(),
                    input_schema: &input_schema,
                }),
            ))
        }

        if self.table_cardinality() != Cardinality::unknown() {
            vec.push(("cardinality", Pretty::display(&self.table_cardinality())));
        }

        childless_record("LogicalCdcScan", vec)
    }
}

impl ColPrunable for LogicalCdcScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let output_col_idx: Vec<usize> = required_cols
            .iter()
            .map(|i| self.required_col_idx()[*i])
            .collect();
        assert!(output_col_idx
            .iter()
            .all(|i| self.output_col_idx().contains(i)));

        self.clone_with_output_indices(output_col_idx).into()
    }
}

impl ExprRewritable for LogicalCdcScan {
    fn has_rewritable_expr(&self) -> bool {
        true
    }

    fn rewrite_exprs(&self, r: &mut dyn ExprRewriter) -> PlanRef {
        let mut core = self.core.clone();
        core.rewrite_exprs(r);
        Self {
            base: self.base.clone_with_new_plan_id(),
            core,
        }
        .into()
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
