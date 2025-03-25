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

use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::bail_not_implemented;
use risingwave_common::catalog::{ColumnDesc, TableDesc};

use super::generic::{GenericPlanNode, GenericPlanRef};
use super::utils::{Distill, childless_record};
use super::{
    BatchFilter, BatchProject, ColPrunable, ExprRewritable, Logical, PlanBase, PlanRef,
    PredicatePushdown, ToBatch, ToStream, generic,
};
use crate::error::Result;
use crate::expr::{CorrelatedInputRef, ExprImpl, ExprRewriter, ExprVisitor, InputRef};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::expr_visitable::ExprVisitable;
use crate::optimizer::plan_node::{
    BatchSysSeqScan, ColumnPruningContext, LogicalFilter, LogicalValues, PredicatePushdownContext,
    RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{Cardinality, Order};
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};

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
        table_name: String, // explain-only
        table_desc: Rc<TableDesc>,
        ctx: OptimizerContextRef,
        table_cardinality: Cardinality,
    ) -> Self {
        generic::SysScan::new(
            table_name,
            (0..table_desc.columns.len()).collect(),
            table_desc,
            ctx,
            Condition::true_cond(),
            table_cardinality,
        )
        .into()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    /// The cardinality of the table **without** applying the predicate.
    pub fn table_cardinality(&self) -> Cardinality {
        self.core.table_cardinality
    }

    /// Get a reference to the logical scan's table desc.
    pub fn table_desc(&self) -> &TableDesc {
        self.core.table_desc.as_ref()
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.core.column_descs()
    }

    /// Get the logical scan's filter predicate
    pub fn predicate(&self) -> &Condition {
        &self.core.predicate
    }

    /// a vec of `InputRef` corresponding to `output_col_idx`, which can represent a pulled project.
    fn output_idx_to_input_ref(&self) -> Vec<ExprImpl> {
        let output_idx = self
            .output_col_idx()
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                InputRef::new(i, self.table_desc().columns[col_idx].data_type.clone()).into()
            })
            .collect_vec();
        output_idx
    }

    /// Undo predicate push down when predicate in scan is not supported.
    pub fn predicate_pull_up(&self) -> (generic::SysScan, Condition, Option<Vec<ExprImpl>>) {
        let mut predicate = self.predicate().clone();
        if predicate.always_true() {
            return (self.core.clone(), Condition::true_cond(), None);
        }

        let mut inverse_mapping = {
            let mapping = ColIndexMapping::new(
                self.required_col_idx().iter().map(|i| Some(*i)).collect(),
                self.table_desc().columns.len(),
            );
            // Since `required_col_idx` mapping is not invertible, we need to inverse manually.
            let mut inverse_map = vec![None; mapping.target_size()];
            for (src, dst) in mapping.mapping_pairs() {
                inverse_map[dst] = Some(src);
            }
            ColIndexMapping::new(inverse_map, mapping.source_size())
        };

        predicate = predicate.rewrite_expr(&mut inverse_mapping);

        let scan_without_predicate = generic::SysScan::new(
            self.table_name().to_owned(),
            self.required_col_idx().to_vec(),
            self.core.table_desc.clone(),
            self.ctx(),
            Condition::true_cond(),
            self.table_cardinality(),
        );
        let project_expr = if self.required_col_idx() != self.output_col_idx() {
            Some(self.output_idx_to_input_ref())
        } else {
            None
        };
        (scan_without_predicate, predicate, project_expr)
    }

    fn clone_with_predicate(&self, predicate: Condition) -> Self {
        generic::SysScan::new_inner(
            self.table_name().to_owned(),
            self.output_col_idx().to_vec(),
            self.core.table_desc.clone(),
            self.base.ctx().clone(),
            predicate,
            self.table_cardinality(),
        )
        .into()
    }

    pub fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        generic::SysScan::new_inner(
            self.table_name().to_owned(),
            output_col_idx,
            self.core.table_desc.clone(),
            self.base.ctx().clone(),
            self.predicate().clone(),
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

impl_plan_tree_node_for_leaf! {LogicalSysScan}

impl Distill for LogicalSysScan {
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
                            let col_name = &self.table_desc().columns[*i].name;
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

        childless_record("LogicalSysScan", vec)
    }
}

impl ColPrunable for LogicalSysScan {
    fn prune_col(&self, required_cols: &[usize], _ctx: &mut ColumnPruningContext) -> PlanRef {
        let output_col_idx: Vec<usize> = required_cols
            .iter()
            .map(|i| self.required_col_idx()[*i])
            .collect();
        assert!(
            output_col_idx
                .iter()
                .all(|i| self.output_col_idx().contains(i))
        );

        self.clone_with_output_indices(output_col_idx).into()
    }
}

impl ExprRewritable for LogicalSysScan {
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

impl ExprVisitable for LogicalSysScan {
    fn visit_exprs(&self, v: &mut dyn ExprVisitor) {
        self.core.visit_exprs(v);
    }
}

impl PredicatePushdown for LogicalSysScan {
    // TODO(kwannoel): Unify this with logical_scan.
    fn predicate_pushdown(
        &self,
        mut predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // If the predicate contains `CorrelatedInputRef` or `now()`. We don't push down.
        // This case could come from the predicate push down before the subquery unnesting.
        struct HasCorrelated {
            has: bool,
        }
        impl ExprVisitor for HasCorrelated {
            fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) {
                self.has = true;
            }
        }
        let non_pushable_predicate: Vec<_> = predicate
            .conjunctions
            .extract_if(.., |expr| {
                if expr.count_nows() > 0 {
                    true
                } else {
                    let mut visitor = HasCorrelated { has: false };
                    visitor.visit_expr(expr);
                    visitor.has
                }
            })
            .collect();
        let predicate = predicate.rewrite_expr(&mut ColIndexMapping::new(
            self.output_col_idx().iter().map(|i| Some(*i)).collect(),
            self.table_desc().columns.len(),
        ));
        if non_pushable_predicate.is_empty() {
            self.clone_with_predicate(predicate.and(self.predicate().clone()))
                .into()
        } else {
            LogicalFilter::create(
                self.clone_with_predicate(predicate.and(self.predicate().clone()))
                    .into(),
                Condition {
                    conjunctions: non_pushable_predicate,
                },
            )
        }
    }
}

impl LogicalSysScan {
    // TODO(kwannoel): Unify this with logical_scan.
    fn to_batch_inner_with_required(&self, required_order: &Order) -> Result<PlanRef> {
        if self.predicate().always_true() {
            required_order
                .enforce_if_not_satisfies(BatchSysSeqScan::new(self.core.clone(), vec![]).into())
        } else {
            let (scan_ranges, predicate) = self.predicate().clone().split_to_scan_ranges(
                self.core.table_desc.clone(),
                self.base.ctx().session_ctx().config().max_split_range_gap() as u64,
            )?;
            let mut scan = self.clone();
            scan.core.predicate = predicate; // We want to keep `required_col_idx` unchanged, so do not call `clone_with_predicate`.

            let plan: PlanRef = if scan.core.predicate.always_false() {
                LogicalValues::create(vec![], scan.core.schema(), scan.core.ctx).to_batch()?
            } else {
                let (scan, predicate, project_expr) = scan.predicate_pull_up();

                let mut plan: PlanRef = BatchSysSeqScan::new(scan, scan_ranges).into();
                if !predicate.always_true() {
                    plan = BatchFilter::new(generic::Filter::new(predicate, plan)).into();
                }
                if let Some(exprs) = project_expr {
                    plan = BatchProject::new(generic::Project::new(exprs, plan)).into()
                }
                plan
            };

            assert_eq!(plan.schema(), self.schema());
            required_order.enforce_if_not_satisfies(plan)
        }
    }
}

impl ToBatch for LogicalSysScan {
    fn to_batch(&self) -> Result<PlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let new = self.clone_with_predicate(self.predicate().clone());
        new.to_batch_inner_with_required(required_order)
    }
}

impl ToStream for LogicalSysScan {
    fn to_stream(&self, _ctx: &mut ToStreamContext) -> Result<PlanRef> {
        bail_not_implemented!("streaming on system table");
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        bail_not_implemented!("streaming on system table");
    }
}
