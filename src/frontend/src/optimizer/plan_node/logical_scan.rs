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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::rc::Rc;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Field, Schema, TableDesc};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

use super::generic::{GenericPlanNode, GenericPlanRef};
use super::{
    generic, BatchFilter, BatchProject, ColPrunable, ExprRewritable, PlanBase, PlanRef,
    PredicatePushdown, StreamTableScan, ToBatch, ToStream,
};
use crate::catalog::{ColumnId, IndexCatalog};
use crate::expr::{
    CollectInputRef, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, ExprVisitor, InputRef,
};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::plan_node::{
    BatchSeqScan, ColumnPruningContext, LogicalFilter, LogicalProject, LogicalValues,
    PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{FunctionalDependencySet, Order};
use crate::optimizer::rule::IndexSelectionRule;
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition, ConditionDisplay};

/// `LogicalScan` returns contents of a table or other equivalent object
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalScan {
    pub base: PlanBase,
    core: generic::Scan,
}

impl LogicalScan {
    /// Create a `LogicalScan` node. Used internally by optimizer.
    pub(crate) fn new(
        table_name: String, // explain-only
        is_sys_table: bool,
        output_col_idx: Vec<usize>, // the column index in the table
        table_desc: Rc<TableDesc>,
        indexes: Vec<Rc<IndexCatalog>>,
        ctx: OptimizerContextRef,
        predicate: Condition, // refers to column indexes of the table
    ) -> Self {
        // here we have 3 concepts
        // 1. column_id: ColumnId, stored in catalog and a ID to access data from storage.
        // 2. table_idx: usize, column index in the TableDesc or tableCatalog.
        // 3. operator_idx: usize, column index in the ScanOperator's schema.
        // In a query we get the same version of catalog, so the mapping from column_id and
        // table_idx will not change. And the `required_col_idx` is the `table_idx` of the
        // required columns, i.e., the mapping from operator_idx to table_idx.

        let mut required_col_idx = output_col_idx.clone();
        let mut visitor =
            CollectInputRef::new(FixedBitSet::with_capacity(table_desc.columns.len()));
        predicate.visit_expr(&mut visitor);
        let predicate_col_idx: FixedBitSet = visitor.into();
        predicate_col_idx.ones().for_each(|idx| {
            if !required_col_idx.contains(&idx) {
                required_col_idx.push(idx);
            }
        });

        let core = generic::Scan {
            table_name,
            is_sys_table,
            required_col_idx,
            output_col_idx,
            table_desc,
            indexes,
            predicate,
            chunk_size: None,
        };

        let schema = core.schema();
        let pk_indices = core.logical_pk();

        let functional_dependency = match &pk_indices {
            Some(pk_indices) => FunctionalDependencySet::with_key(schema.len(), pk_indices),
            None => FunctionalDependencySet::new(schema.len()),
        };
        let base = PlanBase::new_logical(
            ctx,
            schema,
            pk_indices.unwrap_or_default(),
            functional_dependency,
        );

        Self { base, core }
    }

    /// Create a [`LogicalScan`] node. Used by planner.
    pub fn create(
        table_name: String, // explain-only
        is_sys_table: bool,
        table_desc: Rc<TableDesc>,
        indexes: Vec<Rc<IndexCatalog>>,
        ctx: OptimizerContextRef,
    ) -> Self {
        Self::new(
            table_name,
            is_sys_table,
            (0..table_desc.columns.len()).collect(),
            table_desc,
            indexes,
            ctx,
            Condition::true_cond(),
        )
    }

    pub(super) fn column_names(&self) -> Vec<String> {
        self.output_col_idx()
            .iter()
            .map(|i| self.table_desc().columns[*i].name.clone())
            .collect()
    }

    pub(super) fn column_names_with_table_prefix(&self) -> Vec<String> {
        self.output_col_idx()
            .iter()
            .map(|i| {
                format!(
                    "{}.{}",
                    self.table_name(),
                    self.table_desc().columns[*i].name
                )
            })
            .collect()
    }

    pub(super) fn order_names(&self) -> Vec<String> {
        self.table_desc()
            .order_column_indices()
            .iter()
            .map(|&i| self.table_desc().columns[i].name.clone())
            .collect()
    }

    pub(super) fn order_names_with_table_prefix(&self) -> Vec<String> {
        self.table_desc()
            .order_column_indices()
            .iter()
            .map(|&i| {
                format!(
                    "{}.{}",
                    self.table_name(),
                    self.table_desc().columns[i].name
                )
            })
            .collect()
    }

    pub fn table_name(&self) -> &str {
        &self.core.table_name
    }

    pub fn is_sys_table(&self) -> bool {
        self.core.is_sys_table
    }

    /// Get a reference to the logical scan's table desc.
    pub fn table_desc(&self) -> &TableDesc {
        self.core.table_desc.as_ref()
    }

    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.core.column_descs()
    }

    /// Get the ids of the output columns.
    pub fn output_column_ids(&self) -> Vec<ColumnId> {
        self.output_col_idx()
            .iter()
            .map(|i| self.table_desc().columns[*i].column_id)
            .collect()
    }

    pub fn output_column_indices(&self) -> &[usize] {
        &self.core.output_col_idx
    }

    /// Get all indexes on this table
    pub fn indexes(&self) -> &[Rc<IndexCatalog>] {
        &self.core.indexes
    }

    /// Get the logical scan's filter predicate
    pub fn predicate(&self) -> &Condition {
        &self.core.predicate
    }

    /// get the Mapping of columnIndex from internal column index to output column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        ColIndexMapping::with_remaining_columns(
            self.output_col_idx(),
            self.table_desc().columns.len(),
        )
    }

    /// Return indices of fields the output is ordered by and
    /// corresponding direction
    pub fn get_out_column_index_order(&self) -> Order {
        let id_to_tb_idx = self.table_desc().get_id_to_op_idx_mapping();
        let order = Order::new(
            self.table_desc()
                .pk
                .iter()
                .map(|order| {
                    let idx = id_to_tb_idx
                        .get(&self.table_desc().columns[order.column_index].column_id)
                        .unwrap();
                    ColumnOrder::new(*idx, order.order_type)
                })
                .collect(),
        );
        self.i2o_col_mapping().rewrite_provided_order(&order)
    }

    /// The mapped distribution key of the scan operator.
    ///
    /// The column indices in it is the position in the `output_col_idx`, instead of the position
    /// in all the columns of the table (which is the table's distribution key).
    ///
    /// Return `None` if the table's distribution key are not all in the `output_col_idx`.
    pub fn distribution_key(&self) -> Option<Vec<usize>> {
        let tb_idx_to_op_idx = self
            .output_col_idx()
            .iter()
            .enumerate()
            .map(|(op_idx, tb_idx)| (*tb_idx, op_idx))
            .collect::<HashMap<_, _>>();
        self.table_desc()
            .distribution_key
            .iter()
            .map(|&tb_idx| tb_idx_to_op_idx.get(&tb_idx).cloned())
            .collect()
    }

    pub fn watermark_columns(&self) -> FixedBitSet {
        let watermark_columns = &self.table_desc().watermark_columns;
        self.i2o_col_mapping().rewrite_bitset(watermark_columns)
    }

    pub fn to_index_scan(
        &self,
        index_name: &str,
        index_table_desc: Rc<TableDesc>,
        primary_to_secondary_mapping: &BTreeMap<usize, usize>,
    ) -> LogicalScan {
        let new_output_col_idx = self
            .output_col_idx()
            .iter()
            .map(|col_idx| *primary_to_secondary_mapping.get(col_idx).unwrap())
            .collect_vec();

        struct Rewriter<'a> {
            primary_to_secondary_mapping: &'a BTreeMap<usize, usize>,
        }
        impl ExprRewriter for Rewriter<'_> {
            fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
                InputRef::new(
                    *self
                        .primary_to_secondary_mapping
                        .get(&input_ref.index)
                        .unwrap(),
                    input_ref.return_type(),
                )
                .into()
            }
        }
        let mut rewriter = Rewriter {
            primary_to_secondary_mapping,
        };

        let new_predicate = self.predicate().clone().rewrite_expr(&mut rewriter);

        Self::new(
            index_name.to_string(),
            false,
            new_output_col_idx,
            index_table_desc,
            vec![],
            self.ctx(),
            new_predicate,
        )
    }

    /// used by optimizer (currently `top_n_on_index_rule`) to help reduce useless `chunk_size` at
    /// executor
    pub fn set_chunk_size(&mut self, chunk_size: u32) {
        self.core.chunk_size = Some(chunk_size);
    }

    pub fn chunk_size(&self) -> Option<u32> {
        self.core.chunk_size
    }

    pub fn primary_key(&self) -> Vec<ColumnOrder> {
        self.core.table_desc.pk.clone()
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
    pub fn predicate_pull_up(&self) -> (LogicalScan, Condition, Option<Vec<ExprImpl>>) {
        let mut predicate = self.predicate().clone();
        if predicate.always_true() {
            return (self.clone(), Condition::true_cond(), None);
        }

        let mut mapping =
            ColIndexMapping::new(self.required_col_idx().iter().map(|i| Some(*i)).collect())
                .inverse();
        predicate = predicate.rewrite_expr(&mut mapping);

        let scan_without_predicate = Self::new(
            self.table_name().to_string(),
            self.is_sys_table(),
            self.required_col_idx().to_vec(),
            self.core.table_desc.clone(),
            self.indexes().to_vec(),
            self.ctx(),
            Condition::true_cond(),
        );
        let project_expr = if self.required_col_idx() != self.output_col_idx() {
            Some(self.output_idx_to_input_ref())
        } else {
            None
        };
        (scan_without_predicate, predicate, project_expr)
    }

    fn clone_with_predicate(&self, predicate: Condition) -> Self {
        Self::new(
            self.table_name().to_string(),
            self.is_sys_table(),
            self.output_col_idx().to_vec(),
            self.core.table_desc.clone(),
            self.indexes().to_vec(),
            self.base.ctx.clone(),
            predicate,
        )
    }

    pub fn clone_with_output_indices(&self, output_col_idx: Vec<usize>) -> Self {
        Self::new(
            self.table_name().to_string(),
            self.is_sys_table(),
            output_col_idx,
            self.core.table_desc.clone(),
            self.indexes().to_vec(),
            self.base.ctx.clone(),
            self.predicate().clone(),
        )
    }

    pub fn output_col_idx(&self) -> &Vec<usize> {
        &self.core.output_col_idx
    }

    pub fn required_col_idx(&self) -> &Vec<usize> {
        &self.core.required_col_idx
    }
}

impl_plan_tree_node_for_leaf! {LogicalScan}

impl fmt::Display for LogicalScan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = self.base.ctx.is_explain_verbose();
        let output_col_names = if verbose {
            self.column_names_with_table_prefix()
        } else {
            self.column_names()
        }
        .join(", ");

        if self.predicate().always_true() {
            write!(
                f,
                "LogicalScan {{ table: {}, columns: [{}] }}",
                self.table_name(),
                output_col_names,
            )
        } else {
            write!(f, "LogicalScan {{ table: {}", self.table_name())?;
            if self.output_col_idx() == self.required_col_idx() {
                write!(f, ", columns: [{}]", output_col_names)?;
            } else {
                write!(
                    f,
                    ", output_columns: [{}], required_columns: [{}]",
                    output_col_names,
                    self.required_col_idx().iter().format_with(", ", |i, f| {
                        if verbose {
                            f(&format_args!(
                                "{}.{}",
                                self.table_name(),
                                self.table_desc().columns[*i].name
                            ))
                        } else {
                            f(&format_args!("{}", self.table_desc().columns[*i].name))
                        }
                    })
                )?;
            }

            let fields = self
                .table_desc()
                .columns
                .iter()
                .map(|col| Field::from_with_table_name_prefix(col, self.table_name()))
                .collect_vec();
            let input_schema = Schema { fields };
            write!(
                f,
                ", predicate: {} }}",
                ConditionDisplay {
                    condition: self.predicate(),
                    input_schema: &input_schema,
                }
            )
        }
    }
}

impl ColPrunable for LogicalScan {
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

impl ExprRewritable for LogicalScan {
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

impl PredicatePushdown for LogicalScan {
    fn predicate_pushdown(
        &self,
        mut predicate: Condition,
        _ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        // If the predicate contains `CorrelatedInputRef` or `now()`. We don't push down.
        // This case could come from the predicate push down before the subquery unnesting.
        struct HasCorrelated {}
        impl ExprVisitor<bool> for HasCorrelated {
            fn merge(a: bool, b: bool) -> bool {
                a | b
            }

            fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) -> bool {
                true
            }
        }
        let non_pushable_predicate: Vec<_> = predicate
            .conjunctions
            .drain_filter(|expr| expr.count_nows() > 0 || HasCorrelated {}.visit_expr(expr))
            .collect();
        let predicate = predicate.rewrite_expr(&mut ColIndexMapping::new(
            self.output_col_idx().iter().map(|i| Some(*i)).collect(),
        ));
        if non_pushable_predicate.is_empty() {
            self.clone_with_predicate(predicate.and(self.predicate().clone()))
                .into()
        } else {
            return LogicalFilter::create(
                self.clone_with_predicate(predicate.and(self.predicate().clone()))
                    .into(),
                Condition {
                    conjunctions: non_pushable_predicate,
                },
            );
        }
    }
}

impl LogicalScan {
    fn to_batch_inner_with_required(&self, required_order: &Order) -> Result<PlanRef> {
        if self.predicate().always_true() {
            required_order.enforce_if_not_satisfies(BatchSeqScan::new(self.clone(), vec![]).into())
        } else {
            let (scan_ranges, predicate) = self.predicate().clone().split_to_scan_ranges(
                self.core.table_desc.clone(),
                self.base
                    .ctx
                    .session_ctx()
                    .config()
                    .get_max_split_range_gap(),
            )?;
            let mut scan = self.clone();
            scan.core.predicate = predicate; // We want to keep `required_col_idx` unchanged, so do not call `clone_with_predicate`.
            let (scan, predicate, project_expr) = scan.predicate_pull_up();

            if predicate.always_false() {
                let plan =
                    LogicalValues::create(vec![], scan.schema().clone(), scan.ctx()).to_batch()?;
                assert_eq!(plan.schema(), self.schema());
                return required_order.enforce_if_not_satisfies(plan);
            }

            let mut plan: PlanRef = BatchSeqScan::new(scan, scan_ranges).into();
            if !predicate.always_true() {
                plan = BatchFilter::new(LogicalFilter::new(plan, predicate)).into();
            }
            if let Some(exprs) = project_expr {
                plan = BatchProject::new(LogicalProject::new(plan, exprs)).into()
            }
            assert_eq!(plan.schema(), self.schema());
            required_order.enforce_if_not_satisfies(plan)
        }
    }

    // For every index, check if the order of the index satisfies the required_order
    // If yes, use an index scan
    fn use_index_scan_if_order_is_satisfied(
        &self,
        required_order: &Order,
    ) -> Option<Result<PlanRef>> {
        if required_order.column_orders.is_empty() {
            return None;
        }

        let index = self.indexes().iter().find(|idx| {
            // TODO(): Do we support `[ ASC | DESC ] [ NULLS { FIRST | LAST } ]` when creating
            // index? if so, we should somehow use the column orders stored in the index catalog.
            Order {
                column_orders: idx
                    .index_item
                    .iter()
                    .map(|idx_item| ColumnOrder::new(idx_item.index, OrderType::default()))
                    .collect(),
            }
            .satisfies(required_order)
        })?;

        let p2s_mapping = index.primary_to_secondary_mapping();
        if self
            .required_col_idx()
            .iter()
            .all(|x| p2s_mapping.contains_key(x))
        {
            let index_scan = self.to_index_scan(
                &index.name,
                index.index_table.table_desc().into(),
                p2s_mapping,
            );
            Some(index_scan.to_batch())
        } else {
            None
        }
    }
}

impl ToBatch for LogicalScan {
    fn to_batch(&self) -> Result<PlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let new = self.clone_with_predicate(self.predicate().clone());

        if !new.indexes().is_empty() {
            let index_selection_rule = IndexSelectionRule::create();
            if let Some(applied) = index_selection_rule.apply(new.clone().into()) {
                if let Some(scan) = applied.as_logical_scan() {
                    // covering index
                    return required_order.enforce_if_not_satisfies(scan.to_batch().unwrap());
                } else if let Some(join) = applied.as_logical_join() {
                    // index lookup join
                    return required_order.enforce_if_not_satisfies(
                        join.index_lookup_join_to_batch_lookup_join().unwrap(),
                    );
                } else {
                    unreachable!();
                }
            } else {
                // Try to make use of index if it satisfies the required order
                if let Some(plan_ref) = new.use_index_scan_if_order_is_satisfied(required_order) {
                    return plan_ref;
                }
            }
        }
        new.to_batch_inner_with_required(required_order)
    }
}

impl ToStream for LogicalScan {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        if self.is_sys_table() {
            return Err(RwError::from(ErrorCode::NotImplemented(
                "streaming on system table is not allowed".to_string(),
                None.into(),
            )));
        }
        if self.predicate().always_true() {
            Ok(StreamTableScan::new(self.clone()).into())
        } else {
            let (scan, predicate, project_expr) = self.predicate_pull_up();
            let mut plan = LogicalFilter::create(scan.into(), predicate);
            if let Some(exprs) = project_expr {
                plan = LogicalProject::create(plan, exprs)
            }
            plan.to_stream(ctx)
        }
    }

    fn logical_rewrite_for_stream(
        &self,
        _ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        if self.is_sys_table() {
            return Err(RwError::from(ErrorCode::NotImplemented(
                "streaming on system table is not allowed".to_string(),
                None.into(),
            )));
        }
        match self.base.logical_pk.is_empty() {
            true => {
                let mut col_ids = HashSet::new();

                for &idx in self.output_col_idx() {
                    col_ids.insert(self.table_desc().columns[idx].column_id);
                }
                let col_need_to_add = self
                    .table_desc()
                    .pk
                    .iter()
                    .filter_map(|c| {
                        if !col_ids.contains(&self.table_desc().columns[c.column_index].column_id) {
                            Some(c.column_index)
                        } else {
                            None
                        }
                    })
                    .collect_vec();

                let mut output_col_idx = self.output_col_idx().clone();
                output_col_idx.extend(col_need_to_add);
                let new_len = output_col_idx.len();
                Ok((
                    self.clone_with_output_indices(output_col_idx).into(),
                    ColIndexMapping::identity_or_none(self.schema().len(), new_len),
                ))
            }
            false => Ok((
                self.clone().into(),
                ColIndexMapping::identity(self.schema().len()),
            )),
        }
    }
}
