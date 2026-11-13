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

//! Splits a normalized logical plan into blocks and summarizes them.
//!
//! A block starts at the plan root, at every child of a boundary operator and at every
//! aggregation that is not the top of its block. From its root it grows downwards:
//!
//! - `Project`, `Filter`, inner `Join` and table `Scan` belong to the block.
//! - An `Agg` with nothing but `Project`s above it is the aggregation of the block. Any other
//!   `Agg` is the root of a nested block, which is an input of the current one. So `HAVING` and
//!   joins over aggregated results belong to the parent block.
//! - Everything else is a boundary and becomes an input of the block.

use risingwave_common::catalog::Engine;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_pb::plan_common::JoinType;

use super::summary::{
    AggCallSummary, AggSummary, BlockInput, BlockSummary, ColKey, ColumnRef, Predicate, Term,
    canonical_set, split_conjuncts,
};
use crate::expr::{Expr, ExprImpl, ExprRewriter, SessionTimezone};
use crate::optimizer::plan_expr_rewriter::ConstEvalRewriter;
use crate::optimizer::plan_node::generic::{GenericPlanRef, PlanAggCall};
use crate::optimizer::plan_node::{
    LogicalJoin, LogicalPlanRef as PlanRef, LogicalScan, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::utils::Condition;

/// A block of a query, with what is needed to rewrite it.
pub struct ExtractedBlock {
    pub summary: BlockSummary,
    /// A plan for every input. For a table it is a scan of all columns without predicate; the
    /// predicate of the original scan is part of the summary.
    pub input_plans: Vec<PlanRef>,
    /// The term of every output column of the plans in `input_plans`.
    pub input_cols: Vec<Vec<Term>>,
    /// Every input as it is in the plan of the block, where a table scan keeps the predicates
    /// that were pushed into it.
    pub original_inputs: Vec<PlanRef>,
}

/// Whether the scan reads a table that can be an input of a block: stored in Hummock and read
/// at the snapshot of the query.
fn is_block_scan(scan: &LogicalScan) -> bool {
    scan.as_of().is_none() && scan.table().engine() == Engine::Hummock
}

/// Whether the join is a boundary whose two sides can be compared with a view definition.
fn is_comparable_join(join: &LogicalJoin) -> bool {
    matches!(
        join.join_type(),
        JoinType::LeftOuter
            | JoinType::RightOuter
            | JoinType::FullOuter
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::RightSemi
            | JoinType::RightAnti
    )
}

/// Applies `f` to the root of every block directly below the block rooted at `plan`. `f` returns
/// `None` to keep a block as it is. Returns the plan rebuilt over the changed blocks, or `None`
/// if no block changed.
///
/// It must classify plan nodes exactly as [`BlockExtractor`] does.
pub fn map_lower_blocks(
    plan: &PlanRef,
    f: &mut dyn FnMut(&PlanRef) -> Option<PlanRef>,
) -> Option<PlanRef> {
    /// Rebuilds `plan` over its inputs, each mapped by `map`.
    fn rebuild(
        plan: &PlanRef,
        mut map: impl FnMut(&PlanRef) -> Option<PlanRef>,
    ) -> Option<PlanRef> {
        let inputs = plan.inputs();
        let mapped: Vec<Option<PlanRef>> = inputs.iter().map(&mut map).collect();
        if mapped.iter().all(Option::is_none) {
            return None;
        }
        let inputs: Vec<PlanRef> = mapped
            .into_iter()
            .zip_eq_fast(inputs)
            .map(|(new, old)| new.unwrap_or(old))
            .collect();
        Some(plan.clone_root_with_inputs(&inputs))
    }

    fn go(
        plan: &PlanRef,
        only_projects_above: bool,
        f: &mut dyn FnMut(&PlanRef) -> Option<PlanRef>,
    ) -> Option<PlanRef> {
        if plan.as_logical_project().is_some() {
            return rebuild(plan, |input| go(input, only_projects_above, f));
        }
        if let Some(agg) = plan.as_logical_agg()
            && agg.grouping_sets().is_empty()
        {
            if only_projects_above {
                return rebuild(plan, |input| go(input, false, f));
            }
            return f(plan);
        }
        let in_block = plan.as_logical_filter().is_some()
            || plan
                .as_logical_join()
                .is_some_and(|join| join.join_type() == JoinType::Inner);
        if in_block {
            return rebuild(plan, |input| go(input, false, f));
        }
        if plan.as_logical_scan().is_some_and(is_block_scan) {
            return None;
        }
        // A boundary: every child starts a block.
        rebuild(plan, |input| f(input))
    }
    go(plan, true, f)
}

#[derive(Default)]
struct Builder {
    inputs: Vec<BlockInput>,
    input_plans: Vec<PlanRef>,
    input_cols: Vec<Vec<Term>>,
    original_inputs: Vec<PlanRef>,
    conjuncts: Vec<Term>,
}

impl Builder {
    fn push(&mut self, input: BlockInput, plan: PlanRef, cols: Vec<Term>) {
        self.inputs.push(input);
        self.original_inputs.push(plan.clone());
        self.input_plans.push(plan);
        self.input_cols.push(cols);
    }
}

fn nested_column(input: usize, side: usize, term: &Term) -> Term {
    Term::Column(ColumnRef {
        input,
        key: ColKey::Nested {
            side,
            term: Box::new(term.clone()),
        },
        ty: term.ty(),
    })
}

fn to_term(expr: &ExprImpl, cols: &[Term]) -> Option<Term> {
    match expr {
        ExprImpl::InputRef(input_ref) => cols.get(input_ref.index()).cloned(),
        ExprImpl::Literal(literal) => Some(Term::constant(
            literal.get_data().clone(),
            literal.return_type(),
        )),
        ExprImpl::FunctionCall(call) => {
            let args = call
                .inputs()
                .iter()
                .map(|arg| to_term(arg, cols))
                .collect::<Option<Vec<_>>>()?;
            Some(Term::func(call.func_type(), call.return_type(), args))
        }
        // Subqueries, window functions, UDFs, parameters and so on are not terms.
        _ => None,
    }
}

pub struct BlockExtractor {
    timezone: SessionTimezone,
}

impl BlockExtractor {
    /// `timezone` is the session timezone the plan is interpreted in.
    pub fn new(timezone: String) -> Self {
        Self {
            timezone: SessionTimezone::new_silent(timezone),
        }
    }

    /// Whether any extracted term depends on the session timezone.
    pub fn timezone_used(&self) -> bool {
        self.timezone.used()
    }

    /// Summarizes the block rooted at `plan`.
    ///
    /// Returns `None` if the block cannot be rewritten: it has an expression that is not a
    /// term, or its predicate can never be true.
    pub fn extract(&mut self, plan: &PlanRef) -> Option<ExtractedBlock> {
        let mut builder = Builder::default();

        let mut projects = vec![];
        let mut top = plan.clone();
        while let Some(project) = top.as_logical_project() {
            let input = project.input();
            projects.push(top.clone());
            top = input;
        }

        let (agg, mut cols) = match top.as_logical_agg() {
            Some(agg) if agg.grouping_sets().is_empty() => {
                let input_cols = self.collect(&mut builder, &agg.input())?;
                let group: Vec<Term> = agg
                    .group_key()
                    .indices()
                    .map(|idx| input_cols[idx].clone())
                    .collect();
                let calls = agg
                    .agg_calls()
                    .iter()
                    .map(|call| self.agg_call(call, &input_cols))
                    .collect::<Option<Vec<_>>>()?;
                let mut cols = group.clone();
                cols.extend(calls.iter().map(|call| Term::Agg(Box::new(call.clone()))));
                (Some(AggSummary { group, calls }), cols)
            }
            _ => (None, self.collect(&mut builder, &top)?),
        };
        for project in projects.iter().rev() {
            let project = project.as_logical_project().unwrap();
            cols = project
                .exprs()
                .iter()
                .map(|expr| self.term(expr, &cols))
                .collect::<Option<Vec<_>>>()?;
        }

        let predicate = Predicate::from_conjuncts(builder.conjuncts)?;
        Some(ExtractedBlock {
            summary: BlockSummary {
                inputs: builder.inputs,
                predicate,
                agg,
                output: cols,
            },
            input_plans: builder.input_plans,
            input_cols: builder.input_cols,
            original_inputs: builder.original_inputs,
        })
    }

    /// Converts an expression over `cols` into a term in normal form: the session timezone is
    /// inlined and constants are folded first.
    fn term(&mut self, expr: &ExprImpl, cols: &[Term]) -> Option<Term> {
        if expr.is_impure() {
            return None;
        }
        let expr = self.timezone.rewrite_expr(expr.clone());
        let mut folder = ConstEvalRewriter { error: None };
        let expr = folder.rewrite_expr(expr);
        if folder.error.is_some() {
            return None;
        }
        to_term(&expr, cols)
    }

    fn conjuncts(&mut self, condition: &Condition, cols: &[Term]) -> Option<Vec<Term>> {
        let mut conjuncts = vec![];
        for expr in &condition.conjunctions {
            split_conjuncts(self.term(expr, cols)?, &mut conjuncts);
        }
        Some(conjuncts)
    }

    fn agg_call(&mut self, call: &PlanAggCall, cols: &[Term]) -> Option<AggCallSummary> {
        Some(AggCallSummary {
            agg_type: call.agg_type.clone(),
            return_type: call.return_type.clone(),
            args: call
                .inputs
                .iter()
                .map(|input| cols[input.index()].clone())
                .collect(),
            distinct: call.distinct,
            order_by: call
                .order_by
                .iter()
                .map(|order| (cols[order.column_index].clone(), order.order_type))
                .collect(),
            filter: canonical_set(self.conjuncts(&call.filter, cols)?),
            direct_args: call.direct_args.clone(),
        })
    }

    /// Walks the part of the block below its aggregation, registers the inputs and predicates
    /// found on the way, and returns the term of every output column of `plan`.
    fn collect(&mut self, builder: &mut Builder, plan: &PlanRef) -> Option<Vec<Term>> {
        if let Some(scan) = plan.as_logical_scan()
            && is_block_scan(scan)
        {
            let input = builder.inputs.len();
            let table = scan.table();
            let all_cols: Vec<Term> = table
                .columns()
                .iter()
                .map(|col| {
                    Term::Column(ColumnRef {
                        input,
                        key: ColKey::Table(col.column_id()),
                        ty: col.data_type().clone(),
                    })
                })
                .collect();
            let full_scan = scan
                .clone_with_output_indices((0..all_cols.len()).collect())
                .clone_with_predicate(Condition::true_cond());
            builder.push(
                BlockInput::Table { table_id: table.id },
                full_scan.into(),
                all_cols.clone(),
            );
            *builder.original_inputs.last_mut().unwrap() = plan.clone();
            // The predicate of a scan is over all columns of the table.
            let conjuncts = self.conjuncts(scan.predicate(), &all_cols)?;
            builder.conjuncts.extend(conjuncts);
            return Some(
                scan.output_col_idx()
                    .iter()
                    .map(|idx| all_cols[*idx].clone())
                    .collect(),
            );
        }
        if let Some(project) = plan.as_logical_project() {
            let cols = self.collect(builder, &project.input())?;
            return project
                .exprs()
                .iter()
                .map(|expr| self.term(expr, &cols))
                .collect();
        }
        if let Some(filter) = plan.as_logical_filter() {
            let cols = self.collect(builder, &filter.input())?;
            let conjuncts = self.conjuncts(filter.predicate(), &cols)?;
            builder.conjuncts.extend(conjuncts);
            return Some(cols);
        }
        if let Some(join) = plan.as_logical_join() {
            if join.join_type() == JoinType::Inner {
                let mut cols = self.collect(builder, &join.left())?;
                cols.extend(self.collect(builder, &join.right())?);
                let conjuncts = self.conjuncts(join.on(), &cols)?;
                builder.conjuncts.extend(conjuncts);
                return Some(
                    join.output_indices()
                        .iter()
                        .map(|idx| cols[*idx].clone())
                        .collect(),
                );
            }
            if is_comparable_join(join)
                && let Some(cols) = self.join_boundary(builder, plan, join)
            {
                return Some(cols);
            }
            return Some(Self::opaque(builder, plan));
        }
        if let Some(agg) = plan.as_logical_agg()
            && agg.grouping_sets().is_empty()
            && let Some(nested) = self.extract(plan)
            && nested.summary.agg.is_some()
        {
            let input = builder.inputs.len();
            let cols: Vec<Term> = nested
                .summary
                .output
                .iter()
                .map(|term| nested_column(input, 0, term))
                .collect();
            builder.push(
                BlockInput::Block(Box::new(nested.summary)),
                plan.clone(),
                cols.clone(),
            );
            return Some(cols);
        }
        Some(Self::opaque(builder, plan))
    }

    fn join_boundary(
        &mut self,
        builder: &mut Builder,
        plan: &PlanRef,
        join: &LogicalJoin,
    ) -> Option<Vec<Term>> {
        let left = self.extract(&join.left())?;
        let right = self.extract(&join.right())?;
        let input = builder.inputs.len();
        let left_cols: Vec<Term> = left
            .summary
            .output
            .iter()
            .map(|term| nested_column(input, 0, term))
            .collect();
        let right_cols: Vec<Term> = right
            .summary
            .output
            .iter()
            .map(|term| nested_column(input, 1, term))
            .collect();
        let mut both = left_cols.clone();
        both.extend(right_cols.iter().cloned());
        let on = canonical_set(self.conjuncts(join.on(), &both)?);
        let internal = match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti => left_cols,
            JoinType::RightSemi | JoinType::RightAnti => right_cols,
            _ => both,
        };
        let cols: Vec<Term> = join
            .output_indices()
            .iter()
            .map(|idx| internal[*idx].clone())
            .collect();
        builder.push(
            BlockInput::Join {
                join_type: join.join_type(),
                on,
                left: Box::new(left.summary),
                right: Box::new(right.summary),
            },
            plan.clone(),
            cols.clone(),
        );
        Some(cols)
    }

    fn opaque(builder: &mut Builder, plan: &PlanRef) -> Vec<Term> {
        let input = builder.inputs.len();
        let cols: Vec<Term> = plan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                Term::Column(ColumnRef {
                    input,
                    key: ColKey::Position(idx),
                    ty: field.data_type(),
                })
            })
            .collect();
        builder.push(BlockInput::Opaque, plan.clone(), cols.clone());
        cols
    }
}
