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

use std::collections::HashMap;
use std::{fmt, iter};

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, TrackingIssue};
use risingwave_common::types::{DataType, Datum, OrderedF64, ScalarImpl};
use risingwave_expr::expr::AggKind;

use super::generic::{
    self, AggCallState, GenericPlanNode, GenericPlanRef, PlanAggCall, PlanAggOrderByField,
    ProjectBuilder,
};
use super::{
    BatchHashAgg, BatchSimpleAgg, ColPrunable, ExprRewritable, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamGlobalSimpleAgg, StreamHashAgg,
    StreamLocalSimpleAgg, StreamProject, ToBatch, ToStream,
};
use crate::catalog::table_catalog::TableCatalog;
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Literal, OrderBy,
};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::{
    gen_filter_and_pushdown, BatchSortAgg, ColumnPruningContext, LogicalProject,
    PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::Direction::{Asc, Desc};
use crate::optimizer::property::{
    Distribution, FieldOrder, FunctionalDependencySet, Order, RequiredDist,
};
use crate::utils::{ColIndexMapping, ColIndexMappingRewriteExt, Condition, Substitute};

/// `LogicalAgg` groups input data by their group key and computes aggregation functions.
///
/// It corresponds to the `GROUP BY` operator in a SQL query statement together with the aggregate
/// functions in the `SELECT` clause.
///
/// The output schema will first include the group key and then the aggregation calls.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogicalAgg {
    pub base: PlanBase,
    core: generic::Agg<PlanRef>,
}

/// We insert a `count(*)` agg at the beginning of stream agg calls.
const STREAM_ROW_COUNT_COLUMN: usize = 0;

impl LogicalAgg {
    /// Infer agg result table for streaming agg.
    pub fn infer_result_table(&self, vnode_col_idx: Option<usize>) -> TableCatalog {
        self.core.infer_result_table(&self.base, vnode_col_idx)
    }

    /// Infer `AggCallState`s for streaming agg.
    pub fn infer_stream_agg_state(&self, vnode_col_idx: Option<usize>) -> Vec<AggCallState> {
        self.core.infer_stream_agg_state(&self.base, vnode_col_idx)
    }

    /// Infer dedup tables for distinct agg calls.
    pub fn infer_distinct_dedup_tables(
        &self,
        vnode_col_idx: Option<usize>,
    ) -> HashMap<usize, TableCatalog> {
        self.core
            .infer_distinct_dedup_tables(&self.base, vnode_col_idx)
    }

    /// Generate plan for stateless 2-phase streaming agg.
    /// Should only be used iff input is distributed. Input must be converted to stream form.
    fn gen_stateless_two_phase_streaming_agg_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        debug_assert!(self.group_key().is_empty());
        let local_agg = StreamLocalSimpleAgg::new(self.clone_with_input(stream_input));
        let exchange =
            RequiredDist::single().enforce_if_not_satisfies(local_agg.into(), &Order::any())?;
        let global_agg = StreamGlobalSimpleAgg::new(LogicalAgg::new(
            self.agg_calls()
                .iter()
                .enumerate()
                .map(|(partial_output_idx, agg_call)| {
                    agg_call.partial_to_total_agg_call(
                        partial_output_idx,
                        partial_output_idx == STREAM_ROW_COUNT_COLUMN,
                    )
                })
                .collect(),
            vec![],
            exchange,
        ));
        Ok(global_agg.into())
    }

    /// Generate plan for stateless/stateful 2-phase streaming agg.
    /// Should only be used iff input is distributed.
    /// Input must be converted to stream form.
    fn gen_vnode_two_phase_streaming_agg_plan(
        &self,
        stream_input: PlanRef,
        dist_key: &[usize],
    ) -> Result<PlanRef> {
        // Generate vnode via project
        let input_fields = stream_input.schema().fields();
        let input_col_num = input_fields.len();

        let mut exprs: Vec<_> = input_fields
            .iter()
            .enumerate()
            .map(|(idx, field)| InputRef::new(idx, field.data_type.clone()).into())
            .collect();
        exprs.push(
            FunctionCall::new(
                ExprType::Vnode,
                dist_key
                    .iter()
                    .map(|idx| InputRef::new(*idx, input_fields[*idx].data_type()).into())
                    .collect(),
            )?
            .into(),
        );
        let vnode_col_idx = exprs.len() - 1;
        // TODO(kwannoel): We should apply Project optimization rules here.
        let project = StreamProject::new(LogicalProject::new(stream_input, exprs));

        // Generate local agg step
        let mut local_group_key = self.group_key().to_vec();
        local_group_key.push(vnode_col_idx);
        let n_local_group_key = local_group_key.len();
        let local_agg = StreamHashAgg::new(
            LogicalAgg::new(self.agg_calls().to_vec(), local_group_key, project.into()),
            Some(vnode_col_idx),
        );
        // Global group key excludes vnode.
        let local_group_key_without_vnode =
            &local_agg.group_key()[..local_agg.group_key().len() - 1];
        let global_group_key = local_agg
            .i2o_col_mapping()
            .rewrite_dist_key(local_group_key_without_vnode)
            .expect("some input group key could not be mapped");

        // Generate global agg step
        if self.group_key().is_empty() {
            let exchange =
                RequiredDist::single().enforce_if_not_satisfies(local_agg.into(), &Order::any())?;
            let global_agg = StreamGlobalSimpleAgg::new(LogicalAgg::new(
                self.agg_calls()
                    .iter()
                    .enumerate()
                    .map(|(partial_output_idx, agg_call)| {
                        agg_call.partial_to_total_agg_call(
                            n_local_group_key + partial_output_idx,
                            partial_output_idx == STREAM_ROW_COUNT_COLUMN,
                        )
                    })
                    .collect(),
                global_group_key,
                exchange,
            ));
            Ok(global_agg.into())
        } else {
            let exchange = RequiredDist::shard_by_key(input_col_num, &global_group_key)
                .enforce_if_not_satisfies(local_agg.into(), &Order::any())?;
            // Local phase should have reordered the group keys into their required order.
            // we can just follow it.
            let global_agg = StreamHashAgg::new(
                LogicalAgg::new(
                    self.agg_calls()
                        .iter()
                        .enumerate()
                        .map(|(partial_output_idx, agg_call)| {
                            agg_call.partial_to_total_agg_call(
                                n_local_group_key + partial_output_idx,
                                partial_output_idx == STREAM_ROW_COUNT_COLUMN,
                            )
                        })
                        .collect(),
                    global_group_key,
                    exchange,
                ),
                None,
            );
            Ok(global_agg.into())
        }
    }

    fn gen_single_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        Ok(StreamGlobalSimpleAgg::new(self.clone_with_input(
            RequiredDist::single().enforce_if_not_satisfies(stream_input, &Order::any())?,
        ))
        .into())
    }

    fn gen_shuffle_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        Ok(StreamHashAgg::new(
            self.clone_with_input(
                RequiredDist::shard_by_key(stream_input.schema().len(), self.group_key())
                    .enforce_if_not_satisfies(stream_input, &Order::any())?,
            ),
            None,
        )
        .into())
    }

    /// See if all stream aggregation calls have a stateless local agg counterpart.
    fn all_local_aggs_are_stateless(&self, stream_input_append_only: bool) -> bool {
        self.agg_calls().iter().all(|c| {
            matches!(c.agg_kind, AggKind::Sum | AggKind::Count)
                || (matches!(c.agg_kind, AggKind::Min | AggKind::Max) && stream_input_append_only)
        })
    }

    /// Generally used by two phase hash agg.
    /// If input dist already satisfies hash agg distribution,
    /// it will be more expensive to do two phase agg, should just do shuffle agg.
    pub(crate) fn hash_agg_dist_satisfied_by_input_dist(&self, input_dist: &Distribution) -> bool {
        let required_dist =
            RequiredDist::shard_by_key(self.input().schema().len(), self.group_key());
        input_dist.satisfies(&required_dist)
    }

    /// Generates distributed stream plan.
    fn gen_dist_stream_agg_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        let input_dist = stream_input.distribution();
        debug_assert!(*input_dist != Distribution::Broadcast);

        // Shuffle agg
        // If we have group key, and we won't try two phase agg optimization at all,
        // we will always choose shuffle agg over single agg.
        if !self.group_key().is_empty() && !self.must_try_two_phase_agg() {
            return self.gen_shuffle_plan(stream_input);
        }

        // Standalone agg
        // If no group key, and cannot two phase agg, we have to use single plan.
        if self.group_key().is_empty() && !self.can_two_phase_agg() {
            return self.gen_single_plan(stream_input);
        }

        debug_assert!(if !self.group_key().is_empty() {
            self.must_try_two_phase_agg()
        } else {
            self.can_two_phase_agg()
        });

        // Stateless 2-phase simple agg
        // can be applied on stateless simple agg calls,
        // with input distributed by [`Distribution::AnyShard`]
        if self.group_key().is_empty()
            && self.all_local_aggs_are_stateless(stream_input.append_only())
            && input_dist.satisfies(&RequiredDist::AnyShard)
        {
            return self.gen_stateless_two_phase_streaming_agg_plan(stream_input);
        }

        // If input is [`Distribution::SomeShard`] and we must try to use two phase agg,
        // The only remaining strategy is Vnode-based 2-phase agg.
        // We shall first distribute it by PK,
        // so it obeys consistent hash strategy via [`Distribution::HashShard`].
        let stream_input =
            if *input_dist == Distribution::SomeShard && self.must_try_two_phase_agg() {
                RequiredDist::shard_by_key(stream_input.schema().len(), stream_input.logical_pk())
                    .enforce_if_not_satisfies(stream_input, &Order::any())?
            } else {
                stream_input
            };
        let input_dist = stream_input.distribution();

        // Vnode-based 2-phase agg
        // can be applied on agg calls not affected by order,
        // with input distributed by dist_key.
        match input_dist {
            Distribution::HashShard(dist_key) | Distribution::UpstreamHashShard(dist_key, _)
                if (!self.hash_agg_dist_satisfied_by_input_dist(input_dist)
                    || self.group_key().is_empty()) =>
            {
                let dist_key = dist_key.clone();
                return self.gen_vnode_two_phase_streaming_agg_plan(stream_input, &dist_key);
            }
            _ => {}
        }

        // Fallback to shuffle or single, if we can't generate any 2-phase plans.
        if !self.group_key().is_empty() {
            self.gen_shuffle_plan(stream_input)
        } else {
            self.gen_single_plan(stream_input)
        }
    }

    /// Check if the aggregation result will be affected by order by clause, if any.
    pub(crate) fn is_agg_result_affected_by_order(&self) -> bool {
        self.agg_calls()
            .iter()
            .any(|call| matches!(call.agg_kind, AggKind::StringAgg | AggKind::ArrayAgg))
    }

    pub(crate) fn two_phase_agg_forced(&self) -> bool {
        self.base
            .ctx()
            .session_ctx()
            .config()
            .get_force_two_phase_agg()
    }

    fn two_phase_agg_enabled(&self) -> bool {
        self.base
            .ctx()
            .session_ctx()
            .config()
            .get_enable_two_phase_agg()
    }

    /// Must try two phase agg iff we are forced to, and we satisfy the constraints.
    fn must_try_two_phase_agg(&self) -> bool {
        self.two_phase_agg_forced() && self.can_two_phase_agg()
    }

    pub(crate) fn can_two_phase_agg(&self) -> bool {
        !self.agg_calls().is_empty()
            && self.agg_calls().iter().all(|call| {
                matches!(
                    call.agg_kind,
                    AggKind::Min | AggKind::Max | AggKind::Sum | AggKind::Count
                ) && !call.distinct
                // QUESTION: why do we need `&& call.order_by_fields.is_empty()` ?
                //    && call.order_by_fields.is_empty()
            })
            && !self.is_agg_result_affected_by_order()
            && self.two_phase_agg_enabled()
    }

    // Check if the output of the aggregation needs to be sorted and return ordering req by group
    // keys If group key order satisfies required order, push down the sort below the
    // aggregation and use sort aggregation. The data type of the columns need to be int32
    fn output_requires_order_on_group_keys(&self, required_order: &Order) -> (bool, Order) {
        let group_key_order = Order {
            field_order: self
                .group_key()
                .iter()
                .map(|group_by_idx| {
                    let direct = if required_order.field_order.contains(&FieldOrder {
                        index: *group_by_idx,
                        direct: Desc,
                    }) {
                        // If output requires descending order, use descending order
                        Desc
                    } else {
                        // In all other cases use ascending order
                        Asc
                    };
                    FieldOrder {
                        index: *group_by_idx,
                        direct,
                    }
                })
                .collect(),
        };
        return (
            !required_order.field_order.is_empty()
                && group_key_order.satisfies(required_order)
                && self.group_key().iter().all(|group_by_idx| {
                    self.schema().fields().get(*group_by_idx).unwrap().data_type == DataType::Int32
                }),
            group_key_order,
        );
    }

    // Check if the input is already sorted, and hence sort merge aggregation can be used
    // It can only be used, if the input is sorted on all group key indices and the
    // datatype of the column is int32
    fn input_provides_order_on_group_keys(&self, new_logical: &LogicalAgg) -> bool {
        self.group_key().iter().all(|group_by_idx| {
            new_logical
                .input()
                .order()
                .field_order
                .iter()
                .any(|field_order| field_order.index == *group_by_idx)
                && new_logical
                    .input()
                    .schema()
                    .fields()
                    .get(*group_by_idx)
                    .unwrap()
                    .data_type
                    == DataType::Int32
        })
    }

    pub fn core(&self) -> &generic::Agg<PlanRef> {
        &self.core
    }
}

/// `LogicalAggBuilder` extracts agg calls and references to group columns from select list and
/// build the plan like `LogicalAgg - LogicalProject`.
/// it is constructed by `group_exprs` and collect and rewrite the expression in selection and
/// having clause.
struct LogicalAggBuilder {
    /// the builder of the input Project
    input_proj_builder: ProjectBuilder,
    /// the group key column indices in the project's output
    group_key: Vec<usize>,
    /// the agg calls
    agg_calls: Vec<PlanAggCall>,
    /// the error during the expression rewriting
    error: Option<ErrorCode>,
    /// If `is_in_filter_clause` is true, it means that
    /// we are processing filter clause.
    /// This field is needed because input refs in these clauses
    /// are allowed to refer to any columns, while those not in filter
    /// clause are only allowed to refer to group keys.
    is_in_filter_clause: bool,
}

impl LogicalAggBuilder {
    fn new(group_exprs: Vec<ExprImpl>) -> Result<Self> {
        let mut input_proj_builder = ProjectBuilder::default();

        let group_key = group_exprs
            .into_iter()
            .map(|expr| input_proj_builder.add_expr(&expr))
            .try_collect()
            .map_err(|err| {
                ErrorCode::NotImplemented(format!("{err} inside GROUP BY"), None.into())
            })?;

        Ok(LogicalAggBuilder {
            group_key,
            agg_calls: vec![],
            error: None,
            input_proj_builder,
            is_in_filter_clause: false,
        })
    }

    pub fn build(self, input: PlanRef) -> LogicalAgg {
        // This LogicalProject focuses on the exprs in aggregates and GROUP BY clause.
        let logical_project = LogicalProject::with_core(self.input_proj_builder.build(input));

        // This LogicalAgg focuses on calculating the aggregates and grouping.
        LogicalAgg::new(self.agg_calls, self.group_key, logical_project.into())
    }

    fn rewrite_with_error(&mut self, expr: ExprImpl) -> Result<ExprImpl> {
        let rewritten_expr = self.rewrite_expr(expr);
        if let Some(error) = self.error.take() {
            return Err(error.into());
        }
        Ok(rewritten_expr)
    }

    /// check if the expression is a group by key, and try to return the group key
    pub fn try_as_group_expr(&self, expr: &ExprImpl) -> Option<usize> {
        if let Some(input_index) = self.input_proj_builder.expr_index(expr) {
            if let Some(index) = self
                .group_key
                .iter()
                .position(|group_key| *group_key == input_index)
            {
                return Some(index);
            }
        }
        None
    }

    /// syntax check for distinct aggregates.
    ///
    /// TODO: we may disable this syntax check in the future because we may use another approach to
    /// implement distinct aggregates.
    pub fn syntax_check(&self) -> Result<()> {
        let mut has_distinct = false;
        let mut has_order_by = false;
        // TODO(stonepage): refactor it and unify the 2-phase agg rewriting logic
        let mut has_non_distinct_string_agg = false;
        let mut has_non_distinct_array_agg = false;
        self.agg_calls.iter().for_each(|agg_call| {
            if agg_call.distinct {
                has_distinct = true;
            }
            if !agg_call.order_by_fields.is_empty() {
                has_order_by = true;
            }
            if !agg_call.distinct && agg_call.agg_kind == AggKind::StringAgg {
                has_non_distinct_string_agg = true;
            }
            if !agg_call.distinct && agg_call.agg_kind == AggKind::ArrayAgg {
                has_non_distinct_array_agg = true;
            }
        });

        // order by is disallowed occur with distinct because we can not diectly rewrite agg with
        // order by into 2-phase agg.
        if has_distinct && has_order_by {
            return Err(ErrorCode::InvalidInputSyntax(
                "Order by aggregates are disallowed to occur with distinct aggregates".into(),
            )
            .into());
        }

        // when there are distinct aggregates, non-distinct aggregates will be rewritten as
        // two-phase aggregates, while string_agg can not be rewritten as two-phase aggregates, so
        // we have to ban this case now.
        if has_distinct && has_non_distinct_string_agg {
            return Err(ErrorCode::NotImplemented(
                "Non-distinct string_agg can't appear with distinct aggregates".into(),
                TrackingIssue::none(),
            )
            .into());
        }
        if has_distinct && has_non_distinct_array_agg {
            return Err(ErrorCode::NotImplemented(
                "Non-distinct array_agg can't appear with distinct aggregates".into(),
                TrackingIssue::none(),
            )
            .into());
        }

        Ok(())
    }

    /// When there is an agg call, there are 3 things to do:
    /// 1. eval its inputs via project;
    /// 2. add a `PlanAggCall` to agg;
    /// 3. rewrite it as an `InputRef` to the agg result in select list.
    ///
    /// Note that the rewriter does not traverse into inputs of agg calls.
    fn try_rewrite_agg_call(
        &mut self,
        agg_call: AggCall,
    ) -> std::result::Result<ExprImpl, ErrorCode> {
        let return_type = agg_call.return_type();
        let (agg_kind, inputs, mut distinct, mut order_by, filter) = agg_call.decompose();
        match &agg_kind {
            AggKind::Min | AggKind::Max => {
                distinct = false;
                order_by = OrderBy::any();
            }
            AggKind::Sum
            | AggKind::Count
            | AggKind::Avg
            | AggKind::ApproxCountDistinct
            | AggKind::StddevSamp
            | AggKind::StddevPop
            | AggKind::VarPop
            | AggKind::VarSamp => {
                order_by = OrderBy::any();
            }
            _ => {
                // To be conservative, we just treat newly added AggKind in the future as not
                // rewritable.
            }
        }

        self.is_in_filter_clause = true;
        // filter expr is not added to `input_proj_builder` as a whole. Special exprs incl
        // subquery/agg/table are rejected in `bind_agg`.
        let filter = filter.rewrite_expr(self);
        self.is_in_filter_clause = false;

        let inputs: Vec<_> = inputs
            .iter()
            .map(|expr| {
                let index = self.input_proj_builder.add_expr(expr)?;
                Ok(InputRef::new(index, expr.return_type()))
            })
            .try_collect()
            .map_err(|err: &'static str| {
                ErrorCode::NotImplemented(format!("{err} inside aggregation calls"), None.into())
            })?;

        let order_by_fields: Vec<_> = order_by
            .sort_exprs
            .iter()
            .map(|e| {
                let index = self.input_proj_builder.add_expr(&e.expr)?;
                Ok(PlanAggOrderByField {
                    input: InputRef::new(index, e.expr.return_type()),
                    direction: e.direction,
                    nulls_first: e.nulls_first,
                })
            })
            .try_collect()
            .map_err(|err: &'static str| {
                ErrorCode::NotImplemented(
                    format!("{err} inside aggregation calls order by"),
                    None.into(),
                )
            })?;

        match agg_kind {
            AggKind::Avg => {
                assert_eq!(inputs.len(), 1);

                let left_return_type =
                    AggCall::infer_return_type(&AggKind::Sum, &[inputs[0].return_type()]).unwrap();

                // Rewrite avg to cast(sum as avg_return_type) / count.
                self.agg_calls.push(PlanAggCall {
                    agg_kind: AggKind::Sum,
                    return_type: left_return_type.clone(),
                    inputs: inputs.clone(),
                    distinct,
                    order_by_fields: order_by_fields.clone(),
                    filter: filter.clone(),
                });
                let left = ExprImpl::from(InputRef::new(
                    self.group_key.len() + self.agg_calls.len() - 1,
                    left_return_type,
                ))
                .cast_implicit(return_type)
                .unwrap();

                let right_return_type =
                    AggCall::infer_return_type(&AggKind::Count, &[inputs[0].return_type()])
                        .unwrap();

                self.agg_calls.push(PlanAggCall {
                    agg_kind: AggKind::Count,
                    return_type: right_return_type.clone(),
                    inputs,
                    distinct,
                    order_by_fields,
                    filter,
                });

                let right = InputRef::new(
                    self.group_key.len() + self.agg_calls.len() - 1,
                    right_return_type,
                );

                Ok(ExprImpl::from(
                    FunctionCall::new(ExprType::Divide, vec![left, right.into()]).unwrap(),
                ))
            }

            // We compute `var_samp` as
            // (sum(sq) - sum * sum / count) / (count - 1)
            // and `var_pop` as
            // (sum(sq) - sum * sum / count) / count
            // Since we don't have the square function, we use the plain Multiply for squaring,
            // which is in a sense more general than the pow function, especially when calculating
            // covariances in the future. Also we don't have the sqrt function for rooting, so we
            // use pow(x, 0.5) to simulate
            AggKind::StddevPop | AggKind::StddevSamp | AggKind::VarPop | AggKind::VarSamp => {
                let input = inputs.iter().exactly_one().unwrap();

                // first, we compute sum of squared as sum_sq
                let squared_input_expr = ExprImpl::from(
                    FunctionCall::new(
                        ExprType::Multiply,
                        vec![ExprImpl::from(input.clone()), ExprImpl::from(input.clone())],
                    )
                    .unwrap(),
                );

                let squared_input_proj_index = self
                    .input_proj_builder
                    .add_expr(&squared_input_expr)
                    .unwrap();

                let sum_of_squares_return_type =
                    AggCall::infer_return_type(&AggKind::Sum, &[squared_input_expr.return_type()])
                        .unwrap();

                self.agg_calls.push(PlanAggCall {
                    agg_kind: AggKind::Sum,
                    return_type: sum_of_squares_return_type.clone(),
                    inputs: vec![InputRef::new(
                        squared_input_proj_index,
                        squared_input_expr.return_type(),
                    )],
                    distinct,
                    order_by_fields: order_by_fields.clone(),
                    filter: filter.clone(),
                });

                let sum_of_squares_expr = ExprImpl::from(InputRef::new(
                    self.group_key.len() + self.agg_calls.len() - 1,
                    sum_of_squares_return_type,
                ))
                .cast_implicit(return_type.clone())
                .unwrap();

                // after that, we compute sum
                let sum_return_type =
                    AggCall::infer_return_type(&AggKind::Sum, &[input.return_type()]).unwrap();

                self.agg_calls.push(PlanAggCall {
                    agg_kind: AggKind::Sum,
                    return_type: sum_return_type.clone(),
                    inputs: inputs.clone(),
                    distinct,
                    order_by_fields: order_by_fields.clone(),
                    filter: filter.clone(),
                });

                let sum_expr = ExprImpl::from(InputRef::new(
                    self.group_key.len() + self.agg_calls.len() - 1,
                    sum_return_type,
                ))
                .cast_implicit(return_type.clone())
                .unwrap();

                // then, we compute count
                let count_return_type =
                    AggCall::infer_return_type(&AggKind::Count, &[input.return_type()]).unwrap();

                self.agg_calls.push(PlanAggCall {
                    agg_kind: AggKind::Count,
                    return_type: count_return_type.clone(),
                    inputs,
                    distinct,
                    order_by_fields,
                    filter,
                });

                let count_expr = ExprImpl::from(InputRef::new(
                    self.group_key.len() + self.agg_calls.len() - 1,
                    count_return_type,
                ));

                // we start with variance

                // sum * sum
                let square_of_sum_expr = ExprImpl::from(
                    FunctionCall::new(ExprType::Multiply, vec![sum_expr.clone(), sum_expr])
                        .unwrap(),
                );

                // sum_sq - sum * sum / count
                let numerator_expr = ExprImpl::from(
                    FunctionCall::new(
                        ExprType::Subtract,
                        vec![
                            sum_of_squares_expr,
                            ExprImpl::from(
                                FunctionCall::new(
                                    ExprType::Divide,
                                    vec![square_of_sum_expr, count_expr.clone()],
                                )
                                .unwrap(),
                            ),
                        ],
                    )
                    .unwrap(),
                );

                // count or count - 1
                let denominator_expr = match agg_kind {
                    AggKind::StddevPop | AggKind::VarPop => count_expr.clone(),
                    AggKind::StddevSamp | AggKind::VarSamp => ExprImpl::from(
                        FunctionCall::new(
                            ExprType::Subtract,
                            vec![
                                count_expr.clone(),
                                ExprImpl::from(Literal::new(
                                    Datum::from(ScalarImpl::Int64(1)),
                                    DataType::Int64,
                                )),
                            ],
                        )
                        .unwrap(),
                    ),
                    _ => unreachable!(),
                };

                let mut target_expr = ExprImpl::from(
                    FunctionCall::new(ExprType::Divide, vec![numerator_expr, denominator_expr])
                        .unwrap(),
                );

                // stddev = sqrt(variance)
                if matches!(agg_kind, AggKind::StddevPop | AggKind::StddevSamp) {
                    target_expr = ExprImpl::from(
                        FunctionCall::new(
                            ExprType::Pow,
                            vec![
                                target_expr.clone(),
                                // TODO: The decimal implementation now still relies on float64, so
                                // float64 is still used here
                                ExprImpl::from(Literal::new(
                                    Datum::from(ScalarImpl::Float64(OrderedF64::from(0.5))),
                                    DataType::Float64,
                                )),
                            ],
                        )
                        .unwrap(),
                    );
                }

                match agg_kind {
                    AggKind::VarPop | AggKind::StddevPop => Ok(target_expr),
                    AggKind::StddevSamp | AggKind::VarSamp => {
                        let less_than_expr = ExprImpl::from(
                            FunctionCall::new(
                                ExprType::LessThanOrEqual,
                                vec![
                                    count_expr,
                                    ExprImpl::from(Literal::new(
                                        Datum::from(ScalarImpl::Int64(1)),
                                        DataType::Int64,
                                    )),
                                ],
                            )
                            .unwrap(),
                        );
                        let null_expr = ExprImpl::from(Literal::new(None, return_type));

                        let case_expr = ExprImpl::from(
                            FunctionCall::new(
                                ExprType::Case,
                                vec![less_than_expr, null_expr, target_expr],
                            )
                            .unwrap(),
                        );

                        Ok(case_expr)
                    }
                    _ => unreachable!(),
                }
            }

            _ => {
                self.agg_calls.push(PlanAggCall {
                    agg_kind,
                    return_type: return_type.clone(),
                    inputs,
                    distinct,
                    order_by_fields,
                    filter,
                });
                Ok(
                    InputRef::new(self.group_key.len() + self.agg_calls.len() - 1, return_type)
                        .into(),
                )
            }
        }
    }
}

impl ExprRewriter for LogicalAggBuilder {
    fn rewrite_agg_call(&mut self, agg_call: AggCall) -> ExprImpl {
        let dummy = Literal::new(None, agg_call.return_type()).into();
        match self.try_rewrite_agg_call(agg_call) {
            Ok(expr) => expr,
            Err(err) => {
                self.error = Some(err);
                dummy
            }
        }
    }

    /// When there is an `FunctionCall` (outside of agg call), it must refers to a group column.
    /// Or all `InputRef`s appears in it must refer to a group column.
    fn rewrite_function_call(&mut self, func_call: FunctionCall) -> ExprImpl {
        let expr = func_call.into();
        if let Some(group_key) = self.try_as_group_expr(&expr) {
            InputRef::new(group_key, expr.return_type()).into()
        } else {
            let (func_type, inputs, ret) = expr.into_function_call().unwrap().decompose();
            let inputs = inputs
                .into_iter()
                .map(|expr| self.rewrite_expr(expr))
                .collect();
            FunctionCall::new_unchecked(func_type, inputs, ret).into()
        }
    }

    /// When there is an `InputRef` (outside of agg call), it must refers to a group column.
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let expr = input_ref.into();
        if let Some(group_key) = self.try_as_group_expr(&expr) {
            InputRef::new(group_key, expr.return_type()).into()
        } else if self.is_in_filter_clause {
            InputRef::new(
                self.input_proj_builder.add_expr(&expr).unwrap(),
                expr.return_type(),
            )
            .into()
        } else {
            self.error = Some(ErrorCode::InvalidInputSyntax(
                "column must appear in the GROUP BY clause or be used in an aggregate function"
                    .into(),
            ));
            expr
        }
    }

    fn rewrite_subquery(&mut self, subquery: crate::expr::Subquery) -> ExprImpl {
        if subquery.is_correlated(0) {
            self.error = Some(ErrorCode::NotImplemented(
                "correlated subquery in HAVING or SELECT with agg".into(),
                2275.into(),
            ));
        }
        subquery.into()
    }
}

impl LogicalAgg {
    pub fn new(agg_calls: Vec<PlanAggCall>, group_key: Vec<usize>, input: PlanRef) -> Self {
        let ctx = input.ctx();
        let core = generic::Agg {
            agg_calls,
            group_key,
            input,
        };
        let schema = core.schema();
        let pk_indices = core.logical_pk();
        let functional_dependency = Self::derive_fd(
            schema.len(),
            core.input.schema().len(),
            core.input.functional_dependency(),
            &core.group_key,
        );

        let base = PlanBase::new_logical(
            ctx,
            schema,
            pk_indices.unwrap_or_default(),
            functional_dependency,
        );
        Self { base, core }
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        self.core.o2i_col_mapping()
    }

    /// get the Mapping of columnIndex from input column index to out column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.core.i2o_col_mapping()
    }

    fn derive_fd(
        column_cnt: usize,
        input_len: usize,
        input_fd_set: &FunctionalDependencySet,
        group_key: &[usize],
    ) -> FunctionalDependencySet {
        let mut fd_set =
            FunctionalDependencySet::with_key(column_cnt, &(0..group_key.len()).collect_vec());
        // take group keys from input_columns, then grow the target size to column_cnt
        let i2o = ColIndexMapping::with_remaining_columns(group_key, input_len).composite(
            &ColIndexMapping::identity_or_none(group_key.len(), column_cnt),
        );
        for fd in input_fd_set.as_dependencies() {
            if let Some(fd) = i2o.rewrite_functional_dependency(fd) {
                fd_set.add_functional_dependency(fd);
            }
        }
        fd_set
    }

    /// `create` will analyze select exprs, group exprs and having, and construct a plan like
    ///
    /// ```text
    /// LogicalAgg -> LogicalProject -> input
    /// ```
    ///
    /// It also returns the rewritten select exprs and having that reference into the aggregated
    /// results.
    pub fn create(
        select_exprs: Vec<ExprImpl>,
        group_exprs: Vec<ExprImpl>,
        having: Option<ExprImpl>,
        input: PlanRef,
    ) -> Result<(PlanRef, Vec<ExprImpl>, Option<ExprImpl>)> {
        let mut agg_builder = LogicalAggBuilder::new(group_exprs)?;

        let rewritten_select_exprs = select_exprs
            .into_iter()
            .map(|expr| agg_builder.rewrite_with_error(expr))
            .collect::<Result<_>>()?;
        let rewritten_having = having
            .map(|expr| agg_builder.rewrite_with_error(expr))
            .transpose()?;

        agg_builder.syntax_check()?;

        Ok((
            agg_builder.build(input).into(),
            rewritten_select_exprs,
            rewritten_having,
        ))
    }

    /// Get a reference to the logical agg's agg calls.
    pub fn agg_calls(&self) -> &Vec<PlanAggCall> {
        &self.core.agg_calls
    }

    /// Get a reference to the logical agg's group key.
    pub fn group_key(&self) -> &Vec<usize> {
        &self.core.group_key
    }

    pub fn decompose(self) -> (Vec<PlanAggCall>, Vec<usize>, PlanRef) {
        self.core.decompose()
    }

    #[must_use]
    fn rewrite_with_input_agg(
        &self,
        input: PlanRef,
        agg_calls: &[PlanAggCall],
        mut input_col_change: ColIndexMapping,
    ) -> Self {
        let agg_calls = agg_calls
            .iter()
            .cloned()
            .map(|mut agg_call| {
                agg_call.inputs.iter_mut().for_each(|i| {
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                agg_call.order_by_fields.iter_mut().for_each(|field| {
                    let i = &mut field.input;
                    *i = InputRef::new(input_col_change.map(i.index()), i.return_type())
                });
                agg_call.filter = agg_call.filter.rewrite_expr(&mut input_col_change);
                agg_call
            })
            .collect();
        let group_key = self
            .group_key()
            .iter()
            .cloned()
            .map(|key| input_col_change.map(key))
            .collect();
        Self::new(agg_calls, group_key, input)
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        self.core.fmt_with_name(f, name)
    }

    fn to_batch_simple_agg(&self) -> Result<PlanRef> {
        let new_input = self.input().to_batch()?;
        let new_logical = self.clone_with_input(new_input);
        Ok(BatchSimpleAgg::new(new_logical).into())
    }
}

impl PlanTreeNodeUnary for LogicalAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(self.agg_calls().to_vec(), self.group_key().to_vec(), input)
    }

    #[must_use]
    fn rewrite_with_input(
        &self,
        input: PlanRef,
        input_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let agg = self.rewrite_with_input_agg(input, self.agg_calls(), input_col_change);
        // change the input columns index will not change the output column index
        let out_col_change = ColIndexMapping::identity(agg.schema().len());
        (agg, out_col_change)
    }
}

impl_plan_tree_node_for_unary! {LogicalAgg}

impl fmt::Display for LogicalAgg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_name(f, "LogicalAgg")
    }
}

impl ExprRewritable for LogicalAgg {
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

impl ColPrunable for LogicalAgg {
    fn prune_col(&self, required_cols: &[usize], ctx: &mut ColumnPruningContext) -> PlanRef {
        let group_key_required_cols = FixedBitSet::from_iter(self.group_key().iter().copied());

        let (agg_call_required_cols, agg_calls) = {
            let input_cnt = self.input().schema().fields().len();
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            let new_agg_calls = required_cols
                .iter()
                .filter(|&&index| index >= self.group_key().len())
                .map(|&index| {
                    let index = index - self.group_key().len();
                    let agg_call = self.agg_calls()[index].clone();
                    tmp.extend(agg_call.inputs.iter().map(|x| x.index()));
                    tmp.extend(agg_call.order_by_fields.iter().map(|x| x.input.index()));
                    // collect columns used in aggregate filter expressions
                    for i in &agg_call.filter.conjunctions {
                        tmp.union_with(&i.collect_input_refs(input_cnt));
                    }
                    agg_call
                })
                .collect_vec();
            (tmp, new_agg_calls)
        };

        let input_required_cols = {
            let mut tmp = FixedBitSet::with_capacity(self.input().schema().len());
            tmp.union_with(&group_key_required_cols);
            tmp.union_with(&agg_call_required_cols);
            tmp.ones().collect_vec()
        };
        let input_col_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let agg = {
            let input = self.input().prune_col(&input_required_cols, ctx);
            self.rewrite_with_input_agg(input, &agg_calls, input_col_change)
        };
        let new_output_cols = {
            // group key were never pruned or even re-ordered in current impl
            let mut tmp = (0..agg.group_key().len()).collect_vec();
            tmp.extend(
                required_cols
                    .iter()
                    .filter(|&&index| index >= self.group_key().len()),
            );
            tmp
        };
        if new_output_cols == required_cols {
            // current schema perfectly fit the required columns
            agg.into()
        } else {
            // some columns are not needed, or the order need to be adjusted.
            // so we did a projection to remove/reorder the columns.
            let mapping =
                &ColIndexMapping::with_remaining_columns(&new_output_cols, self.schema().len());
            let output_required_cols = required_cols
                .iter()
                .map(|&idx| mapping.map(idx))
                .collect_vec();
            let src_size = agg.schema().len();
            LogicalProject::with_mapping(
                agg.into(),
                ColIndexMapping::with_remaining_columns(&output_required_cols, src_size),
            )
            .into()
        }
    }
}

impl PredicatePushdown for LogicalAgg {
    fn predicate_pushdown(
        &self,
        predicate: Condition,
        ctx: &mut PredicatePushdownContext,
    ) -> PlanRef {
        let num_group_key = self.group_key().len();
        let num_agg_calls = self.agg_calls().len();
        assert!(num_group_key + num_agg_calls == self.schema().len());

        // SimpleAgg should be skipped because the predicate either references agg_calls
        // or is const.
        // If the filter references agg_calls, we can not push it.
        // When it is constantly true, pushing is useless and may actually cause more evaluation
        // cost of the predicate.
        // When it is constantly false, pushing is wrong - the old plan returns 0 rows but new one
        // returns 1 row.
        if num_group_key == 0 {
            return gen_filter_and_pushdown(self, predicate, Condition::true_cond(), ctx);
        }

        // If the filter references agg_calls, we can not push it.
        let mut agg_call_columns = FixedBitSet::with_capacity(num_group_key + num_agg_calls);
        agg_call_columns.insert_range(num_group_key..num_group_key + num_agg_calls);
        let (agg_call_pred, pushed_predicate) = predicate.split_disjoint(&agg_call_columns);

        // convert the predicate to one that references the child of the agg
        let mut subst = Substitute {
            mapping: self
                .group_key()
                .iter()
                .enumerate()
                .map(|(i, group_key)| {
                    InputRef::new(*group_key, self.schema().fields()[i].data_type()).into()
                })
                .collect(),
        };
        let pushed_predicate = pushed_predicate.rewrite_expr(&mut subst);

        gen_filter_and_pushdown(self, agg_call_pred, pushed_predicate, ctx)
    }
}

impl ToBatch for LogicalAgg {
    fn to_batch(&self) -> Result<PlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let agg_plan = if self.group_key().is_empty() {
            self.to_batch_simple_agg()?
        } else {
            let mut input_order = Order::any();
            let (output_requires_order, group_key_order) =
                self.output_requires_order_on_group_keys(required_order);
            if output_requires_order {
                // Push down sort before aggregation
                input_order = self
                    .o2i_col_mapping()
                    .rewrite_provided_order(&group_key_order);
            }
            let new_input = self.input().to_batch_with_order_required(&input_order)?;
            let new_logical = self.clone_with_input(new_input);
            if self
                .ctx()
                .session_ctx()
                .config()
                .get_batch_enable_sort_agg()
                && (self.input_provides_order_on_group_keys(&new_logical) || output_requires_order)
            {
                BatchSortAgg::new(new_logical).into()
            } else {
                BatchHashAgg::new(new_logical).into()
            }
        };
        required_order.enforce_if_not_satisfies(agg_plan)
    }
}

impl ToStream for LogicalAgg {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        // To rewrite StreamAgg, there are two things to do:
        // 1. insert a RowCount(Count with zero argument) at the beginning of agg_calls of
        // LogicalAgg.
        // 2. increment the index of agg_calls in `out_col_change` by 1 due to
        // the insertion of RowCount, and it will be used to rewrite LogicalProject above this
        // LogicalAgg.
        // Please note that the index of group key need not be changed.

        let mut output_indices = (0..self.schema().len()).collect_vec();
        output_indices
            .iter_mut()
            .skip(self.group_key().len())
            .for_each(|index| {
                *index += 1;
            });
        let agg_calls = iter::once(PlanAggCall::count_star())
            .chain(self.agg_calls().iter().cloned())
            .collect_vec();

        let logical_agg = LogicalAgg::new(agg_calls, self.group_key().to_vec(), self.input());
        let stream_agg = logical_agg.gen_dist_stream_agg_plan(self.input().to_stream(ctx)?)?;

        let stream_project = StreamProject::new(LogicalProject::with_out_col_idx(
            stream_agg,
            output_indices.into_iter(),
        ));
        Ok(stream_project.into())
    }

    fn logical_rewrite_for_stream(
        &self,
        ctx: &mut RewriteStreamContext,
    ) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input().logical_rewrite_for_stream(ctx)?;
        let (agg, out_col_change) = self.rewrite_with_input(input, input_col_change);
        let (map, _) = out_col_change.into_parts();
        let out_col_change = ColIndexMapping::with_target_size(map, agg.schema().len());
        Ok((agg.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::{
        assert_eq_input_ref, input_ref_to_column_indices, AggCall, ExprType, FunctionCall, OrderBy,
    };
    use crate::optimizer::optimizer_context::OptimizerContext;
    use crate::optimizer::plan_node::LogicalValues;

    #[tokio::test]
    async fn test_create() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(vec![], Schema { fields }, ctx);
        let input = PlanRef::from(values);
        let input_ref_1 = InputRef::new(0, ty.clone());
        let input_ref_2 = InputRef::new(1, ty.clone());
        let input_ref_3 = InputRef::new(2, ty.clone());

        let gen_internal_value = |select_exprs: Vec<ExprImpl>,
                                  group_exprs|
         -> (Vec<ExprImpl>, Vec<PlanAggCall>, Vec<usize>) {
            let (plan, exprs, _) =
                LogicalAgg::create(select_exprs, group_exprs, None, input.clone()).unwrap();

            let logical_agg = plan.as_logical_agg().unwrap();
            let agg_calls = logical_agg.agg_calls().to_vec();
            let group_key = logical_agg.group_key().to_vec();

            (exprs, agg_calls, group_key)
        };

        // Test case: select v1 from test group by v1;
        {
            let select_exprs = vec![input_ref_1.clone().into()];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_key) = gen_internal_value(select_exprs, group_exprs);

            assert_eq!(exprs.len(), 1);
            assert_eq_input_ref!(&exprs[0], 0);

            assert_eq!(agg_calls.len(), 0);
            assert_eq!(group_key, vec![0]);
        }

        // Test case: select v1, min(v2) from test group by v1;
        {
            let min_v2 = AggCall::new(
                AggKind::Min,
                vec![input_ref_2.clone().into()],
                false,
                OrderBy::any(),
                Condition::true_cond(),
            )
            .unwrap();
            let select_exprs = vec![input_ref_1.clone().into(), min_v2.into()];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_key) = gen_internal_value(select_exprs, group_exprs);

            assert_eq!(exprs.len(), 2);
            assert_eq_input_ref!(&exprs[0], 0);
            assert_eq_input_ref!(&exprs[1], 1);

            assert_eq!(agg_calls.len(), 1);
            assert_eq!(agg_calls[0].agg_kind, AggKind::Min);
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(group_key, vec![0]);
        }

        // Test case: select v1, min(v2) + max(v3) from t group by v1;
        {
            let min_v2 = AggCall::new(
                AggKind::Min,
                vec![input_ref_2.clone().into()],
                false,
                OrderBy::any(),
                Condition::true_cond(),
            )
            .unwrap();
            let max_v3 = AggCall::new(
                AggKind::Max,
                vec![input_ref_3.clone().into()],
                false,
                OrderBy::any(),
                Condition::true_cond(),
            )
            .unwrap();
            let func_call =
                FunctionCall::new(ExprType::Add, vec![min_v2.into(), max_v3.into()]).unwrap();
            let select_exprs = vec![input_ref_1.clone().into(), ExprImpl::from(func_call)];
            let group_exprs = vec![input_ref_1.clone().into()];

            let (exprs, agg_calls, group_key) = gen_internal_value(select_exprs, group_exprs);

            assert_eq_input_ref!(&exprs[0], 0);
            if let ExprImpl::FunctionCall(func_call) = &exprs[1] {
                assert_eq!(func_call.get_expr_type(), ExprType::Add);
                let inputs = func_call.inputs();
                assert_eq_input_ref!(&inputs[0], 1);
                assert_eq_input_ref!(&inputs[1], 2);
            } else {
                panic!("Wrong expression type!");
            }

            assert_eq!(agg_calls.len(), 2);
            assert_eq!(agg_calls[0].agg_kind, AggKind::Min);
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(agg_calls[1].agg_kind, AggKind::Max);
            assert_eq!(input_ref_to_column_indices(&agg_calls[1].inputs), vec![2]);
            assert_eq!(group_key, vec![0]);
        }

        // Test case: select v2, min(v1 * v3) from test group by v2;
        {
            let v1_mult_v3 = FunctionCall::new(
                ExprType::Multiply,
                vec![input_ref_1.into(), input_ref_3.into()],
            )
            .unwrap();
            let agg_call = AggCall::new(
                AggKind::Min,
                vec![v1_mult_v3.into()],
                false,
                OrderBy::any(),
                Condition::true_cond(),
            )
            .unwrap();
            let select_exprs = vec![input_ref_2.clone().into(), agg_call.into()];
            let group_exprs = vec![input_ref_2.into()];

            let (exprs, agg_calls, group_key) = gen_internal_value(select_exprs, group_exprs);

            assert_eq_input_ref!(&exprs[0], 0);
            assert_eq_input_ref!(&exprs[1], 1);

            assert_eq!(agg_calls.len(), 1);
            assert_eq!(agg_calls[0].agg_kind, AggKind::Min);
            assert_eq!(input_ref_to_column_indices(&agg_calls[0].inputs), vec![1]);
            assert_eq!(group_key, vec![0]);
        }
    }

    /// Generate a agg call node with given [`DataType`] and fields.
    /// For example, `generate_agg_call(Int32, [v1, v2, v3])` will result in:
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    async fn generate_agg_call(ty: DataType, fields: Vec<Field>) -> LogicalAgg {
        let ctx = OptimizerContext::mock().await;

        let values = LogicalValues::new(vec![], Schema { fields }, ctx);
        let agg_call = PlanAggCall {
            agg_kind: AggKind::Min,
            return_type: ty.clone(),
            inputs: vec![InputRef::new(2, ty.clone())],
            distinct: false,
            order_by_fields: vec![],
            filter: Condition::true_cond(),
        };
        LogicalAgg::new(vec![agg_call], vec![1], values.into())
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [0,1] (all columns) will result in
    /// ```text
    /// Agg(min(input_ref(1))) group by (input_ref(0))
    ///  TableScan(v2, v3)
    /// ```
    async fn test_prune_all() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let agg: PlanRef = generate_agg_call(ty.clone(), fields.clone()).await.into();
        // Perform the prune
        let required_cols = vec![0, 1];
        let plan = agg.prune_col(&required_cols, &mut ColumnPruningContext::new(agg.clone()));

        // Check the result
        let agg_new = plan.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), &vec![0]);

        assert_eq!(agg_new.agg_calls().len(), 1);
        let agg_call_new = agg_new.agg_calls()[0].clone();
        assert_eq!(agg_call_new.agg_kind, AggKind::Min);
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![1]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1,0] (all columns, with reversed order) will result in
    /// ```text
    /// Project [input_ref(1), input_ref(0)]
    ///   Agg(min(input_ref(1))) group by (input_ref(0))
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_all_with_order_required() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let agg: PlanRef = generate_agg_call(ty.clone(), fields.clone()).await.into();
        // Perform the prune
        let required_cols = vec![1, 0];
        let plan = agg.prune_col(&required_cols, &mut ColumnPruningContext::new(agg.clone()));
        // Check the result
        let proj = plan.as_logical_project().unwrap();
        assert_eq!(proj.exprs().len(), 2);
        assert_eq!(proj.exprs()[0].as_input_ref().unwrap().index(), 1);
        assert_eq!(proj.exprs()[1].as_input_ref().unwrap().index(), 0);
        let proj_input = proj.input();
        let agg_new = proj_input.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), &vec![0]);

        assert_eq!(agg_new.agg_calls().len(), 1);
        let agg_call_new = agg_new.agg_calls()[0].clone();
        assert_eq!(agg_call_new.agg_kind, AggKind::Min);
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![1]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2))) group by (input_ref(1))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [1] (group key removed) will result in
    /// ```text
    /// Project(input_ref(1))
    ///   Agg(min(input_ref(1))) group by (input_ref(0))
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_group_key() {
        let ctx = OptimizerContext::mock().await;
        let ty = DataType::Int32;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );
        let agg_call = PlanAggCall {
            agg_kind: AggKind::Min,
            return_type: ty.clone(),
            inputs: vec![InputRef::new(2, ty.clone())],
            distinct: false,
            order_by_fields: vec![],
            filter: Condition::true_cond(),
        };
        let agg: PlanRef = LogicalAgg::new(vec![agg_call], vec![1], values.into()).into();

        // Perform the prune
        let required_cols = vec![1];
        let plan = agg.prune_col(&required_cols, &mut ColumnPruningContext::new(agg.clone()));

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 1);
        assert_eq_input_ref!(&project.exprs()[0], 1);
        assert_eq!(project.id().0, 5);

        let agg_new = project.input();
        let agg_new = agg_new.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), &vec![0]);
        assert_eq!(agg_new.id().0, 4);

        assert_eq!(agg_new.agg_calls().len(), 1);
        let agg_call_new = agg_new.agg_calls()[0].clone();
        assert_eq!(agg_call_new.agg_kind, AggKind::Min);
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![1]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }

    #[tokio::test]
    /// Pruning
    /// ```text
    /// Agg(min(input_ref(2)), max(input_ref(1))) group by (input_ref(1), input_ref(2))
    ///   TableScan(v1, v2, v3)
    /// ```
    /// with required columns [0,3] will result in
    /// ```text
    /// Project(input_ref(0), input_ref(2))
    ///   Agg(max(input_ref(0))) group by (input_ref(0), input_ref(1))
    ///     TableScan(v2, v3)
    /// ```
    async fn test_prune_agg() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = vec![
            Field::with_name(ty.clone(), "v1"),
            Field::with_name(ty.clone(), "v2"),
            Field::with_name(ty.clone(), "v3"),
        ];
        let values = LogicalValues::new(
            vec![],
            Schema {
                fields: fields.clone(),
            },
            ctx,
        );

        let agg_calls = vec![
            PlanAggCall {
                agg_kind: AggKind::Min,
                return_type: ty.clone(),
                inputs: vec![InputRef::new(2, ty.clone())],
                distinct: false,
                order_by_fields: vec![],
                filter: Condition::true_cond(),
            },
            PlanAggCall {
                agg_kind: AggKind::Max,
                return_type: ty.clone(),
                inputs: vec![InputRef::new(1, ty.clone())],
                distinct: false,
                order_by_fields: vec![],
                filter: Condition::true_cond(),
            },
        ];
        let agg: PlanRef = LogicalAgg::new(agg_calls, vec![1, 2], values.into()).into();

        // Perform the prune
        let required_cols = vec![0, 3];
        let plan = agg.prune_col(&required_cols, &mut ColumnPruningContext::new(agg.clone()));
        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 0);
        assert_eq_input_ref!(&project.exprs()[1], 2);

        let agg_new = project.input();
        let agg_new = agg_new.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), &vec![0, 1]);

        assert_eq!(agg_new.agg_calls().len(), 1);
        let agg_call_new = agg_new.agg_calls()[0].clone();
        assert_eq!(agg_call_new.agg_kind, AggKind::Max);
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![0]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }
}
