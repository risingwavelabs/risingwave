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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::error::{ErrorCode, Result, TrackingIssue};
use risingwave_common::types::{DataType, Datum, ScalarImpl};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_expr::agg::AggKind;

use super::generic::{self, Agg, GenericPlanRef, PlanAggCall, ProjectBuilder};
use super::utils::impl_distill_by_unit;
use super::{
    BatchHashAgg, BatchSimpleAgg, ColPrunable, ExprRewritable, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamHashAgg, StreamProject, StreamSimpleAgg,
    StreamStatelessSimpleAgg, ToBatch, ToStream,
};
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Literal, OrderBy,
};
use crate::optimizer::plan_node::stream::StreamPlanRef;
use crate::optimizer::plan_node::{
    gen_filter_and_pushdown, BatchSortAgg, ColumnPruningContext, LogicalDedup, LogicalProject,
    PredicatePushdownContext, RewriteStreamContext, ToStreamContext,
};
use crate::optimizer::property::{Distribution, Order, RequiredDist};
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
    core: Agg<PlanRef>,
}

impl LogicalAgg {
    /// Generate plan for stateless 2-phase streaming agg.
    /// Should only be used iff input is distributed. Input must be converted to stream form.
    fn gen_stateless_two_phase_streaming_agg_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        debug_assert!(self.group_key().is_empty());
        let mut logical = self.core.clone();
        logical.input = stream_input;
        let local_agg = StreamStatelessSimpleAgg::new(logical);
        let exchange =
            RequiredDist::single().enforce_if_not_satisfies(local_agg.into(), &Order::any())?;
        let global_agg = new_stream_simple_agg(Agg::new(
            self.agg_calls()
                .iter()
                .enumerate()
                .map(|(partial_output_idx, agg_call)| {
                    agg_call.partial_to_total_agg_call(partial_output_idx)
                })
                .collect(),
            FixedBitSet::new(),
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
        let project = StreamProject::new(generic::Project::new(exprs, stream_input));

        // Generate local agg step
        let mut local_group_key = self.group_key().clone();
        local_group_key.extend_one(vnode_col_idx);
        let n_local_group_key = local_group_key.count_ones(..);
        let local_agg = new_stream_hash_agg(
            Agg::new(self.agg_calls().to_vec(), local_group_key, project.into()),
            Some(vnode_col_idx),
        );
        // Global group key excludes vnode.
        let local_agg_group_key_cardinality = local_agg.group_key().count_ones(..);
        let local_group_key_without_vnode =
            &local_agg.group_key().ones().collect_vec()[..local_agg_group_key_cardinality - 1];
        let global_group_key = local_agg
            .i2o_col_mapping()
            .rewrite_dist_key(local_group_key_without_vnode)
            .expect("some input group key could not be mapped");

        // Generate global agg step
        if self.group_key().count_ones(..) == 0 {
            let exchange =
                RequiredDist::single().enforce_if_not_satisfies(local_agg.into(), &Order::any())?;
            let global_agg = new_stream_simple_agg(Agg::new(
                self.agg_calls()
                    .iter()
                    .enumerate()
                    .map(|(partial_output_idx, agg_call)| {
                        agg_call.partial_to_total_agg_call(n_local_group_key + partial_output_idx)
                    })
                    .collect(),
                global_group_key.into_iter().collect::<FixedBitSet>(),
                exchange,
            ));
            Ok(global_agg.into())
        } else {
            let exchange = RequiredDist::shard_by_key(input_col_num, &global_group_key)
                .enforce_if_not_satisfies(local_agg.into(), &Order::any())?;
            // Local phase should have reordered the group keys into their required order.
            // we can just follow it.
            let global_agg = new_stream_hash_agg(
                Agg::new(
                    self.agg_calls()
                        .iter()
                        .enumerate()
                        .map(|(partial_output_idx, agg_call)| {
                            agg_call
                                .partial_to_total_agg_call(n_local_group_key + partial_output_idx)
                        })
                        .collect(),
                    global_group_key.into_iter().collect::<FixedBitSet>(),
                    exchange,
                ),
                None,
            );
            Ok(global_agg.into())
        }
    }

    fn gen_single_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        let mut logical = self.core.clone();
        let input = RequiredDist::single().enforce_if_not_satisfies(stream_input, &Order::any())?;
        logical.input = input;
        Ok(new_stream_simple_agg(logical).into())
    }

    fn gen_shuffle_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        let input = RequiredDist::shard_by_key(
            stream_input.schema().len(),
            &self.group_key().ones().collect_vec(),
        )
        .enforce_if_not_satisfies(stream_input, &Order::any())?;
        let mut logical = self.core.clone();
        logical.input = input;
        Ok(new_stream_hash_agg(logical, None).into())
    }

    /// See if all stream aggregation calls have a stateless local agg counterpart.
    fn all_local_aggs_are_stateless(&self, stream_input_append_only: bool) -> bool {
        self.agg_calls().iter().all(|c| {
            matches!(c.agg_kind, AggKind::Sum | AggKind::Count)
                || (matches!(c.agg_kind, AggKind::Min | AggKind::Max) && stream_input_append_only)
        })
    }

    /// Generates distributed stream plan.
    fn gen_dist_stream_agg_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        let input_dist = stream_input.distribution();
        debug_assert!(*input_dist != Distribution::Broadcast);

        // Shuffle agg
        // If we have group key, and we won't try two phase agg optimization at all,
        // we will always choose shuffle agg over single agg.
        if !self.group_key().is_empty() && !self.core.must_try_two_phase_agg() {
            return self.gen_shuffle_plan(stream_input);
        }

        // Standalone agg
        // If no group key, and cannot two phase agg, we have to use single plan.
        if self.group_key().is_empty() && !self.core.can_two_phase_agg() {
            return self.gen_single_plan(stream_input);
        }

        debug_assert!(if !self.group_key().is_empty() {
            self.core.must_try_two_phase_agg()
        } else {
            self.core.can_two_phase_agg()
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
            if *input_dist == Distribution::SomeShard && self.core.must_try_two_phase_agg() {
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
                if (!self.core.hash_agg_dist_satisfied_by_input_dist(input_dist)
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

    pub fn core(&self) -> &Agg<PlanRef> {
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
    group_key: FixedBitSet,
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
        Agg::new(self.agg_calls, self.group_key, logical_project.into()).into()
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
                .ones()
                .position(|group_key| group_key == input_index)
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
            if !agg_call.order_by.is_empty() {
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

    fn schema_agg_start_offset(&self) -> usize {
        self.group_key.count_ones(..)
    }

    /// Push a new planned agg call into the builder.
    /// Return an `InputRef` to that agg call.
    /// For existing agg calls, return an `InputRef` to the existing one.
    fn push_agg_call(&mut self, agg_call: PlanAggCall) -> InputRef {
        if let Some((pos, existing)) = self.agg_calls.iter().find_position(|&c| c == &agg_call) {
            return InputRef::new(
                self.schema_agg_start_offset() + pos,
                existing.return_type.clone(),
            );
        }
        let index = self.schema_agg_start_offset() + self.agg_calls.len();
        let data_type = agg_call.return_type.clone();
        self.agg_calls.push(agg_call);
        InputRef::new(index, data_type)
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

        let order_by: Vec<_> = order_by
            .sort_exprs
            .iter()
            .map(|e| {
                let index = self.input_proj_builder.add_expr(&e.expr)?;
                Ok(ColumnOrder::new(index, e.order_type))
            })
            .try_collect()
            .map_err(|err: &'static str| {
                ErrorCode::NotImplemented(
                    format!("{err} inside aggregation calls order by"),
                    None.into(),
                )
            })?;

        match agg_kind {
            // Rewrite avg to cast(sum as avg_return_type) / count.
            AggKind::Avg => {
                assert_eq!(inputs.len(), 1);

                let left_return_type =
                    AggCall::infer_return_type(AggKind::Sum, &[inputs[0].return_type()]).unwrap();
                let left_ref = self.push_agg_call(PlanAggCall {
                    agg_kind: AggKind::Sum,
                    return_type: left_return_type,
                    inputs: inputs.clone(),
                    distinct,
                    order_by: order_by.clone(),
                    filter: filter.clone(),
                });
                let left = ExprImpl::from(left_ref).cast_explicit(return_type).unwrap();

                let right_return_type =
                    AggCall::infer_return_type(AggKind::Count, &[inputs[0].return_type()]).unwrap();
                let right_ref = self.push_agg_call(PlanAggCall {
                    agg_kind: AggKind::Count,
                    return_type: right_return_type,
                    inputs,
                    distinct,
                    order_by,
                    filter,
                });

                Ok(ExprImpl::from(
                    FunctionCall::new(ExprType::Divide, vec![left, right_ref.into()]).unwrap(),
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
                let pre_proj_input = self.input_proj_builder.get_expr(input.index).unwrap();

                // first, we compute sum of squared as sum_sq
                let squared_input_expr = ExprImpl::from(
                    FunctionCall::new(
                        ExprType::Multiply,
                        vec![pre_proj_input.clone(), pre_proj_input.clone()],
                    )
                    .unwrap(),
                );

                let squared_input_proj_index = self
                    .input_proj_builder
                    .add_expr(&squared_input_expr)
                    .unwrap();

                let sum_of_squares_return_type =
                    AggCall::infer_return_type(AggKind::Sum, &[squared_input_expr.return_type()])
                        .unwrap();

                let sum_of_squares_expr = ExprImpl::from(self.push_agg_call(PlanAggCall {
                    agg_kind: AggKind::Sum,
                    return_type: sum_of_squares_return_type,
                    inputs: vec![InputRef::new(
                        squared_input_proj_index,
                        squared_input_expr.return_type(),
                    )],
                    distinct,
                    order_by: order_by.clone(),
                    filter: filter.clone(),
                }))
                .cast_explicit(return_type.clone())
                .unwrap();

                // after that, we compute sum
                let sum_return_type =
                    AggCall::infer_return_type(AggKind::Sum, &[input.return_type()]).unwrap();

                let sum_expr = ExprImpl::from(self.push_agg_call(PlanAggCall {
                    agg_kind: AggKind::Sum,
                    return_type: sum_return_type,
                    inputs: inputs.clone(),
                    distinct,
                    order_by: order_by.clone(),
                    filter: filter.clone(),
                }))
                .cast_explicit(return_type.clone())
                .unwrap();

                // then, we compute count
                let count_return_type =
                    AggCall::infer_return_type(AggKind::Count, &[input.return_type()]).unwrap();

                let count_expr = ExprImpl::from(self.push_agg_call(PlanAggCall {
                    agg_kind: AggKind::Count,
                    return_type: count_return_type,
                    inputs,
                    distinct,
                    order_by,
                    filter,
                }));

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
                        FunctionCall::new(ExprType::Sqrt, vec![target_expr]).unwrap(),
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

            _ => Ok(self
                .push_agg_call(PlanAggCall {
                    agg_kind,
                    return_type,
                    inputs,
                    distinct,
                    order_by,
                    filter,
                })
                .into()),
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

impl From<Agg<PlanRef>> for LogicalAgg {
    fn from(core: Agg<PlanRef>) -> Self {
        let base = PlanBase::new_logical_with_core(&core);
        Self { base, core }
    }
}

/// Because `From`/`Into` are not transitive
impl From<Agg<PlanRef>> for PlanRef {
    fn from(core: Agg<PlanRef>) -> Self {
        LogicalAgg::from(core).into()
    }
}

impl LogicalAgg {
    /// get the Mapping of columnIndex from input column index to out column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.core.i2o_col_mapping()
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
    pub fn group_key(&self) -> &FixedBitSet {
        &self.core.group_key
    }

    pub fn decompose(self) -> (Vec<PlanAggCall>, FixedBitSet, PlanRef) {
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
                agg_call.order_by.iter_mut().for_each(|o| {
                    o.column_index = input_col_change.map(o.column_index);
                });
                agg_call.filter = agg_call.filter.rewrite_expr(&mut input_col_change);
                agg_call
            })
            .collect();
        let group_key = self
            .group_key()
            .ones()
            .map(|key| input_col_change.map(key))
            .collect();
        Agg::new(agg_calls, group_key, input).into()
    }
}

impl PlanTreeNodeUnary for LogicalAgg {
    fn input(&self) -> PlanRef {
        self.core.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Agg::new(self.agg_calls().to_vec(), self.group_key().clone(), input).into()
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

impl_distill_by_unit!(LogicalAgg, core, "LogicalAgg");

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
        let group_key_required_cols = self.group_key().clone();

        let (agg_call_required_cols, agg_calls) = {
            let input_cnt = self.input().schema().len();
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            let group_key_cardinality = self.group_key().count_ones(..);
            let new_agg_calls = required_cols
                .iter()
                .filter(|&&index| index >= group_key_cardinality)
                .map(|&index| {
                    let index = index - group_key_cardinality;
                    let agg_call = self.agg_calls()[index].clone();
                    tmp.extend(agg_call.inputs.iter().map(|x| x.index()));
                    tmp.extend(agg_call.order_by.iter().map(|x| x.column_index));
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
            let group_key_cardinality = agg.group_key().count_ones(..);
            let mut tmp = (0..group_key_cardinality).collect_vec();
            tmp.extend(
                required_cols
                    .iter()
                    .filter(|&&index| index >= group_key_cardinality),
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
        let num_group_key = self.group_key().count_ones(..);
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
                .ones()
                .enumerate()
                .map(|(i, group_key)| {
                    InputRef::new(group_key, self.schema().fields()[i].data_type()).into()
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

    // TODO(rc): `to_batch_with_order_required` seems to be useless after we decide to use
    // `BatchSortAgg` only when input is already sorted
    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
        let input = self.input().to_batch()?;
        let new_logical = Agg {
            input,
            ..self.core.clone()
        };
        let agg_plan = if self.group_key().is_empty() {
            BatchSimpleAgg::new(new_logical).into()
        } else if self
            .ctx()
            .session_ctx()
            .config()
            .get_batch_enable_sort_agg()
            && new_logical.input_provides_order_on_group_keys()
        {
            BatchSortAgg::new(new_logical).into()
        } else {
            BatchHashAgg::new(new_logical).into()
        };
        required_order.enforce_if_not_satisfies(agg_plan)
    }
}

fn find_or_append_row_count(mut logical: Agg<PlanRef>) -> (Agg<PlanRef>, usize) {
    // `HashAgg`/`SimpleAgg` executors require a `count(*)` to correctly build changes, so
    // append a `count(*)` if not exists.
    let count_star = PlanAggCall::count_star();
    let row_count_idx = if let Some((idx, _)) = logical
        .agg_calls
        .iter()
        .find_position(|&c| c == &count_star)
    {
        idx
    } else {
        let idx = logical.agg_calls.len();
        logical.agg_calls.push(count_star);
        idx
    };
    (logical, row_count_idx)
}

fn new_stream_simple_agg(logical: Agg<PlanRef>) -> StreamSimpleAgg {
    let (logical, row_count_idx) = find_or_append_row_count(logical);
    StreamSimpleAgg::new(logical, row_count_idx)
}

fn new_stream_hash_agg(logical: Agg<PlanRef>, vnode_col_idx: Option<usize>) -> StreamHashAgg {
    let (logical, row_count_idx) = find_or_append_row_count(logical);
    StreamHashAgg::new(logical, vnode_col_idx, row_count_idx)
}

impl ToStream for LogicalAgg {
    fn to_stream(&self, ctx: &mut ToStreamContext) -> Result<PlanRef> {
        let eowc = ctx.emit_on_window_close();
        let stream_input = self.input().to_stream(ctx)?;

        // Use Dedup operator, if possible.
        if stream_input.append_only() && self.agg_calls().is_empty() {
            let input = if self.group_key().count_ones(..) != self.input().schema().len() {
                let cols = &self.group_key().ones().collect_vec();
                LogicalProject::with_mapping(
                    self.input(),
                    ColIndexMapping::with_remaining_columns(cols, self.input().schema().len()),
                )
                .into()
            } else {
                self.input()
            };
            let input_schema_len = input.schema().len();
            let logical_dedup = LogicalDedup::new(input, (0..input_schema_len).collect());
            return logical_dedup.to_stream(ctx);
        }

        let plan = self.gen_dist_stream_agg_plan(stream_input)?;

        let (plan, n_final_agg_calls) = if let Some(final_agg) = plan.as_stream_simple_agg() {
            if eowc {
                return Err(ErrorCode::InvalidInputSyntax(
                    "`EMIT ON WINDOW CLOSE` cannot be used for aggregation without `GROUP BY`"
                        .to_string(),
                )
                .into());
            }
            (plan.clone(), final_agg.agg_calls().len())
        } else if let Some(final_agg) = plan.as_stream_hash_agg() {
            (
                if eowc {
                    final_agg.to_eowc_version()?
                } else {
                    plan.clone()
                },
                final_agg.agg_calls().len(),
            )
        } else {
            panic!("the root PlanNode must be either StreamHashAgg or StreamSimpleAgg");
        };

        if self.agg_calls().len() == n_final_agg_calls {
            // an existing `count(*)` is used as row count column in `StreamXxxAgg`
            Ok(plan)
        } else {
            // a `count(*)` is appended, should project the output
            Ok(StreamProject::new(generic::Project::with_out_col_idx(
                plan,
                0..self.schema().len(),
            ))
            .into())
        }
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
         -> (Vec<ExprImpl>, Vec<PlanAggCall>, FixedBitSet) {
            let (plan, exprs, _) =
                LogicalAgg::create(select_exprs, group_exprs, None, input.clone()).unwrap();

            let logical_agg = plan.as_logical_agg().unwrap();
            let agg_calls = logical_agg.agg_calls().to_vec();
            let group_key = logical_agg.group_key().clone();

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
            assert_eq!(group_key, FixedBitSet::from_iter(vec![0]));
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
            assert_eq!(group_key, FixedBitSet::from_iter(vec![0]));
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
                assert_eq!(func_call.func_type(), ExprType::Add);
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
            assert_eq!(group_key, FixedBitSet::from_iter(vec![0]));
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
            assert_eq!(group_key, FixedBitSet::from_iter(vec![0]));
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
            order_by: vec![],
            filter: Condition::true_cond(),
        };
        Agg::new(
            vec![agg_call],
            FixedBitSet::from_iter(vec![1]),
            values.into(),
        )
        .into()
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
        assert_eq!(agg_new.group_key(), &FixedBitSet::from_iter(vec![0]));

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
        assert_eq!(agg_new.group_key(), &FixedBitSet::from_iter(vec![0]));

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
            order_by: vec![],
            filter: Condition::true_cond(),
        };
        let agg: PlanRef = Agg::new(
            vec![agg_call],
            FixedBitSet::from_iter(vec![1]),
            values.into(),
        )
        .into();

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
        assert_eq!(agg_new.group_key(), &FixedBitSet::from_iter(vec![0]));
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
                order_by: vec![],
                filter: Condition::true_cond(),
            },
            PlanAggCall {
                agg_kind: AggKind::Max,
                return_type: ty.clone(),
                inputs: vec![InputRef::new(1, ty.clone())],
                distinct: false,
                order_by: vec![],
                filter: Condition::true_cond(),
            },
        ];
        let agg: PlanRef =
            Agg::new(agg_calls, FixedBitSet::from_iter(vec![1, 2]), values.into()).into();

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
        assert_eq!(agg_new.group_key(), &FixedBitSet::from_iter(vec![0, 1]));

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
