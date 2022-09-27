// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeSet;
use std::{fmt, iter};

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{Field, FieldDisplay, Schema};
use risingwave_common::error::{ErrorCode, Result, TrackingIssue};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_pb::expr::agg_call::OrderByField as ProstAggOrderByField;
use risingwave_pb::expr::AggCall as ProstAggCall;
use risingwave_pb::stream_plan::{agg_call_state, AggCallState as AggCallStateProst};

use super::{
    BatchHashAgg, BatchSimpleAgg, ColPrunable, LogicalProjectBuilder, PlanBase, PlanRef,
    PlanTreeNodeUnary, PredicatePushdown, StreamGlobalSimpleAgg, StreamHashAgg,
    StreamLocalSimpleAgg, StreamProject, ToBatch, ToStream,
};
use crate::catalog::table_catalog::TableCatalog;
use crate::expr::{
    AggCall, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, InputRefDisplay,
    Literal, OrderBy,
};
use crate::optimizer::plan_node::utils::TableCatalogBuilder;
use crate::optimizer::plan_node::{gen_filter_and_pushdown, BatchSortAgg, LogicalProject};
use crate::optimizer::property::Direction::{Asc, Desc};
use crate::optimizer::property::{
    Direction, Distribution, FieldOrder, FunctionalDependencySet, Order, RequiredDist,
};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay, Substitute};

/// Rewritten version of [`crate::expr::OrderByExpr`] which uses `InputRef` instead of `ExprImpl`.
/// Refer to [`LogicalAggBuilder::try_rewrite_agg_call`] for more details.
///
/// TODO(yuchao): replace `PlanAggOrderByField` with enhanced `FieldOrder`
#[derive(Clone)]
pub struct PlanAggOrderByField {
    pub input: InputRef,
    pub direction: Direction,
    pub nulls_first: bool,
}

impl fmt::Debug for PlanAggOrderByField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.input)?;
        match self.direction {
            Direction::Asc => write!(f, " ASC")?,
            Direction::Desc => write!(f, " DESC")?,
            _ => {}
        }
        write!(
            f,
            " NULLS {}",
            if self.nulls_first { "FIRST" } else { "LAST" }
        )?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct PlanAggOrderByFieldDisplay<'a> {
    pub plan_agg_order_by_field: &'a PlanAggOrderByField,
    pub input_schema: &'a Schema,
}

impl fmt::Debug for PlanAggOrderByFieldDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.plan_agg_order_by_field;
        write!(
            f,
            "{:?}",
            InputRefDisplay {
                input_ref: &that.input,
                input_schema: self.input_schema
            }
        )?;
        match that.direction {
            Direction::Asc => write!(f, " ASC")?,
            Direction::Desc => write!(f, " DESC")?,
            _ => {}
        }
        write!(
            f,
            " NULLS {}",
            if that.nulls_first { "FIRST" } else { "LAST" }
        )?;
        Ok(())
    }
}

impl PlanAggOrderByField {
    fn to_protobuf(&self) -> ProstAggOrderByField {
        ProstAggOrderByField {
            input: Some(self.input.to_proto()),
            r#type: Some(self.input.data_type.to_protobuf()),
            direction: self.direction.to_protobuf() as i32,
            nulls_first: self.nulls_first,
        }
    }
}

/// Rewritten version of [`AggCall`] which uses `InputRef` instead of `ExprImpl`.
/// Refer to [`LogicalAggBuilder::try_rewrite_agg_call`] for more details.
#[derive(Clone)]
pub struct PlanAggCall {
    /// Kind of aggregation function
    pub agg_kind: AggKind,

    /// Data type of the returned column
    pub return_type: DataType,

    /// Column indexes of input columns.
    ///
    /// Its length can be:
    /// - 0 (`RowCount`)
    /// - 1 (`Max`, `Min`)
    /// - 2 (`StringAgg`).
    ///
    /// Usually, we mark the first column as the aggregated column.
    pub inputs: Vec<InputRef>,

    pub distinct: bool,
    pub order_by_fields: Vec<PlanAggOrderByField>,
    /// Selective aggregation: only the input rows for which
    /// `filter` evaluates to `true` will be fed to the aggregate function.
    pub filter: Condition,
}

impl fmt::Debug for PlanAggCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.agg_kind)?;
        if !self.inputs.is_empty() {
            write!(f, "(")?;
            for (idx, input) in self.inputs.iter().enumerate() {
                if idx == 0 && self.distinct {
                    write!(f, "distinct ")?;
                }
                write!(f, "{:?}", input)?;
                if idx != (self.inputs.len() - 1) {
                    write!(f, ",")?;
                }
            }
            if !self.order_by_fields.is_empty() {
                let clause_text = self
                    .order_by_fields
                    .iter()
                    .map(|e| format!("{:?}", e))
                    .join(", ");
                write!(f, " order_by({})", clause_text)?;
            }
            write!(f, ")")?;
        }
        if !self.filter.always_true() {
            write!(
                f,
                " filter({:?})",
                self.filter.as_expr_unless_true().unwrap()
            )?;
        }
        Ok(())
    }
}

impl PlanAggCall {
    pub fn to_protobuf(&self) -> ProstAggCall {
        ProstAggCall {
            r#type: self.agg_kind.to_prost().into(),
            return_type: Some(self.return_type.to_protobuf()),
            args: self.inputs.iter().map(InputRef::to_agg_arg_proto).collect(),
            distinct: self.distinct,
            order_by_fields: self
                .order_by_fields
                .iter()
                .map(PlanAggOrderByField::to_protobuf)
                .collect(),
            filter: self
                .filter
                .as_expr_unless_true()
                .map(|expr| expr.to_expr_proto()),
        }
    }

    pub fn partial_to_total_agg_call(&self, partial_output_idx: usize) -> PlanAggCall {
        let total_agg_kind = match &self.agg_kind {
            AggKind::Min | AggKind::Max | AggKind::StringAgg => self.agg_kind,
            AggKind::Count | AggKind::Sum | AggKind::ApproxCountDistinct => AggKind::Sum,
            AggKind::Avg => {
                panic!("Avg aggregation should have been rewritten to Sum+Count")
            }
            AggKind::ArrayAgg => {
                panic!("2-phase ArrayAgg is not supported yet")
            }
        };
        PlanAggCall {
            agg_kind: total_agg_kind,
            inputs: vec![InputRef::new(partial_output_idx, self.return_type.clone())],
            order_by_fields: vec![], // order must make no difference when we use 2-phase agg
            filter: Condition::true_cond(),
            ..self.clone()
        }
    }

    pub fn count_star() -> Self {
        PlanAggCall {
            agg_kind: AggKind::Count,
            return_type: DataType::Int64,
            inputs: vec![],
            distinct: false,
            order_by_fields: vec![],
            filter: Condition::true_cond(),
        }
    }

    pub fn with_condition(mut self, filter: Condition) -> Self {
        self.filter = filter;
        self
    }

    pub fn input_indices(&self) -> Vec<usize> {
        self.inputs.iter().map(|input| input.index()).collect()
    }
}

pub struct PlanAggCallDisplay<'a> {
    pub plan_agg_call: &'a PlanAggCall,
    pub input_schema: &'a Schema,
}

impl fmt::Debug for PlanAggCallDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.plan_agg_call;
        write!(f, "{}", that.agg_kind)?;
        if !that.inputs.is_empty() {
            write!(f, "(")?;
            for (idx, input) in that.inputs.iter().enumerate() {
                if idx == 0 && that.distinct {
                    write!(f, "distinct ")?;
                }
                write!(
                    f,
                    "{}",
                    InputRefDisplay {
                        input_ref: input,
                        input_schema: self.input_schema
                    }
                )?;
                if idx != (that.inputs.len() - 1) {
                    write!(f, ", ")?;
                }
            }
            if !that.order_by_fields.is_empty() {
                write!(
                    f,
                    " order_by({})",
                    that.order_by_fields.iter().format_with(", ", |e, f| {
                        f(&format_args!(
                            "{:?}",
                            PlanAggOrderByFieldDisplay {
                                plan_agg_order_by_field: e,
                                input_schema: self.input_schema,
                            }
                        ))
                    })
                )?;
            }
            write!(f, ")")?;
        }

        if !that.filter.always_true() {
            write!(
                f,
                " filter({:?})",
                ConditionDisplay {
                    condition: &that.filter,
                    input_schema: self.input_schema,
                }
            )?;
        }
        Ok(())
    }
}

/// `LogicalAgg` groups input data by their group key and computes aggregation functions.
///
/// It corresponds to the `GROUP BY` operator in a SQL query statement together with the aggregate
/// functions in the `SELECT` clause.
///
/// The output schema will first include the group key and then the aggregation calls.
#[derive(Clone, Debug)]
pub struct LogicalAgg {
    pub base: PlanBase,
    agg_calls: Vec<PlanAggCall>,
    group_key: Vec<usize>,
    input: PlanRef,
}

pub enum AggCallState {
    ResultValueState,
    MaterializedInputState(MaterializedAggInputState),
}
impl AggCallState {
    pub fn to_prost(self, state: &mut BuildFragmentGraphState) -> AggCallStateProst {
        AggCallStateProst {
            inner: Some(match self {
                AggCallState::ResultValueState => agg_call_state::Inner::ResultValueState(
                    risingwave_pb::stream_plan::AggResultState {},
                ),
                AggCallState::MaterializedInputState(s) => {
                    agg_call_state::Inner::MaterializedState(
                        risingwave_pb::stream_plan::MaterializedAggInputState {
                            table: Some(
                                s.table
                                    .with_id(state.gen_table_id_wrapped())
                                    .to_internal_table_prost(),
                            ),
                            column_mapping: s.column_mapping.into_iter().map(|x| x as _).collect(),
                        },
                    )
                }
            }),
        }
    }
}
struct MaterializedAggInputState {
    pub table: TableCatalog,
    pub column_mapping: Vec<usize>,
}
impl LogicalAgg {
    pub fn infer_result_table(&self, vnode_col_idx: Option<usize>) -> TableCatalog {
        let out_fields = self.base.schema.fields();
        let in_fields = self.input().schema().fields().to_vec();
        let in_dist_key = self.input().distribution().dist_column_indices().to_vec();
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(self.ctx().inner().with_options.internal_table_subset());
        for field in out_fields.iter() {
            let tb_column_idx = internal_table_catalog_builder.add_column(field);
            if tb_column_idx < self.group_key().len() {
                internal_table_catalog_builder
                    .add_order_column(tb_column_idx, OrderType::Ascending);
            }
        }
        let mapping = self.i2o_col_mapping();
        let tb_dist = mapping.rewrite_dist_key(&in_dist_key).unwrap_or_default();
        if let Some(tb_vnode_idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
            internal_table_catalog_builder.set_vnode_col_idx(tb_vnode_idx);
        }
        internal_table_catalog_builder.build(tb_dist)
    }

    /// return StreamAgg's AggCallStates
    pub fn infer_stream_agg_state(&self, vnode_col_idx: Option<usize>) -> Vec<AggCallState> {
        let out_fields = self.base.schema.fields();
        let in_fields = self.input().schema().fields().to_vec();
        let in_pks = self.input().logical_pk().to_vec();
        let in_append_only = self.input.append_only();
        let in_dist_key = self.input().distribution().dist_column_indices().to_vec();
        let mut column_mapping = vec![];
        let get_sorted_input_state_table = |sort_keys: Vec<(OrderType, usize)>,
                                            include_keys: Vec<usize>|
         -> MaterializedAggInputState {
            let mut internal_table_catalog_builder =
                TableCatalogBuilder::new(self.ctx().inner().with_options.internal_table_subset());

            for &idx in &self.group_key {
                let tb_column_idx = internal_table_catalog_builder.add_column(&in_fields[idx]);
                internal_table_catalog_builder
                    .add_order_column(tb_column_idx, OrderType::Ascending);
                column_mapping.push(idx);
            }

            for (order_type, idx) in sort_keys {
                let tb_column_idx = internal_table_catalog_builder.add_column(&in_fields[idx]);
                internal_table_catalog_builder.add_order_column(tb_column_idx, order_type);
                column_mapping.push(idx);
            }

            // Add upstream pk.
            for pk_index in &in_pks {
                let tb_column_idx =
                    internal_table_catalog_builder.add_column(&in_fields[*pk_index]);
                internal_table_catalog_builder
                    .add_order_column(tb_column_idx, OrderType::Ascending);
                // TODO: Dedup input pks and group key.
                column_mapping.push(*pk_index);
            }

            for include_key in include_keys {
                internal_table_catalog_builder.add_column(&in_fields[include_key]);
                column_mapping.push(include_key);
            }
            let mapping = ColIndexMapping::with_column_mapping(&column_mapping, in_fields.len());
            let tb_dist = mapping.rewrite_dist_key(&in_dist_key);
            if let Some(tb_vnode_idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
                internal_table_catalog_builder.set_vnode_col_idx(tb_vnode_idx);
            }
            MaterializedAggInputState {
                table: internal_table_catalog_builder.build(tb_dist.unwrap_or_default()),
                column_mapping,
            }
        };

        self.agg_calls
            .iter()
            .enumerate()
            .map(|(agg_idx, agg_call)| match agg_call.agg_kind {
                AggKind::Min | AggKind::Max | AggKind::StringAgg | AggKind::ArrayAgg => {
                    if !in_append_only {
                        let mut sort_column_set = BTreeSet::new();
                        let sort_keys = {
                            match agg_call.agg_kind {
                                AggKind::Min => {
                                    vec![(OrderType::Ascending, agg_call.inputs[0].index)]
                                }
                                AggKind::Max => {
                                    vec![(OrderType::Descending, agg_call.inputs[0].index)]
                                }
                                AggKind::StringAgg | AggKind::ArrayAgg => agg_call
                                    .order_by_fields
                                    .iter()
                                    .map(|o| {
                                        let col_idx = o.input.index;
                                        sort_column_set.insert(col_idx);
                                        (o.direction.to_order(), col_idx)
                                    })
                                    .collect(),
                                _ => unreachable!(),
                            }
                        };

                        let include_keys = match agg_call.agg_kind {
                            AggKind::StringAgg | AggKind::ArrayAgg => agg_call
                                .inputs
                                .iter()
                                .map(|i| i.index)
                                .filter(|i| !sort_column_set.contains(i))
                                .collect(),
                            _ => vec![],
                        };
                        AggCallState::MaterializedInputState(get_sorted_input_state_table(
                            sort_keys,
                            include_keys,
                        ))
                    } else {
                        AggCallState::ResultValueState
                    }
                }
                AggKind::Sum | AggKind::Count | AggKind::Avg | AggKind::ApproxCountDistinct => {
                    AggCallState::ResultValueState
                }
            })
            .collect()
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
                    agg_call.partial_to_total_agg_call(partial_output_idx)
                })
                .collect(),
            vec![],
            exchange,
        ));
        Ok(global_agg.into())
    }

    /// Generate plan for stateless/stateful 2-phase streaming agg.
    /// Should only be used iff input is distributed. Input must be converted to stream form.
    fn gen_vnode_two_phase_streaming_agg_plan(
        &self,
        stream_input: PlanRef,
        dist_key: &[usize],
    ) -> Result<PlanRef> {
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
        let project = StreamProject::new(LogicalProject::new(stream_input, exprs));

        let mut local_group_key = self.group_key().to_vec();
        local_group_key.push(vnode_col_idx);
        let n_local_group_key = local_group_key.len();
        let local_agg = StreamHashAgg::new(
            LogicalAgg::new(self.agg_calls().to_vec(), local_group_key, project.into()),
            Some(vnode_col_idx),
        );

        if self.group_key().is_empty() {
            let exchange =
                RequiredDist::single().enforce_if_not_satisfies(local_agg.into(), &Order::any())?;
            let global_agg = StreamGlobalSimpleAgg::new(LogicalAgg::new(
                self.agg_calls()
                    .iter()
                    .enumerate()
                    .map(|(partial_output_idx, agg_call)| {
                        agg_call.partial_to_total_agg_call(n_local_group_key + partial_output_idx)
                    })
                    .collect(),
                self.group_key().to_vec(),
                exchange,
            ));
            Ok(global_agg.into())
        } else {
            let exchange = RequiredDist::shard_by_key(input_col_num, self.group_key())
                .enforce_if_not_satisfies(local_agg.into(), &Order::any())?;
            let global_agg = StreamHashAgg::new(
                LogicalAgg::new(
                    self.agg_calls()
                        .iter()
                        .enumerate()
                        .map(|(partial_output_idx, agg_call)| {
                            agg_call
                                .partial_to_total_agg_call(n_local_group_key + partial_output_idx)
                        })
                        .collect(),
                    self.group_key().to_vec(),
                    exchange,
                ),
                None,
            );
            Ok(global_agg.into())
        }
    }

    fn gen_dist_stream_agg_plan(&self, stream_input: PlanRef) -> Result<PlanRef> {
        // having group key, is not simple agg. we will just use shuffle agg
        // TODO(stonepage): in some situation the 2-phase agg is better. maybe some switch or
        // hints for it.
        if !self.group_key().is_empty() {
            return Ok(StreamHashAgg::new(
                self.clone_with_input(
                    RequiredDist::shard_by_key(stream_input.schema().len(), self.group_key())
                        .enforce_if_not_satisfies(stream_input, &Order::any())?,
                ),
                None,
            )
            .into());
        }

        // now only simple agg
        let input_dist = stream_input.distribution().clone();
        let input_append_only = stream_input.append_only();

        let gen_single_plan = |stream_input: PlanRef| -> Result<PlanRef> {
            Ok(StreamGlobalSimpleAgg::new(self.clone_with_input(
                RequiredDist::single().enforce_if_not_satisfies(stream_input, &Order::any())?,
            ))
            .into())
        };

        // some agg function can not rewrite to 2-phase agg
        // we can only generate stand alone plan for the simple agg
        let all_agg_calls_can_use_two_phase = self.agg_calls.iter().all(|c| {
            matches!(
                c.agg_kind,
                AggKind::Min | AggKind::Max | AggKind::Sum | AggKind::Count
            ) && c.order_by_fields.is_empty()
        });
        if !all_agg_calls_can_use_two_phase {
            return gen_single_plan(stream_input);
        }

        // stateless 2-phase simple agg
        // can be applied on stateless simple agg calls with input distributed by any shard
        let all_local_are_stateless = self.agg_calls.iter().all(|c| {
            matches!(c.agg_kind, AggKind::Sum | AggKind::Count)
                || (matches!(c.agg_kind, AggKind::Min | AggKind::Max) && input_append_only)
        });
        if all_local_are_stateless && input_dist.satisfies(&RequiredDist::AnyShard) {
            return self.gen_stateless_two_phase_streaming_agg_plan(stream_input);
        }

        // try to use the vnode-based 2-phase simple agg
        // can be applied on agg calls not affected by order with input distributed by dist_key
        match input_dist {
            Distribution::Single | Distribution::SomeShard => gen_single_plan(stream_input),
            Distribution::Broadcast => unreachable!(),
            Distribution::HashShard(dists) | Distribution::UpstreamHashShard(dists) => {
                self.gen_vnode_two_phase_streaming_agg_plan(stream_input, &dists)
            }
        }
    }

    /// Check if the aggregation result will be affected by order by clause, if any.
    pub(crate) fn is_agg_result_affected_by_order(&self) -> bool {
        self.agg_calls
            .iter()
            .any(|call| matches!(call.agg_kind, AggKind::StringAgg | AggKind::ArrayAgg))
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
}

/// `LogicalAggBuilder` extracts agg calls and references to group columns from select list and
/// build the plan like `LogicalAgg - LogicalProject`.
/// it is constructed by `group_exprs` and collect and rewrite the expression in selection and
/// having clause.
struct LogicalAggBuilder {
    /// the builder of the input Project
    input_proj_builder: LogicalProjectBuilder,
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
        let mut input_proj_builder = LogicalProjectBuilder::default();

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
        let logical_project = self.input_proj_builder.build(input);

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
        let mut has_non_distinct_string_agg = false;
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
        let (agg_kind, inputs, distinct, mut order_by, filter) = agg_call.decompose();
        match &agg_kind {
            AggKind::Min
            | AggKind::Max
            | AggKind::Sum
            | AggKind::Count
            | AggKind::Avg
            | AggKind::ApproxCountDistinct => {
                // this order by is unnecessary.
                order_by = OrderBy::new(vec![]);
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

        if agg_kind == AggKind::Avg {
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
                AggCall::infer_return_type(&AggKind::Count, &[inputs[0].return_type()]).unwrap();

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
        } else {
            self.agg_calls.push(PlanAggCall {
                agg_kind,
                return_type: return_type.clone(),
                inputs,
                distinct,
                order_by_fields,
                filter,
            });
            Ok(InputRef::new(self.group_key.len() + self.agg_calls.len() - 1, return_type).into())
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
        if subquery.is_correlated() {
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
        let schema = Self::derive_schema(input.schema(), &group_key, &agg_calls);
        // there is only one row in simple agg's output, so its pk_indices is empty
        let pk_indices = (0..group_key.len()).into_iter().collect_vec();
        let functional_dependency = Self::derive_fd(
            schema.len(),
            input.schema().len(),
            input.functional_dependency(),
            &group_key,
        );
        let base = PlanBase::new_logical(ctx, schema, pk_indices, functional_dependency);
        Self {
            base,
            agg_calls,
            group_key,
            input,
        }
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let agg_cal_num = self.agg_calls().len();
        let group_key = self.group_key();
        let mut map = vec![None; agg_cal_num + group_key.len()];
        for (i, key) in group_key.iter().enumerate() {
            map[i] = Some(*key);
        }
        ColIndexMapping::with_target_size(map, input_len)
    }

    /// get the Mapping of columnIndex from input column index to out column index
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.o2i_col_mapping().inverse()
    }

    fn derive_schema(input: &Schema, group_key: &[usize], agg_calls: &[PlanAggCall]) -> Schema {
        let fields = group_key
            .iter()
            .cloned()
            .map(|i| input.fields()[i].clone())
            .chain(agg_calls.iter().map(|agg_call| {
                let plan_agg_call_display = PlanAggCallDisplay {
                    plan_agg_call: agg_call,
                    input_schema: input,
                };
                let name = format!("{:?}", plan_agg_call_display);
                Field::with_name(agg_call.return_type.clone(), name)
            }))
            .collect();
        Schema { fields }
    }

    fn derive_fd(
        column_cnt: usize,
        input_len: usize,
        input_fd_set: &FunctionalDependencySet,
        group_key: &[usize],
    ) -> FunctionalDependencySet {
        let mut fd_set = FunctionalDependencySet::with_key(
            column_cnt,
            &(0..group_key.len()).into_iter().collect_vec(),
        );
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
        if select_exprs
            .iter()
            .chain(group_exprs.iter())
            .any(|e| e.has_table_function())
        {
            return Err(ErrorCode::NotImplemented(
                "Table functions in agg call or group by is not supported yet".to_string(),
                3814.into(),
            )
            .into());
        }

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
    pub fn agg_calls(&self) -> &[PlanAggCall] {
        self.agg_calls.as_ref()
    }

    pub fn agg_calls_display(&self) -> Vec<PlanAggCallDisplay<'_>> {
        self.agg_calls()
            .iter()
            .map(|plan_agg_call| PlanAggCallDisplay {
                plan_agg_call,
                input_schema: self.input.schema(),
            })
            .collect_vec()
    }

    pub fn group_key_display(&self) -> Vec<FieldDisplay<'_>> {
        self.group_key()
            .iter()
            .copied()
            .map(|i| FieldDisplay(self.input.schema().fields.get(i).unwrap()))
            .collect_vec()
    }

    /// Get a reference to the logical agg's group key.
    pub fn group_key(&self) -> &[usize] {
        self.group_key.as_ref()
    }

    pub fn decompose(self) -> (Vec<PlanAggCall>, Vec<usize>, PlanRef) {
        (self.agg_calls, self.group_key, self.input)
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
            .group_key
            .iter()
            .cloned()
            .map(|key| input_col_change.map(key))
            .collect();
        Self::new(agg_calls, group_key, input)
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        if !self.group_key.is_empty() {
            builder.field("group_key", &self.group_key_display());
        }
        builder.field("aggs", &self.agg_calls_display());
        builder.finish()
    }
}

impl PlanTreeNodeUnary for LogicalAgg {
    fn input(&self) -> PlanRef {
        self.input.clone()
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
        let agg = self.rewrite_with_input_agg(input, &self.agg_calls, input_col_change);
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

impl ColPrunable for LogicalAgg {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let group_key_required_cols = FixedBitSet::from_iter(self.group_key.iter().copied());

        let (agg_call_required_cols, agg_calls) = {
            let input_cnt = self.input().schema().fields().len();
            let mut tmp = FixedBitSet::with_capacity(input_cnt);
            let new_agg_calls = required_cols
                .iter()
                .filter(|&&index| index >= self.group_key.len())
                .map(|&index| {
                    let index = index - self.group_key.len();
                    let agg_call = self.agg_calls[index].clone();
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
            let mut tmp = FixedBitSet::with_capacity(self.input.schema().len());
            tmp.union_with(&group_key_required_cols);
            tmp.union_with(&agg_call_required_cols);
            tmp.ones().collect_vec()
        };
        let input_col_change = ColIndexMapping::with_remaining_columns(
            &input_required_cols,
            self.input().schema().len(),
        );
        let agg = {
            let input = self.input.prune_col(&input_required_cols);
            self.rewrite_with_input_agg(input, &agg_calls, input_col_change)
        };
        let new_output_cols = {
            // group key were never pruned or even re-ordered in current impl
            let mut tmp = (0..agg.group_key().len()).collect_vec();
            tmp.extend(
                required_cols
                    .iter()
                    .filter(|&&index| index >= self.group_key.len()),
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
    fn predicate_pushdown(&self, predicate: Condition) -> PlanRef {
        let num_group_key = self.group_key.len();
        let num_agg_calls = self.agg_calls.len();
        assert!(num_group_key + num_agg_calls == self.schema().len());

        // SimpleAgg should be skipped because the predicate either references agg_calls
        // or is const.
        // If the filter references agg_calls, we can not push it.
        // When it is constantly true, pushing is useless and may actually cause more evaluation
        // cost of the predicate.
        // When it is constantly false, pushing is wrong - the old plan returns 0 rows but new one
        // returns 1 row.
        if num_group_key == 0 {
            return gen_filter_and_pushdown(self, predicate, Condition::true_cond());
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

        gen_filter_and_pushdown(self, agg_call_pred, pushed_predicate)
    }
}

impl ToBatch for LogicalAgg {
    fn to_batch(&self) -> Result<PlanRef> {
        self.to_batch_with_order_required(&Order::any())
    }

    fn to_batch_with_order_required(&self, required_order: &Order) -> Result<PlanRef> {
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
        if self.group_key().is_empty() {
            required_order.enforce_if_not_satisfies(BatchSimpleAgg::new(new_logical).into())
        } else if self.input_provides_order_on_group_keys(&new_logical) || output_requires_order {
            required_order.enforce_if_not_satisfies(BatchSortAgg::new(new_logical).into())
        } else {
            required_order.enforce_if_not_satisfies(BatchHashAgg::new(new_logical).into())
        }
    }
}

impl ToStream for LogicalAgg {
    fn to_stream(&self) -> Result<PlanRef> {
        // To rewrite StreamAgg, there are two things to do:
        // 1. insert a RowCount(Count with zero argument) at the beginning of agg_calls of
        // LogicalAgg.
        // 2. increment the index of agg_calls in `out_col_change` by 1 due to
        // the insertion of RowCount, and it will be used to rewrite LogicalProject above this
        // LogicalAgg.
        // Please note that the index of group key need not be changed.

        let mut output_indices = (0..self.schema().len()).into_iter().collect_vec();
        output_indices
            .iter_mut()
            .skip(self.group_key.len())
            .for_each(|index| {
                *index += 1;
            });
        let agg_calls = iter::once(PlanAggCall::count_star())
            .chain(self.agg_calls().iter().cloned())
            .collect_vec();

        let logical_agg = LogicalAgg::new(agg_calls, self.group_key().to_vec(), self.input());
        let stream_agg = logical_agg.gen_dist_stream_agg_plan(self.input().to_stream()?)?;

        let stream_project = StreamProject::new(LogicalProject::with_out_col_idx(
            stream_agg,
            output_indices.into_iter(),
        ));
        Ok(stream_project.into())
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (input, input_col_change) = self.input.logical_rewrite_for_stream()?;
        let (agg, out_col_change) = self.rewrite_with_input(input, input_col_change);
        let (map, _) = out_col_change.into_parts();
        let out_col_change = ColIndexMapping::with_target_size(map, agg.schema().len());
        Ok((agg.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use risingwave_common::catalog::Field;
    use risingwave_common::types::DataType;

    use super::*;
    use crate::expr::{
        assert_eq_input_ref, input_ref_to_column_indices, AggCall, ExprType, FunctionCall, OrderBy,
    };
    use crate::optimizer::plan_node::LogicalValues;
    use crate::session::OptimizerContext;

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
        let input = Rc::new(values);
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
        let agg = generate_agg_call(ty.clone(), fields.clone()).await;
        // Perform the prune
        let required_cols = vec![0, 1];
        let plan = agg.prune_col(&required_cols);

        // Check the result
        let agg_new = plan.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), vec![0]);

        assert_eq!(agg_new.agg_calls.len(), 1);
        let agg_call_new = agg_new.agg_calls[0].clone();
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
        let agg = generate_agg_call(ty.clone(), fields.clone()).await;
        // Perform the prune
        let required_cols = vec![1, 0];
        let plan = agg.prune_col(&required_cols);
        // Check the result
        let proj = plan.as_logical_project().unwrap();
        assert_eq!(proj.exprs().len(), 2);
        assert_eq!(proj.exprs()[0].as_input_ref().unwrap().index(), 1);
        assert_eq!(proj.exprs()[1].as_input_ref().unwrap().index(), 0);
        let proj_input = proj.input();
        let agg_new = proj_input.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), vec![0]);

        assert_eq!(agg_new.agg_calls.len(), 1);
        let agg_call_new = agg_new.agg_calls[0].clone();
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
        let agg = LogicalAgg::new(vec![agg_call], vec![1], values.into());

        // Perform the prune
        let required_cols = vec![1];
        let plan = agg.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 1);
        assert_eq_input_ref!(&project.exprs()[0], 1);
        assert_eq!(project.id().0, 4);

        let agg_new = project.input();
        let agg_new = agg_new.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), vec![0]);
        assert_eq!(agg_new.id().0, 3);

        assert_eq!(agg_new.agg_calls.len(), 1);
        let agg_call_new = agg_new.agg_calls[0].clone();
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
        let agg = LogicalAgg::new(agg_calls, vec![1, 2], values.into());

        // Perform the prune
        let required_cols = vec![0, 3];
        let plan = agg.prune_col(&required_cols);
        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 0);
        assert_eq_input_ref!(&project.exprs()[1], 2);

        let agg_new = project.input();
        let agg_new = agg_new.as_logical_agg().unwrap();
        assert_eq!(agg_new.group_key(), vec![0, 1]);

        assert_eq!(agg_new.agg_calls.len(), 1);
        let agg_call_new = agg_new.agg_calls[0].clone();
        assert_eq!(agg_call_new.agg_kind, AggKind::Max);
        assert_eq!(input_ref_to_column_indices(&agg_call_new.inputs), vec![0]);
        assert_eq!(agg_call_new.return_type, ty);

        let values = agg_new.input();
        let values = values.as_logical_values().unwrap();
        assert_eq!(values.schema().fields(), &fields[1..]);
    }
}
