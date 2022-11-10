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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::catalog::{ColumnDesc, Field, FieldDisplay, Schema, TableDesc};
use risingwave_common::types::{DataType, IntervalUnit};
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_pb::expr::agg_call::OrderByField as ProstAggOrderByField;
use risingwave_pb::expr::AggCall as ProstAggCall;
use risingwave_pb::plan_common::JoinType;
use risingwave_pb::stream_plan::{agg_call_state, AggCallState as AggCallStateProst};

use super::stream;
use super::utils::{IndicesDisplay, TableCatalogBuilder};
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::{ColumnId, IndexCatalog};
use crate::expr::{Expr, ExprDisplay, ExprImpl, InputRef, InputRefDisplay};
use crate::optimizer::property::{Direction, Order};
use crate::session::OptimizerContextRef;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{ColIndexMapping, Condition, ConditionDisplay};
use crate::TableCatalog;

pub trait GenericPlanRef {
    fn schema(&self) -> &Schema;
    fn logical_pk(&self) -> &[usize];
    fn ctx(&self) -> OptimizerContextRef;
}

#[derive(Clone, Debug)]
pub struct DynamicFilter<PlanRef> {
    /// The predicate (formed with exactly one of < , <=, >, >=)
    pub predicate: Condition,
    // dist_key_l: Distribution,
    pub left_index: usize,
    pub left: PlanRef,
    pub right: PlanRef,
}

pub mod dynamic_filter {
    use risingwave_common::util::sort_util::OrderType;

    use crate::optimizer::plan_node::stream;
    use crate::optimizer::plan_node::utils::TableCatalogBuilder;
    use crate::TableCatalog;

    pub fn infer_left_internal_table_catalog(
        me: &impl stream::StreamPlanRef,
        left_key_index: usize,
    ) -> TableCatalog {
        let schema = me.schema();

        let dist_keys = me.distribution().dist_column_indices().to_vec();

        // The pk of dynamic filter internal table should be left_key + input_pk.
        let mut pk_indices = vec![left_key_index];
        // TODO(yuhao): dedup the dist key and pk.
        pk_indices.extend(me.logical_pk());

        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(me.ctx().inner().with_options.internal_table_subset());

        schema.fields().iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });

        pk_indices.iter().for_each(|idx| {
            internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending)
        });

        internal_table_catalog_builder.build(dist_keys)
    }

    pub fn infer_right_internal_table_catalog(input: &impl stream::StreamPlanRef) -> TableCatalog {
        let schema = input.schema();

        // We require that the right table has distribution `Single`
        assert_eq!(
            input.distribution().dist_column_indices().to_vec(),
            Vec::<usize>::new()
        );

        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(input.ctx().inner().with_options.internal_table_subset());

        schema.fields().iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });

        // No distribution keys
        internal_table_catalog_builder.build(vec![])
    }
}

/// [`HopWindow`] implements Hop Table Function.
#[derive(Debug, Clone)]
pub struct HopWindow<PlanRef> {
    pub input: PlanRef,
    pub(super) time_col: InputRef,
    pub(super) window_slide: IntervalUnit,
    pub(super) window_size: IntervalUnit,
    pub(super) output_indices: Vec<usize>,
}

impl<PlanRef: GenericPlanRef> HopWindow<PlanRef> {
    pub fn into_parts(self) -> (PlanRef, InputRef, IntervalUnit, IntervalUnit, Vec<usize>) {
        (
            self.input,
            self.time_col,
            self.window_slide,
            self.window_size,
            self.output_indices,
        )
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let output_type = DataType::window_of(&self.time_col.data_type).unwrap();
        write!(
            f,
            "{} {{ time_col: {}, slide: {}, size: {}, output: {} }}",
            name,
            format_args!(
                "{}",
                InputRefDisplay {
                    input_ref: &self.time_col,
                    input_schema: self.input.schema()
                }
            ),
            self.window_slide,
            self.window_size,
            if self
                .output_indices
                .iter()
                .copied()
                // Behavior is the same as `LogicalHopWindow::internal_column_num`
                .eq(0..(self.input.schema().len() + 2))
            {
                "all".to_string()
            } else {
                let original_schema: Schema = self
                    .input
                    .schema()
                    .clone()
                    .into_fields()
                    .into_iter()
                    .chain([
                        Field::with_name(output_type.clone(), "window_start"),
                        Field::with_name(output_type, "window_end"),
                    ])
                    .collect();
                format!(
                    "{:?}",
                    &IndicesDisplay {
                        indices: &self.output_indices,
                        input_schema: &original_schema,
                    }
                )
            },
        )
    }
}

/// [`Agg`] groups input data by their group key and computes aggregation functions.
///
/// It corresponds to the `GROUP BY` operator in a SQL query statement together with the aggregate
/// functions in the `SELECT` clause.
///
/// The output schema will first include the group key and then the aggregation calls.
#[derive(Clone, Debug)]
pub struct Agg<PlanRef> {
    pub agg_calls: Vec<PlanAggCall>,
    pub group_key: Vec<usize>,
    pub input: PlanRef,
}

pub enum AggCallState {
    ResultValue,
    Table(Box<TableState>),
    MaterializedInput(Box<MaterializedInputState>),
}

impl AggCallState {
    pub fn into_prost(self, state: &mut BuildFragmentGraphState) -> AggCallStateProst {
        AggCallStateProst {
            inner: Some(match self {
                AggCallState::ResultValue => {
                    agg_call_state::Inner::ResultValueState(agg_call_state::ResultValueState {})
                }
                AggCallState::Table(s) => {
                    agg_call_state::Inner::TableState(agg_call_state::TableState {
                        table: Some(
                            s.table
                                .with_id(state.gen_table_id_wrapped())
                                .to_internal_table_prost(),
                        ),
                    })
                }
                AggCallState::MaterializedInput(s) => {
                    agg_call_state::Inner::MaterializedInputState(
                        agg_call_state::MaterializedInputState {
                            table: Some(
                                s.table
                                    .with_id(state.gen_table_id_wrapped())
                                    .to_internal_table_prost(),
                            ),
                            included_upstream_indices: s
                                .included_upstream_indices
                                .into_iter()
                                .map(|x| x as _)
                                .collect(),
                            table_value_indices: s
                                .table_value_indices
                                .into_iter()
                                .map(|x| x as _)
                                .collect(),
                        },
                    )
                }
            }),
        }
    }
}

pub struct TableState {
    pub table: TableCatalog,
}

pub struct MaterializedInputState {
    pub table: TableCatalog,
    pub included_upstream_indices: Vec<usize>,
    pub table_value_indices: Vec<usize>,
}

impl<PlanRef: stream::StreamPlanRef> Agg<PlanRef> {
    /// Infer `AggCallState`s for streaming agg.
    pub fn infer_stream_agg_state(
        &self,
        me: &impl stream::StreamPlanRef,
        vnode_col_idx: Option<usize>,
    ) -> Vec<AggCallState> {
        let in_fields = self.input.schema().fields().to_vec();
        let in_pks = self.input.logical_pk().to_vec();
        let in_append_only = self.input.append_only();
        let in_dist_key = self.input.distribution().dist_column_indices().to_vec();

        let gen_materialized_input_state = |sort_keys: Vec<(OrderType, usize)>,
                                            include_keys: Vec<usize>|
         -> MaterializedInputState {
            let mut internal_table_catalog_builder =
                TableCatalogBuilder::new(me.ctx().inner().with_options.internal_table_subset());

            let mut included_upstream_indices = vec![]; // all upstream indices that are included in the state table
            let mut column_mapping = BTreeMap::new(); // key: upstream col idx, value: table col idx
            let mut table_value_indices = BTreeSet::new(); // table column indices of value columns
            let mut add_column = |upstream_idx, order_type, is_value| {
                column_mapping.entry(upstream_idx).or_insert_with(|| {
                    let table_col_idx =
                        internal_table_catalog_builder.add_column(&in_fields[upstream_idx]);
                    if let Some(order_type) = order_type {
                        internal_table_catalog_builder.add_order_column(table_col_idx, order_type);
                    }
                    included_upstream_indices.push(upstream_idx);
                    table_col_idx
                });
                if is_value {
                    // note that some indices may be added before as group keys which are not value
                    table_value_indices.insert(column_mapping[&upstream_idx]);
                }
            };

            for &idx in &self.group_key {
                add_column(idx, Some(OrderType::Ascending), false);
            }
            for (order_type, idx) in sort_keys {
                add_column(idx, Some(order_type), true);
            }
            for &idx in &in_pks {
                add_column(idx, Some(OrderType::Ascending), true);
            }
            for idx in include_keys {
                add_column(idx, None, true);
            }

            let mapping =
                ColIndexMapping::with_included_columns(&included_upstream_indices, in_fields.len());
            let tb_dist = mapping.rewrite_dist_key(&in_dist_key);
            if let Some(tb_vnode_idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
                internal_table_catalog_builder.set_vnode_col_idx(tb_vnode_idx);
            }

            // set value indices to reduce ser/de overhead
            let table_value_indices = table_value_indices.into_iter().collect_vec();
            internal_table_catalog_builder.set_value_indices(table_value_indices.clone());

            MaterializedInputState {
                table: internal_table_catalog_builder.build(tb_dist.unwrap_or_default()),
                included_upstream_indices,
                table_value_indices,
            }
        };

        let gen_table_state = |agg_kind: AggKind| -> TableState {
            let mut internal_table_catalog_builder =
                TableCatalogBuilder::new(me.ctx().inner().with_options.internal_table_subset());

            let mut included_upstream_indices = vec![];
            for &idx in &self.group_key {
                let tb_column_idx = internal_table_catalog_builder.add_column(&in_fields[idx]);
                internal_table_catalog_builder
                    .add_order_column(tb_column_idx, OrderType::Ascending);
                included_upstream_indices.push(idx);
            }

            match agg_kind {
                AggKind::ApproxCountDistinct => {
                    // Add register column.
                    internal_table_catalog_builder.add_column(&Field {
                        data_type: DataType::List {
                            datatype: Box::new(DataType::Int64),
                        },
                        name: String::from("registers"),
                        sub_fields: vec![],
                        type_name: String::default(),
                    });
                }
                _ => {
                    panic!(
                        "state of agg kind `{}` is not supposed to be `TableState`",
                        agg_kind
                    );
                }
            }

            let mapping =
                ColIndexMapping::with_included_columns(&included_upstream_indices, in_fields.len());
            let tb_dist = mapping.rewrite_dist_key(&in_dist_key);
            if let Some(tb_vnode_idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
                internal_table_catalog_builder.set_vnode_col_idx(tb_vnode_idx);
            }
            TableState {
                table: internal_table_catalog_builder.build(tb_dist.unwrap_or_default()),
            }
        };

        self.agg_calls
            .iter()
            .map(|agg_call| match agg_call.agg_kind {
                AggKind::Min
                | AggKind::Max
                | AggKind::StringAgg
                | AggKind::ArrayAgg
                | AggKind::FirstValue => {
                    if !in_append_only {
                        // columns with order requirement in state table
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
                                    .map(|o| (o.direction.to_order(), o.input.index))
                                    .collect(),
                                _ => unreachable!(),
                            }
                        };
                        // other columns that should be contained in state table
                        let include_keys = match agg_call.agg_kind {
                            AggKind::StringAgg | AggKind::ArrayAgg => {
                                agg_call.inputs.iter().map(|i| i.index).collect()
                            }
                            _ => vec![],
                        };
                        let state = gen_materialized_input_state(sort_keys, include_keys);
                        AggCallState::MaterializedInput(Box::new(state))
                    } else {
                        AggCallState::ResultValue
                    }
                }
                AggKind::Sum | AggKind::Sum0 | AggKind::Count | AggKind::Avg => {
                    AggCallState::ResultValue
                }
                AggKind::ApproxCountDistinct => {
                    if !in_append_only {
                        // FIXME: now the approx count distinct on a non-append-only stream does not
                        // really has state and can handle failover or scale-out correctly
                        AggCallState::ResultValue
                    } else {
                        let state = gen_table_state(agg_call.agg_kind);
                        AggCallState::Table(Box::new(state))
                    }
                }
            })
            .collect()
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let agg_cal_num = self.agg_calls.len();
        let group_key = &self.group_key;
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

    pub fn infer_result_table(
        &self,
        me: &impl GenericPlanRef,
        vnode_col_idx: Option<usize>,
    ) -> TableCatalog {
        let out_fields = me.schema().fields();
        let in_dist_key = self.input.distribution().dist_column_indices().to_vec();
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(me.ctx().inner().with_options.internal_table_subset());
        for field in out_fields.iter() {
            let tb_column_idx = internal_table_catalog_builder.add_column(field);
            if tb_column_idx < self.group_key.len() {
                internal_table_catalog_builder
                    .add_order_column(tb_column_idx, OrderType::Ascending);
            }
        }
        let mapping = self.i2o_col_mapping();
        let tb_dist = mapping.rewrite_dist_key(&in_dist_key).unwrap_or_default();
        if let Some(tb_vnode_idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
            internal_table_catalog_builder.set_vnode_col_idx(tb_vnode_idx);
        }

        // the result_table is composed of group_key and all agg_call's values, so the value_indices
        // of this table should skip group_key.len().
        internal_table_catalog_builder
            .set_value_indices((self.group_key.len()..out_fields.len()).collect());
        internal_table_catalog_builder.build(tb_dist)
    }

    pub fn decompose(self) -> (Vec<PlanAggCall>, Vec<usize>, PlanRef) {
        (self.agg_calls, self.group_key, self.input)
    }

    pub fn agg_calls_display(&self) -> Vec<PlanAggCallDisplay<'_>> {
        self.agg_calls
            .iter()
            .map(|plan_agg_call| PlanAggCallDisplay {
                plan_agg_call,
                input_schema: self.input.schema(),
            })
            .collect_vec()
    }

    pub fn group_key_display(&self) -> Vec<FieldDisplay<'_>> {
        self.group_key
            .iter()
            .copied()
            .map(|i| FieldDisplay(self.input.schema().fields.get(i).unwrap()))
            .collect_vec()
    }
}

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
            AggKind::Min | AggKind::Max | AggKind::StringAgg | AggKind::FirstValue => self.agg_kind,
            AggKind::Count | AggKind::ApproxCountDistinct | AggKind::Sum0 => AggKind::Sum0,
            AggKind::Sum => AggKind::Sum,
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

/// [`ProjectSet`] projects one row multiple times according to `select_list`.
///
/// Different from `Project`, it supports [`TableFunction`](crate::expr::TableFunction)s.
/// See also [`ProjectSetSelectItem`](risingwave_pb::expr::ProjectSetSelectItem) for examples.
///
/// To have a pk, it has a hidden column `projected_row_id` at the beginning. The implementation of
/// `LogicalProjectSet` is highly similar to [`LogicalProject`], except for the additional hidden
/// column.
#[derive(Debug, Clone)]
pub struct ProjectSet<PlanRef> {
    pub select_list: Vec<ExprImpl>,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> ProjectSet<PlanRef> {
    /// Gets the Mapping of columnIndex from output column index to input column index
    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let input_len = self.input.schema().len();
        let mut map = vec![None; 1 + self.select_list.len()];
        for (i, item) in self.select_list.iter().enumerate() {
            map[1 + i] = match item {
                ExprImpl::InputRef(input) => Some(input.index()),
                _ => None,
            }
        }
        ColIndexMapping::with_target_size(map, input_len)
    }

    /// Gets the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.o2i_col_mapping().inverse()
    }
}

/// [`Join`] combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition. In addition, the output columns are a subset of the columns of the left and
/// right columns, dependent on the output indices provided. A repeat output index is illegal.
#[derive(Debug, Clone)]
pub struct Join<PlanRef> {
    pub left: PlanRef,
    pub right: PlanRef,
    pub on: Condition,
    pub join_type: JoinType,
    pub output_indices: Vec<usize>,
}

impl<PlanRef> Join<PlanRef> {
    pub fn decompose(self) -> (PlanRef, PlanRef, Condition, JoinType, Vec<usize>) {
        (
            self.left,
            self.right,
            self.on,
            self.join_type,
            self.output_indices,
        )
    }

    pub fn full_out_col_num(left_len: usize, right_len: usize, join_type: JoinType) -> usize {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                left_len + right_len
            }
            JoinType::LeftSemi | JoinType::LeftAnti => left_len,
            JoinType::RightSemi | JoinType::RightAnti => right_len,
            JoinType::Unspecified => unreachable!(),
        }
    }
}

impl<PlanRef: GenericPlanRef> Join<PlanRef> {
    pub fn with_full_output(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on: Condition,
    ) -> Self {
        let out_column_num =
            Self::full_out_col_num(left.schema().len(), right.schema().len(), join_type);
        Self {
            left,
            right,
            join_type,
            on,
            output_indices: (0..out_column_num).collect(),
        }
    }

    pub fn internal_column_num(&self) -> usize {
        Self::full_out_col_num(
            self.left.schema().len(),
            self.right.schema().len(),
            self.join_type,
        )
    }

    /// Get the Mapping of columnIndex from internal column index to left column index.
    pub fn i2l_col_mapping(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        match self.join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::identity_or_none(left_len + right_len, left_len)
            }

            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::identity(left_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::empty(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    /// Get the Mapping of columnIndex from internal column index to right column index.
    pub fn i2r_col_mapping(&self) -> ColIndexMapping {
        let left_len = self.left.schema().len();
        let right_len = self.right.schema().len();

        match self.join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::empty(left_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::identity(right_len),
            JoinType::Unspecified => unreachable!(),
        }
    }

    /// Get the Mapping of columnIndex from left column index to internal column index.
    pub fn l2i_col_mapping(&self) -> ColIndexMapping {
        self.i2l_col_mapping().inverse()
    }

    /// Get the Mapping of columnIndex from right column index to internal column index.
    pub fn r2i_col_mapping(&self) -> ColIndexMapping {
        self.i2r_col_mapping().inverse()
    }
}
/// [`Expand`] expand one row multiple times according to `column_subsets` and also keep
/// original columns of input. It can be used to implement distinct aggregation and group set.
///
/// This is the schema of `Expand`:
/// | expanded columns(i.e. some columns are set to null) | original columns of input | flag |.
///
/// Aggregates use expanded columns as their arguments and original columns for their filter. `flag`
/// is used to distinguish between different `subset`s in `column_subsets`.
#[derive(Debug, Clone)]
pub struct Expand<PlanRef> {
    // `column_subsets` has many `subset`s which specifies the columns that need to be
    // reserved and other columns will be filled with NULL.
    pub column_subsets: Vec<Vec<usize>>,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> Expand<PlanRef> {
    pub fn column_subsets_display(&self) -> Vec<Vec<FieldDisplay<'_>>> {
        self.column_subsets
            .iter()
            .map(|subset| {
                subset
                    .iter()
                    .map(|&i| FieldDisplay(self.input.schema().fields.get(i).unwrap()))
                    .collect_vec()
            })
            .collect_vec()
    }
}

/// [`Filter`] iterates over its input and returns elements for which `predicate` evaluates to
/// true, filtering out the others.
///
/// If the condition allows nulls, then a null value is treated the same as false.
#[derive(Debug, Clone)]
pub struct Filter<PlanRef> {
    pub predicate: Condition,
    pub input: PlanRef,
}

/// `TopN` sorts the input data and fetches up to `limit` rows from `offset`
#[derive(Debug, Clone)]
pub struct TopN<PlanRef> {
    pub input: PlanRef,
    pub limit: u64,
    pub offset: u64,
    pub with_ties: bool,
    pub order: Order,
    pub group_key: Vec<usize>,
}

pub trait GenericPlanNode {
    fn schema(&self) -> Schema;
    fn logical_pk(&self) -> Option<Vec<usize>>;
    fn ctx(&self) -> OptimizerContextRef;
}

impl<PlanRef: stream::StreamPlanRef> TopN<PlanRef> {
    /// Infers the state table catalog for [`StreamTopN`] and [`StreamGroupTopN`].
    pub fn infer_internal_table_catalog(
        &self,
        me: &impl stream::StreamPlanRef,
        vnode_col_idx: Option<usize>,
    ) -> TableCatalog {
        let schema = me.schema();
        let pk_indices = me.logical_pk();
        let columns_fields = schema.fields().to_vec();
        let field_order = &self.order.field_order;
        let mut internal_table_catalog_builder =
            TableCatalogBuilder::new(me.ctx().inner().with_options.internal_table_subset());

        columns_fields.iter().for_each(|field| {
            internal_table_catalog_builder.add_column(field);
        });
        let mut order_cols = HashSet::new();

        // Here we want the state table to store the states in the order we want, firstly in
        // ascending order by the columns specified by the group key, then by the columns
        // specified by `order`. If we do that, when the later group topN operator
        // does a prefix scanning with the group key, we can fetch the data in the
        // desired order.
        self.group_key.iter().for_each(|&idx| {
            internal_table_catalog_builder.add_order_column(idx, OrderType::Ascending);
            order_cols.insert(idx);
        });

        field_order.iter().for_each(|field_order| {
            if !order_cols.contains(&field_order.index) {
                internal_table_catalog_builder
                    .add_order_column(field_order.index, OrderType::from(field_order.direct));
                order_cols.insert(field_order.index);
            }
        });

        pk_indices.iter().for_each(|idx| {
            if !order_cols.contains(idx) {
                internal_table_catalog_builder.add_order_column(*idx, OrderType::Ascending);
                order_cols.insert(*idx);
            }
        });
        if let Some(vnode_col_idx) = vnode_col_idx {
            internal_table_catalog_builder.set_vnode_col_idx(vnode_col_idx);
        }
        internal_table_catalog_builder
            .build(self.input.distribution().dist_column_indices().to_vec())
    }
}

/// [`Scan`] returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct Scan {
    pub table_name: String,
    pub is_sys_table: bool,
    /// Include `output_col_idx` and columns required in `predicate`
    pub required_col_idx: Vec<usize>,
    pub output_col_idx: Vec<usize>,
    // Descriptor of the table
    pub table_desc: Rc<TableDesc>,
    // Descriptors of all indexes on this table
    pub indexes: Vec<Rc<IndexCatalog>>,
    /// The pushed down predicates. It refers to column indexes of the table.
    pub predicate: Condition,
}

impl Scan {
    /// Get the descs of the output columns.
    pub fn column_descs(&self) -> Vec<ColumnDesc> {
        self.output_col_idx
            .iter()
            .map(|&i| self.table_desc.columns[i].clone())
            .collect()
    }

    /// Helper function to create a mapping from `column_id` to `operator_idx`
    pub fn get_id_to_op_idx_mapping(
        output_col_idx: &[usize],
        table_desc: &Rc<TableDesc>,
    ) -> HashMap<ColumnId, usize> {
        let mut id_to_op_idx = HashMap::new();
        output_col_idx
            .iter()
            .enumerate()
            .for_each(|(op_idx, tb_idx)| {
                let col = &table_desc.columns[*tb_idx];
                id_to_op_idx.insert(col.column_id, op_idx);
            });
        id_to_op_idx
    }
}

/// [`Source`] returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct Source {
    pub catalog: Rc<SourceCatalog>,
}

impl Source {
    pub fn infer_internal_table_catalog(me: &impl GenericPlanRef) -> TableCatalog {
        // note that source's internal table is to store partition_id -> offset mapping and its
        // schema is irrelevant to input schema
        let mut builder =
            TableCatalogBuilder::new(me.ctx().inner().with_options.internal_table_subset());

        let key = Field {
            data_type: DataType::Varchar,
            name: "partition_id".to_string(),
            sub_fields: vec![],
            type_name: "".to_string(),
        };
        let value = Field {
            data_type: DataType::Varchar,
            name: "offset".to_string(),
            sub_fields: vec![],
            type_name: "".to_string(),
        };

        let ordered_col_idx = builder.add_column(&key);
        builder.add_column(&value);
        builder.add_order_column(ordered_col_idx, OrderType::Ascending);

        builder.build(vec![])
    }
}

/// [`Project`] computes a set of expressions from its input relation.
#[derive(Debug, Clone)]
pub struct Project<PlanRef> {
    pub exprs: Vec<ExprImpl>,
    pub input: PlanRef,
}

impl<PlanRef: GenericPlanRef> Project<PlanRef> {
    pub fn new(exprs: Vec<ExprImpl>, input: PlanRef) -> Self {
        Project { exprs, input }
    }

    pub fn decompose(self) -> (Vec<ExprImpl>, PlanRef) {
        (self.exprs, self.input)
    }

    pub(super) fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        builder.field(
            "exprs",
            &self
                .exprs
                .iter()
                .map(|expr| ExprDisplay {
                    expr,
                    input_schema: self.input.schema(),
                })
                .collect_vec(),
        );
        builder.finish()
    }

    pub fn o2i_col_mapping(&self) -> ColIndexMapping {
        let exprs = &self.exprs;
        let input_len = self.input.schema().len();
        let mut map = vec![None; exprs.len()];
        for (i, expr) in exprs.iter().enumerate() {
            map[i] = match expr {
                ExprImpl::InputRef(input) => Some(input.index()),
                _ => None,
            }
        }
        ColIndexMapping::with_target_size(map, input_len)
    }

    /// get the Mapping of columnIndex from input column index to output column index,if a input
    /// column corresponds more than one out columns, mapping to any one
    pub fn i2o_col_mapping(&self) -> ColIndexMapping {
        self.o2i_col_mapping().inverse()
    }
}
