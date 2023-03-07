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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use itertools::Itertools;
use risingwave_common::catalog::{Field, FieldDisplay, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::expr::AggKind;
use risingwave_pb::expr::agg_call::OrderByField as ProstAggOrderByField;
use risingwave_pb::expr::AggCall as ProstAggCall;
use risingwave_pb::stream_plan::{agg_call_state, AggCallState as AggCallStateProst};

use super::super::utils::TableCatalogBuilder;
use super::{stream, GenericPlanNode, GenericPlanRef};
use crate::expr::{Expr, ExprRewriter, InputRef, InputRefDisplay};
use crate::optimizer::optimizer_context::OptimizerContextRef;
use crate::optimizer::property::Direction;
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::utils::{
    ColIndexMapping, ColIndexMappingRewriteExt, Condition, ConditionDisplay, IndexRewriter,
};
use crate::TableCatalog;

/// [`Agg`] groups input data by their group key and computes aggregation functions.
///
/// It corresponds to the `GROUP BY` operator in a SQL query statement together with the aggregate
/// functions in the `SELECT` clause.
///
/// The output schema will first include the group key and then the aggregation calls.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Agg<PlanRef> {
    pub agg_calls: Vec<PlanAggCall>,
    pub group_key: Vec<usize>,
    pub input: PlanRef,
}

impl<PlanRef> Agg<PlanRef> {
    pub(crate) fn rewrite_exprs(&mut self, r: &mut dyn ExprRewriter) {
        self.agg_calls.iter_mut().for_each(|call| {
            call.filter = call.filter.clone().rewrite_expr(r);
        });
    }
}

impl<PlanRef: GenericPlanRef> GenericPlanNode for Agg<PlanRef> {
    fn schema(&self) -> Schema {
        let fields = self
            .group_key
            .iter()
            .cloned()
            .map(|i| self.input.schema().fields()[i].clone())
            .chain(self.agg_calls.iter().map(|agg_call| {
                let plan_agg_call_display = PlanAggCallDisplay {
                    plan_agg_call: agg_call,
                    input_schema: self.input.schema(),
                };
                let name = format!("{:?}", plan_agg_call_display);
                Field::with_name(agg_call.return_type.clone(), name)
            }))
            .collect();
        Schema { fields }
    }

    fn logical_pk(&self) -> Option<Vec<usize>> {
        Some((0..self.group_key.len()).collect_vec())
    }

    fn ctx(&self) -> OptimizerContextRef {
        self.input.ctx()
    }
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
                TableCatalogBuilder::new(me.ctx().with_options().internal_table_subset());

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

            // prefix_len_hint should be the length of deduplicated group key because pk is
            // deduplicated.
            let prefix_len = self.group_key.iter().unique().count();
            internal_table_catalog_builder.set_read_prefix_len_hint(prefix_len);
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
                TableCatalogBuilder::new(me.ctx().with_options().internal_table_subset());

            let mut included_upstream_indices = vec![];
            for &idx in &self.group_key {
                let tb_column_idx = internal_table_catalog_builder.add_column(&in_fields[idx]);
                internal_table_catalog_builder
                    .add_order_column(tb_column_idx, OrderType::Ascending);
                included_upstream_indices.push(idx);
            }

            internal_table_catalog_builder.set_read_prefix_len_hint(self.group_key.len());

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
                AggKind::Sum
                | AggKind::Sum0
                | AggKind::Count
                | AggKind::Avg
                | AggKind::StddevPop
                | AggKind::StddevSamp
                | AggKind::VarPop
                | AggKind::VarSamp => AggCallState::ResultValue,
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
            TableCatalogBuilder::new(me.ctx().with_options().internal_table_subset());
        for field in out_fields.iter() {
            let tb_column_idx = internal_table_catalog_builder.add_column(field);
            if tb_column_idx < self.group_key.len() {
                internal_table_catalog_builder
                    .add_order_column(tb_column_idx, OrderType::Ascending);
            }
        }
        internal_table_catalog_builder.set_read_prefix_len_hint(self.group_key.len());
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

    /// Infer dedup tables for distinct agg calls, partitioned by distinct columns.
    /// Since distinct agg calls only dedup on the first argument, the key of the result map is
    /// `usize`, i.e. the distinct column index.
    ///
    /// Dedup table schema:
    /// group key | distinct key | count for AGG1(distinct x) | count for AGG2(distinct x) | ...
    pub fn infer_distinct_dedup_tables(
        &self,
        me: &impl GenericPlanRef,
        vnode_col_idx: Option<usize>,
    ) -> HashMap<usize, TableCatalog> {
        let in_dist_key = self.input.distribution().dist_column_indices().to_vec();
        let in_fields = self.input.schema().fields();

        self.agg_calls
            .iter()
            .enumerate()
            .filter(|(_, call)| call.distinct) // only distinct agg calls need dedup table
            .into_group_map_by(|(_, call)| call.inputs[0].index) // one table per distinct column
            .into_iter()
            .map(|(distinct_col, indices_and_calls)| {
                let mut table_builder =
                    TableCatalogBuilder::new(me.ctx().with_options().internal_table_subset());

                let key_cols = self
                    .group_key
                    .iter()
                    .copied()
                    .chain(std::iter::once(distinct_col))
                    .collect_vec();
                for &idx in &key_cols {
                    let table_col_idx = table_builder.add_column(&in_fields[idx]);
                    table_builder.add_order_column(table_col_idx, OrderType::Ascending);
                }

                // Agg calls with same distinct column share the same dedup table, but they may have
                // different filter conditions, so the count of occurrence of one distinct key may
                // differ among different calls. We add one column for each call in the dedup table.
                for (call_index, _) in indices_and_calls {
                    table_builder.add_column(&Field {
                        data_type: DataType::Int64,
                        name: format!("count_for_agg_call_{}", call_index),
                        sub_fields: vec![],
                        type_name: String::default(),
                    });
                }
                table_builder
                    .set_value_indices((key_cols.len()..table_builder.columns().len()).collect());

                let mapping = ColIndexMapping::with_included_columns(&key_cols, in_fields.len());
                if let Some(idx) = vnode_col_idx.and_then(|idx| mapping.try_map(idx)) {
                    table_builder.set_vnode_col_idx(idx);
                }
                let dist_key = mapping.rewrite_dist_key(&in_dist_key).unwrap_or_default();
                let table = table_builder.build(dist_key);
                (distinct_col, table)
            })
            .collect()
    }

    pub fn decompose(self) -> (Vec<PlanAggCall>, Vec<usize>, PlanRef) {
        (self.agg_calls, self.group_key, self.input)
    }

    pub fn fmt_with_name(&self, f: &mut fmt::Formatter<'_>, name: &str) -> fmt::Result {
        let mut builder = f.debug_struct(name);
        self.fmt_fields_with_builder(&mut builder);
        builder.finish()
    }

    pub fn fmt_fields_with_builder(&self, builder: &mut fmt::DebugStruct<'_, '_>) {
        if !self.group_key.is_empty() {
            builder.field("group_key", &self.group_key_display());
        }
        builder.field("aggs", &self.agg_calls_display());
    }

    fn agg_calls_display(&self) -> Vec<PlanAggCallDisplay<'_>> {
        self.agg_calls
            .iter()
            .map(|plan_agg_call| PlanAggCallDisplay {
                plan_agg_call,
                input_schema: self.input.schema(),
            })
            .collect_vec()
    }

    fn group_key_display(&self) -> Vec<FieldDisplay<'_>> {
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
#[derive(Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PlanAggOrderByFieldDisplay<'a> {
    pub plan_agg_order_by_field: &'a PlanAggOrderByField,
    pub input_schema: &'a Schema,
}

impl fmt::Display for PlanAggOrderByFieldDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let that = self.plan_agg_order_by_field;
        InputRefDisplay {
            input_ref: &that.input,
            input_schema: self.input_schema,
        }
        .fmt(f)?;
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
#[derive(Clone, PartialEq, Eq, Hash)]
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
    pub fn rewrite_input_index(&mut self, mapping: ColIndexMapping) {
        // modify input
        self.inputs.iter_mut().for_each(|x| {
            x.index = mapping.map(x.index);
        });

        // modify order_by_fields
        self.order_by_fields.iter_mut().for_each(|x| {
            x.input.index = mapping.map(x.input.index);
        });

        // modify filter
        let mut rewriter = IndexRewriter { mapping };
        self.filter.conjunctions.iter_mut().for_each(|x| {
            *x = rewriter.rewrite_expr(x.clone());
        });
    }

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
            filter: self.filter.as_expr_unless_true().map(|x| x.to_expr_proto()),
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
            AggKind::StddevPop | AggKind::StddevSamp | AggKind::VarPop | AggKind::VarSamp => {
                panic!("Stddev/Var aggregation should have been rewritten to Sum, Count and Case")
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
                        f(&PlanAggOrderByFieldDisplay {
                            plan_agg_order_by_field: e,
                            input_schema: self.input_schema,
                        })
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
