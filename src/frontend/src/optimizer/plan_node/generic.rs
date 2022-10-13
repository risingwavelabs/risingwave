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

use std::fmt;
use std::rc::Rc;

use itertools::Itertools;
use risingwave_common::catalog::{Field, FieldDisplay, Schema, TableDesc};
use risingwave_common::types::{DataType, IntervalUnit};
use risingwave_expr::expr::AggKind;
use risingwave_pb::expr::agg_call::OrderByField as ProstAggOrderByField;
use risingwave_pb::expr::AggCall as ProstAggCall;
use risingwave_pb::plan_common::JoinType;

use super::utils::IndicesDisplay;
use crate::catalog::source_catalog::SourceCatalog;
use crate::catalog::IndexCatalog;
use crate::expr::{Expr, ExprDisplay, ExprImpl, InputRef, InputRefDisplay};
use crate::optimizer::property::{Direction, Order};
use crate::utils::{Condition, ConditionDisplay};

pub trait GenericPlanRef {
    fn schema(&self) -> &Schema;
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
                        Field::with_name(DataType::Timestamp, "window_start"),
                        Field::with_name(DataType::Timestamp, "window_end"),
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

impl<PlanRef: GenericPlanRef> Agg<PlanRef> {
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
    pub fn new(
        left: PlanRef,
        right: PlanRef,
        on: Condition,
        join_type: JoinType,
        output_indices: Vec<usize>,
    ) -> Self {
        Self {
            left,
            right,
            on,
            join_type,
            output_indices,
        }
    }

    pub fn decompose(self) -> (PlanRef, PlanRef, Condition, JoinType, Vec<usize>) {
        (
            self.left,
            self.right,
            self.on,
            self.join_type,
            self.output_indices,
        )
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
    pub limit: usize,
    pub offset: usize,
    pub with_ties: bool,
    pub order: Order,
    pub group_key: Vec<usize>,
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

/// [`Source`] returns contents of a table or other equivalent object
#[derive(Debug, Clone)]
pub struct Source(pub Rc<SourceCatalog>);

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
}
