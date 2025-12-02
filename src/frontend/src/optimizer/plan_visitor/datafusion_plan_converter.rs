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

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{
    Expr as DFExpr, ExprSchemable, Join, JoinConstraint, JoinType as DFJoinType,
    LogicalPlan as DFLogicalPlan, LogicalPlan, TableScan, Values, build_join_schema,
};
use datafusion_common::{Column, DFSchema, ScalarValue};
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bail_not_implemented;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_pb::plan_common::JoinType;

use crate::datafusion::IcebergTableProvider;
use crate::error::Result as RwResult;
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::{DefaultBehavior, LogicalPlanVisitor};
use crate::optimizer::{LogicalPlanRef, PlanVisitor};

#[derive(Debug, Clone, Copy)]
pub struct DataFusionPlanConverter;

impl LogicalPlanVisitor for DataFusionPlanConverter {
    type DefaultBehavior = DefaultValueBehavior;
    type Result = RwResult<Arc<DFLogicalPlan>>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValueBehavior
    }

    fn visit_logical_project(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalProject,
    ) -> Self::Result {
        let input_plan = self.visit(plan.input())?;
        let input_schema = input_plan.schema();

        let mut df_exprs = Vec::new();
        for expr in plan.exprs() {
            df_exprs.push(convert_expr(expr, input_schema.as_ref())?);
        }

        // DataFusion requires unique expression names in projection.
        let mut duplicated_exprs = HashMap::new();
        for expr in &mut df_exprs {
            let (relation, field) = expr.to_field(input_schema)?;
            let count = duplicated_exprs
                .entry((relation.clone(), field.name().clone()))
                .or_insert(0);
            *count += 1;
            if *count > 1 {
                let take_expr = std::mem::replace(expr, DFExpr::Literal(ScalarValue::Null));
                *expr = take_expr.alias_qualified(relation, format!("{}@{}", field.name(), count));
            }
        }

        let projection = datafusion::logical_expr::Projection::try_new(df_exprs, input_plan)?;
        Ok(Arc::new(LogicalPlan::Projection(projection)))
    }

    fn visit_logical_values(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalValues,
    ) -> Self::Result {
        let rw_schema = plan.schema();

        let arrow_fields = rw_schema
            .fields()
            .iter()
            .map(|f| IcebergArrowConvert.to_arrow_field(&f.name, &f.data_type))
            .collect::<Result<Vec<ArrowField>, _>>()?;

        let arrow_schema = ArrowSchema::new(arrow_fields);
        let df_schema = DFSchema::try_from(Arc::new(arrow_schema))?;

        let mut df_rows: Vec<Vec<DFExpr>> = Vec::with_capacity(plan.rows().len());
        for row in plan.rows() {
            let mut df_row: Vec<DFExpr> = Vec::with_capacity(row.len());
            for expr in row {
                df_row.push(convert_expr(expr, &df_schema)?);
            }
            df_rows.push(df_row);
        }

        Ok(Arc::new(LogicalPlan::Values(Values {
            schema: Arc::new(df_schema),
            values: df_rows,
        })))
    }

    fn visit_logical_join(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalJoin,
    ) -> Self::Result {
        // Recursively convert left and right children
        let left_plan = self.visit(plan.left())?;
        let right_plan = self.visit(plan.right())?;
        let concat_columns = left_plan
            .schema()
            .iter()
            .map(Into::into)
            .chain(right_plan.schema().iter().map(Into::into))
            .collect_vec();

        // Convert join type from RisingWave to DataFusion
        let df_join_type = convert_join_type(plan.join_type())?;

        // Extract equijoin conditions - get equal join key pairs
        let left_col_num = plan.left().schema().len();
        let right_col_num = plan.right().schema().len();
        let (eq_indexes, other_condition) =
            plan.on().clone().split_eq_keys(left_col_num, right_col_num);
        let join_on_exprs = eq_indexes
            .iter()
            .map(|(left, right, _)| {
                let left_expr = DFExpr::Column(concat_columns.column(left.index));
                // right index will exceed left child's column num, so we don't need to adjust it here
                let right_expr = DFExpr::Column(concat_columns.column(right.index));
                (left_expr, right_expr)
            })
            .collect_vec();
        let mut null_equals_null = false;
        if !eq_indexes.is_empty() {
            match (
                eq_indexes.iter().all(|(_, _, null_eq_null)| *null_eq_null),
                eq_indexes.iter().all(|(_, _, null_eq_null)| !*null_eq_null),
            ) {
                (true, false) => {
                    null_equals_null = true;
                }
                (false, true) => {
                    null_equals_null = false;
                }
                _ => {
                    bail_not_implemented!(
                        "DataFusionPlanConverter: mixed null_equals_null and not_null_equals_null in join keys is not supported"
                    );
                }
            }
        }

        // Convert non-equijoin conditions (filter)
        let filter = match other_condition.as_expr_unless_true() {
            Some(expr) => Some(convert_expr(&expr, &concat_columns)?),
            None => None,
        };

        let join_schema =
            build_join_schema(left_plan.schema(), right_plan.schema(), &df_join_type)?;
        let join = Join {
            left: left_plan,
            right: right_plan,
            on: join_on_exprs,
            filter,
            join_type: df_join_type,
            join_constraint: JoinConstraint::On,
            schema: Arc::new(join_schema),
            null_equals_null,
        };
        if plan.output_indices_are_trivial() {
            return Ok(Arc::new(LogicalPlan::Join(join)));
        }

        let projection_exprs = plan
            .output_indices()
            .iter()
            .map(|&idx| DFExpr::Column(join.schema.column(idx)))
            .collect_vec();
        let projection = datafusion::logical_expr::Projection::try_new(
            projection_exprs,
            Arc::new(LogicalPlan::Join(join)),
        )?;
        Ok(Arc::new(LogicalPlan::Projection(projection)))
    }

    fn visit_logical_iceberg_scan(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalIcebergScan,
    ) -> Self::Result {
        // DataFusion requires unique table names for each scan, so we can't use actual table name here.
        let table_name = format!("iceberg_scan#{}", plan.base.id().0);
        let table_source =
            provider_as_source(Arc::new(IcebergTableProvider::from_logical_plan(plan)?));
        let table_scan = TableScan::try_new(table_name, table_source, None, vec![], None)?;
        Ok(Arc::new(LogicalPlan::TableScan(table_scan)))
    }
}

pub struct DefaultValueBehavior;
impl DefaultBehavior<RwResult<Arc<DFLogicalPlan>>> for DefaultValueBehavior {
    fn apply(
        &self,
        _results: impl IntoIterator<Item = RwResult<Arc<DFLogicalPlan>>>,
    ) -> RwResult<Arc<DFLogicalPlan>> {
        bail_not_implemented!("DataFusionPlanConverter: unsupported plan node for conversion");
    }
}

#[easy_ext::ext(LogicalPlanToDataFusionExt)]
pub impl LogicalPlanRef {
    /// Convert RisingWave logical plan to DataFusion logical plan.
    ///
    /// Returns an error if the plan contains unsupported nodes or expressions.
    fn to_datafusion_logical_plan(&self) -> RwResult<Arc<DFLogicalPlan>> {
        let result = DataFusionPlanConverter.visit(self.clone())?;
        Ok(result)
    }
}

fn convert_expr(expr: &ExprImpl, input_schema: &impl ColumnTrait) -> RwResult<DFExpr> {
    match expr {
        ExprImpl::Literal(lit) => {
            let scalar = match lit.get_data() {
                None => ScalarValue::Null,
                Some(sv) => convert_scalar_value(sv, lit.return_type())?,
            };
            Ok(DFExpr::Literal(scalar))
        }
        ExprImpl::InputRef(input_ref) => Ok(DFExpr::Column(input_schema.column(input_ref.index()))),
        // TODO: Handle other expression types as needed
        _ => bail_not_implemented!("DataFusionPlanConverter: unsupported expression {:?}", expr),
    }
}

fn convert_scalar_value(sv: &ScalarImpl, data_type: DataType) -> RwResult<ScalarValue> {
    match (sv, &data_type) {
        (ScalarImpl::Bool(v), DataType::Boolean) => Ok(ScalarValue::Boolean(Some(*v))),
        (ScalarImpl::Int16(v), DataType::Int16) => Ok(ScalarValue::Int16(Some(*v))),
        (ScalarImpl::Int32(v), DataType::Int32) => Ok(ScalarValue::Int32(Some(*v))),
        (ScalarImpl::Int64(v), DataType::Int64) => Ok(ScalarValue::Int64(Some(*v))),
        (ScalarImpl::Float32(v), DataType::Float32) => {
            Ok(ScalarValue::Float32(Some(v.into_inner())))
        }
        (ScalarImpl::Float64(v), DataType::Float64) => {
            Ok(ScalarValue::Float64(Some(v.into_inner())))
        }
        (ScalarImpl::Utf8(v), DataType::Varchar) => Ok(ScalarValue::Utf8(Some(v.to_string()))),
        (ScalarImpl::Bytea(v), DataType::Bytea) => Ok(ScalarValue::Binary(Some(v.to_vec()))),
        // For other types, use fallback conversion via IcebergArrowConvert to ensure consistency
        _ => convert_scalar_value_fallback(sv, data_type),
    }
}

fn convert_scalar_value_fallback(sv: &ScalarImpl, data_type: DataType) -> RwResult<ScalarValue> {
    let mut array_builder = data_type.create_array_builder(1);
    array_builder.append(Some(sv));
    let array = array_builder.finish();
    let arrow_field = IcebergArrowConvert.to_arrow_field("", &data_type)?;
    let array = IcebergArrowConvert.to_arrow_array(arrow_field.data_type(), &array)?;
    let scalar_value = ScalarValue::try_from_array(&array, 0)?;
    Ok(scalar_value)
}

fn convert_join_type(join_type: JoinType) -> RwResult<DFJoinType> {
    match join_type {
        JoinType::Inner => Ok(DFJoinType::Inner),
        JoinType::LeftOuter => Ok(DFJoinType::Left),
        JoinType::RightOuter => Ok(DFJoinType::Right),
        JoinType::FullOuter => Ok(DFJoinType::Full),
        JoinType::LeftSemi => Ok(DFJoinType::LeftSemi),
        JoinType::LeftAnti => Ok(DFJoinType::LeftAnti),
        JoinType::RightSemi => Ok(DFJoinType::RightSemi),
        JoinType::RightAnti => Ok(DFJoinType::RightAnti),
        _ => bail_not_implemented!(
            "DataFusionPlanConverter: unsupported join type {:?}",
            join_type
        ),
    }
}

trait ColumnTrait {
    fn column(&self, index: usize) -> Column;
}

impl ColumnTrait for DFSchema {
    fn column(&self, index: usize) -> Column {
        Column::from(self.qualified_field(index))
    }
}

impl ColumnTrait for Vec<Column> {
    fn column(&self, index: usize) -> Column {
        self[index].clone()
    }
}
