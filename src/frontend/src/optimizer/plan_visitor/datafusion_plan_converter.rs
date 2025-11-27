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

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema as ArrowSchema;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::{
    Expr as DFExpr, Join, JoinConstraint, JoinType as DFJoinType, LogicalPlan as DFLogicalPlan,
    LogicalPlan, TableScan, Values, build_join_schema,
};
use datafusion_common::{Column, DFSchema, ScalarValue};
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::types::ScalarImpl;
use risingwave_pb::plan_common::JoinType;

use crate::datafusion::IcebergTableProvider;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::{DefaultBehavior, LogicalPlanVisitor};
use crate::optimizer::{LogicalPlanRef, PlanVisitor};

#[derive(Debug, Clone, Copy)]
pub struct DataFusionPlanConverter;

impl LogicalPlanVisitor for DataFusionPlanConverter {
    type DefaultBehavior = DefaultValueBehavior;
    type Result = Option<DFLogicalPlan>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValueBehavior
    }

    fn visit_logical_project(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalProject,
    ) -> Self::Result {
        let input_plan = self.visit(plan.input())?;

        let mut df_exprs = Vec::new();
        for expr in plan.exprs() {
            match convert_expr(expr) {
                Some(df_expr) => df_exprs.push(df_expr),
                None => {
                    // TODO: better handling of unsupported expressions
                    return None;
                }
            }
        }

        match datafusion::logical_expr::Projection::try_new(df_exprs, Arc::new(input_plan)) {
            Ok(projection) => Some(LogicalPlan::Projection(projection)),
            Err(_) => None,
        }
    }

    fn visit_logical_values(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalValues,
    ) -> Self::Result {
        let rw_schema = plan.schema();

        let arrow_fields = rw_schema
            .fields()
            .iter()
            .map(|f| {
                IcebergArrowConvert
                    .to_arrow_field(&f.name, &f.data_type)
                    .expect("failed to convert RW field to Arrow field")
            })
            .collect_vec();

        let arrow_schema = ArrowSchema::new(arrow_fields);
        let df_schema = DFSchema::try_from(Arc::new(arrow_schema))
            .expect("failed to create DFSchema from Arrow schema");

        let mut df_rows: Vec<Vec<DFExpr>> = Vec::with_capacity(plan.rows().len());
        for row in plan.rows() {
            let mut df_row: Vec<DFExpr> = Vec::with_capacity(row.len());
            for expr in row {
                match convert_expr(expr) {
                    Some(df_expr) => df_row.push(df_expr),
                    None => {
                        return None;
                    }
                }
            }
            df_rows.push(df_row);
        }

        Some(LogicalPlan::Values(Values {
            schema: Arc::new(df_schema),
            values: df_rows,
        }))
    }

    fn visit_logical_join(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalJoin,
    ) -> Self::Result {
        // Recursively convert left and right children
        let left_plan = self.visit(plan.left())?;
        let right_plan = self.visit(plan.right())?;

        // Convert join type from RisingWave to DataFusion
        let df_join_type = convert_join_type(plan.join_type())?;

        // Extract equijoin conditions - get equal join key pairs
        let left_col_num = plan.left().schema().len();
        let right_col_num = plan.right().schema().len();
        let (eq_indexes, other_condition) =
            plan.on().clone().split_eq_keys(left_col_num, right_col_num);
        let join_on_exprs = eq_indexes
            .into_iter()
            .map(|(left, right, _)| {
                let left_schema = left_plan.schema();
                let right_schema = right_plan.schema();

                // Create column references for the join keys
                let left_field = &left_schema.fields()[left.index];
                let right_field = &right_schema.fields()[right.index - left_col_num];

                let left_expr = DFExpr::Column(Column::from_qualified_name(left_field.name()));
                let right_expr = DFExpr::Column(Column::from_qualified_name(right_field.name()));

                (left_expr, right_expr)
            })
            .collect_vec();

        // Convert non-equijoin conditions (filter)
        let filter = match other_condition.as_expr_unless_true() {
            Some(expr) => match convert_expr(&expr) {
                Some(df_expr) => Some(df_expr),
                // Failed to convert expression
                None => return None,
            },
            None => None,
        };

        // Build the join schema
        let join_schema =
            build_join_schema(left_plan.schema(), right_plan.schema(), &df_join_type).ok()?;

        Some(LogicalPlan::Join(Join {
            left: Arc::new(left_plan),
            right: Arc::new(right_plan),
            on: join_on_exprs,
            filter,
            join_type: df_join_type,
            join_constraint: JoinConstraint::On,
            schema: Arc::new(join_schema),
            null_equals_null: false,
        }))
    }

    fn visit_logical_iceberg_scan(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalIcebergScan,
    ) -> Self::Result {
        let table_name = plan.source_catalog()?.name.clone();
        let table_source = provider_as_source(Arc::new(
            IcebergTableProvider::from_logical_plan(plan).ok()?,
        ));
        Some(LogicalPlan::TableScan(
            TableScan::try_new(table_name, table_source, None, vec![], None).ok()?,
        ))
    }
}

pub struct DefaultValueBehavior;
impl<T> DefaultBehavior<T> for DefaultValueBehavior {
    fn apply(&self, _results: impl IntoIterator<Item = T>) -> T {
        panic!("RwToDfConverter: encountered unsupported node in default_behavior")
    }
}

#[easy_ext::ext(LogicalPlanToDataFusionExt)]
pub impl LogicalPlanRef {
    /// Convert RisingWave logical plan to DataFusion logical plan.
    ///
    /// Returns `None` if any part of the plan cannot be converted.
    fn to_datafusion_logical_plan(&self) -> Option<DFLogicalPlan> {
        DataFusionPlanConverter.visit(self.clone())
    }
}

fn convert_expr(expr: &ExprImpl) -> Option<DFExpr> {
    match expr {
        ExprImpl::Literal(lit) => {
            let scalar = match lit.get_data() {
                None => ScalarValue::Null,
                Some(sv) => convert_scalar_value(sv)?,
            };
            Some(DFExpr::Literal(scalar))
        }
        ExprImpl::InputRef(input_ref) => {
            // Create a column reference
            // For simplicity, use the column index as the column name
            let col_name = format!("col_{}", input_ref.index);
            Some(DFExpr::Column(Column::from_name(col_name)))
        }
        // TODO: Handle other expression types as needed
        _ => None, // For other expression types that we don't support yet, return None
    }
}

fn convert_scalar_value(sv: &ScalarImpl) -> Option<ScalarValue> {
    match sv {
        ScalarImpl::Int32(v) => Some(ScalarValue::Int32(Some(*v))),
        ScalarImpl::Int64(v) => Some(ScalarValue::Int64(Some(*v))),
        ScalarImpl::Float64(v) => Some(ScalarValue::Float64(Some(v.0))),
        ScalarImpl::Bool(v) => Some(ScalarValue::Boolean(Some(*v))),
        ScalarImpl::Utf8(s) => Some(ScalarValue::Utf8(Some(s.to_string()))),
        // TODO: Handle other ScalarImpl variants as needed
        _ => None, // Unsupported type
    }
}

fn convert_join_type(join_type: JoinType) -> Option<DFJoinType> {
    match join_type {
        JoinType::Inner => Some(DFJoinType::Inner),
        JoinType::LeftOuter => Some(DFJoinType::Left),
        JoinType::RightOuter => Some(DFJoinType::Right),
        JoinType::FullOuter => Some(DFJoinType::Full),
        JoinType::LeftSemi => Some(DFJoinType::LeftSemi),
        JoinType::LeftAnti => Some(DFJoinType::LeftAnti),
        JoinType::RightSemi => Some(DFJoinType::RightSemi),
        JoinType::RightAnti => Some(DFJoinType::RightAnti),
        _ => None, // Unsupported join type
    }
}
