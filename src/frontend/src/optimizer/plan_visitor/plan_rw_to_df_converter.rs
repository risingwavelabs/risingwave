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

use datafusion::arrow::datatypes::{Field, Schema as ArrowSchema};
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::empty::EmptyTable;
use datafusion::logical_expr::{
    Expr as DFExpr, Join, JoinConstraint, JoinType as DFJoinType, LogicalPlan as DFLogicalPlan,
    LogicalPlan, TableScan, Values, build_join_schema,
};
use datafusion_common::{Column, DFSchema, ScalarValue};
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::types::ScalarImpl;
use risingwave_pb::plan_common::JoinType;

use crate::expr::ExprImpl;
use crate::optimizer::PlanVisitor;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{Logical, PlanRef, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::{DefaultBehavior, LogicalPlanVisitor};

#[derive(Debug, Clone, Default)]
pub struct RwToDfConverter {}

impl RwToDfConverter {
    pub fn convert(&mut self, plan: PlanRef<Logical>) -> DFLogicalPlan {
        PlanVisitor::visit(self, plan)
    }

    /// Convert a RisingWave expression to a `DataFusion` expression
    fn convert_expr(&self, expr: &ExprImpl) -> Option<DFExpr> {
        match expr {
            ExprImpl::Literal(lit) => {
                let scalar = match lit.get_data() {
                    None => ScalarValue::Null,
                    Some(sv) => match sv {
                        ScalarImpl::Int32(v) => ScalarValue::Int32(Some(*v)),
                        ScalarImpl::Int64(v) => ScalarValue::Int64(Some(*v)),
                        ScalarImpl::Float64(v) => ScalarValue::Float64(Some(v.0)),
                        ScalarImpl::Bool(v) => ScalarValue::Boolean(Some(*v)),
                        ScalarImpl::Utf8(s) => ScalarValue::Utf8(Some(s.to_string())),
                        _ => {
                            // For unsupported types, return None to indicate conversion failure
                            return None;
                        }
                    },
                };
                Some(DFExpr::Literal(scalar))
            }
            ExprImpl::InputRef(input_ref) => {
                // Create a column reference
                // For simplicity, use the column index as the column name
                let col_name = format!("col_{}", input_ref.index);
                Some(DFExpr::Column(Column::from_name(col_name)))
            }
            // For other expression types that we don't support yet, return None
            _ => None,
        }
    }
}

impl LogicalPlanVisitor for RwToDfConverter {
    type DefaultBehavior = DefaultValueBehavior;
    type Result = DFLogicalPlan;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValueBehavior
    }

    fn visit_logical_project(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalProject,
    ) -> Self::Result {
        let input_plan = self.convert(plan.input());

        let mut df_exprs = Vec::new();
        for expr in plan.exprs() {
            match self.convert_expr(expr) {
                Some(df_expr) => df_exprs.push(df_expr),
                None => {
                    let empty_schema = DFSchema::empty();
                    return LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: std::sync::Arc::new(empty_schema),
                    });
                }
            }
        }

        match datafusion::logical_expr::Projection::try_new(
            df_exprs,
            std::sync::Arc::new(input_plan),
        ) {
            Ok(projection) => LogicalPlan::Projection(projection),
            Err(_) => {
                let empty_schema: DFSchema = DFSchema::empty();
                LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: std::sync::Arc::new(empty_schema),
                })
            }
        }
    }

    fn visit_logical_values(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalValues,
    ) -> Self::Result {
        let rw_schema = plan.schema();

        let converter = IcebergArrowConvert {};
        let arrow_fields: Vec<Field> = rw_schema
            .fields()
            .iter()
            .map(|f| {
                converter
                    .to_arrow_field(&f.name, &f.data_type)
                    .expect("failed to convert RW field to Arrow field")
            })
            .collect();

        let arrow_schema = ArrowSchema::new(arrow_fields);
        let df_schema = DFSchema::try_from(std::sync::Arc::new(arrow_schema))
            .expect("failed to create DFSchema from Arrow schema");

        let mut df_rows: Vec<Vec<DFExpr>> = Vec::with_capacity(plan.rows().len());
        for row in plan.rows() {
            let mut df_row: Vec<DFExpr> = Vec::with_capacity(row.len());
            for expr in row {
                match expr {
                    ExprImpl::Literal(lit) => {
                        let scalar = match lit.get_data() {
                            None => ScalarValue::Null,
                            Some(sv) => match sv {
                                ScalarImpl::Int32(v) => ScalarValue::Int32(Some(*v)),
                                ScalarImpl::Int64(v) => ScalarValue::Int64(Some(*v)),
                                ScalarImpl::Float64(v) => ScalarValue::Float64(Some(v.0)),
                                ScalarImpl::Bool(v) => ScalarValue::Boolean(Some(*v)),
                                ScalarImpl::Utf8(s) => ScalarValue::Utf8(Some(s.to_string())),
                                other => {
                                    // fallback: stringify using Debug
                                    ScalarValue::Utf8(Some(format!("{:?}", other)))
                                }
                            },
                        };
                        df_row.push(DFExpr::Literal(scalar));
                    }
                    _ => {
                        // unsupported expr in Values -> return an EmptyRelation so caller can handle
                        return LogicalPlan::EmptyRelation(
                            datafusion::logical_expr::EmptyRelation {
                                produce_one_row: false,
                                schema: std::sync::Arc::new(df_schema),
                            },
                        );
                    }
                }
            }
            df_rows.push(df_row);
        }

        LogicalPlan::Values(Values {
            schema: std::sync::Arc::new(df_schema),
            values: df_rows,
        })
    }

    fn visit_logical_join(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalJoin,
    ) -> Self::Result {
        // Recursively convert left and right children
        let left_plan = self.convert(plan.left());
        let right_plan = self.convert(plan.right());

        // Convert join type from RisingWave to DataFusion
        let df_join_type = match plan.join_type() {
            JoinType::Inner => DFJoinType::Inner,
            JoinType::LeftOuter => DFJoinType::Left,
            JoinType::RightOuter => DFJoinType::Right,
            JoinType::FullOuter => DFJoinType::Full,
            JoinType::LeftSemi => DFJoinType::LeftSemi,
            JoinType::LeftAnti => DFJoinType::LeftAnti,
            JoinType::RightSemi => DFJoinType::RightSemi,
            JoinType::RightAnti => DFJoinType::RightAnti,
            _ => {
                // For unsupported join types (like AsOf joins), return EmptyRelation
                let empty_schema = DFSchema::empty();
                return LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: std::sync::Arc::new(empty_schema),
                });
            }
        };

        // Extract equijoin conditions - get equal join key pairs
        let eq_indexes = plan.eq_indexes();
        let on: Vec<(DFExpr, DFExpr)> = eq_indexes
            .into_iter()
            .map(|(left_idx, right_idx)| {
                let left_schema = left_plan.schema();
                let right_schema = right_plan.schema();

                // Create column references for the join keys
                let left_field = &left_schema.fields()[left_idx];
                let right_field = &right_schema.fields()[right_idx - left_schema.fields().len()];

                let left_expr = DFExpr::Column(Column::from_qualified_name(left_field.name()));
                let right_expr = DFExpr::Column(Column::from_qualified_name(right_field.name()));

                (left_expr, right_expr)
            })
            .collect();

        // Convert non-equijoin conditions (filter)
        let condition = plan.on();
        let filter = if condition.always_true() {
            None
        } else {
            // For now, convert complex conditions to a simple true condition
            // A full implementation would need to convert the entire condition tree
            Some(DFExpr::Literal(ScalarValue::Boolean(Some(true))))
        };

        // Build the join schema
        let join_schema =
            match build_join_schema(left_plan.schema(), right_plan.schema(), &df_join_type) {
                Ok(schema) => schema,
                Err(_) => {
                    let empty_schema = DFSchema::empty();
                    return LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: std::sync::Arc::new(empty_schema),
                    });
                }
            };

        LogicalPlan::Join(Join {
            left: std::sync::Arc::new(left_plan),
            right: std::sync::Arc::new(right_plan),
            on,
            filter,
            join_type: df_join_type,
            join_constraint: JoinConstraint::On,
            schema: std::sync::Arc::new(join_schema),
            null_equals_null: false,
        })
    }

    fn visit_logical_iceberg_scan(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalIcebergScan,
    ) -> Self::Result {
        let rw_schema = plan.schema();

        let converter = IcebergArrowConvert {};
        let arrow_fields: Vec<Field> = rw_schema
            .fields()
            .iter()
            .map(|f| {
                converter
                    .to_arrow_field(&f.name, &f.data_type)
                    .expect("failed to convert RW field to Arrow field")
            })
            .collect();

        let arrow_schema = ArrowSchema::new(arrow_fields);
        let df_schema = match DFSchema::try_from(std::sync::Arc::new(arrow_schema)) {
            Ok(schema) => schema,
            Err(_) => {
                let empty_schema = DFSchema::empty();
                return LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: std::sync::Arc::new(empty_schema),
                });
            }
        };

        let table_name = if let Some(catalog) = plan.source_catalog() {
            catalog.name.clone()
        } else {
            "iceberg_table".to_owned()
        };

        LogicalPlan::TableScan(TableScan {
            table_name: table_name.into(),
            source: std::sync::Arc::new(DefaultTableSource::new(std::sync::Arc::new(
                EmptyTable::new(std::sync::Arc::new(df_schema.as_arrow().clone())),
            ))),
            projection: None,
            projected_schema: std::sync::Arc::new(df_schema),
            filters: vec![],
            fetch: None,
        })
    }
}

pub struct DefaultValueBehavior;
impl DefaultBehavior<DFLogicalPlan> for DefaultValueBehavior {
    fn apply(&self, _results: impl IntoIterator<Item = DFLogicalPlan>) -> DFLogicalPlan {
        panic!("RwToDfConverter: encountered unsupported node in default_behavior")
    }
}
