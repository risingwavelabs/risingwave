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
use datafusion::logical_expr::{Expr as DFExpr, LogicalPlan as DFLogicalPlan, LogicalPlan, Values};
use datafusion_common::{DFSchema, ScalarValue};
use risingwave_common::types::{DataType as RWDataType, ScalarImpl};

use crate::optimizer::PlanVisitor;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{Logical, LogicalValues, PlanRef};
use crate::optimizer::plan_visitor::{DefaultBehavior, LogicalPlanVisitor};

#[derive(Debug, Clone, Default)]
pub struct RWToDFConverter {}

impl RWToDFConverter {
    pub fn convert(&mut self, plan: PlanRef<Logical>) -> DFLogicalPlan {
        PlanVisitor::visit(self, plan)
    }
}

impl LogicalPlanVisitor for RWToDFConverter {
    type DefaultBehavior = DefaultValueBehavior;
    type Result = DFLogicalPlan;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValueBehavior
    }

    fn visit_logical_values(&mut self, plan: &LogicalValues) -> Self::Result {
        // Build Arrow fields from RW schema (names + types). For basic implementation,
        // map a few common types; unsupported types default to Utf8.
        let rw_schema = plan.schema();
        let arrow_fields: Vec<Field> = rw_schema
            .fields()
            .iter()
            .map(|f| {
                let name = f.name.clone();
                // map basic types; for simplicity assume nullable true
                let arrow_dt = match f.data_type() {
                    RWDataType::Int32 => datafusion::arrow::datatypes::DataType::Int32,
                    RWDataType::Int64 => datafusion::arrow::datatypes::DataType::Int64,
                    RWDataType::Float64 => datafusion::arrow::datatypes::DataType::Float64,
                    RWDataType::Boolean => datafusion::arrow::datatypes::DataType::Boolean,
                    _ => datafusion::arrow::datatypes::DataType::Utf8,
                };
                Field::new(&name, arrow_dt, true)
            })
            .collect();

        let arrow_schema = ArrowSchema::new(arrow_fields);
        let df_schema = DFSchema::try_from(std::sync::Arc::new(arrow_schema))
            .expect("failed to create DFSchema from Arrow schema");

        // Convert rows: only support Literal for now
        let mut df_rows: Vec<Vec<DFExpr>> = Vec::with_capacity(plan.rows().len());
        for row in plan.rows() {
            let mut df_row: Vec<DFExpr> = Vec::with_capacity(row.len());
            for expr in row {
                match expr {
                    crate::expr::ExprImpl::Literal(lit) => {
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
            values: df_rows,
            schema: std::sync::Arc::new(df_schema),
        })
    }
}

pub struct DefaultValueBehavior;
impl DefaultBehavior<DFLogicalPlan> for DefaultValueBehavior {
    fn apply(&self, _results: impl IntoIterator<Item = DFLogicalPlan>) -> DFLogicalPlan {
        panic!("RWToDFConverter: encountered unsupported node in default_behavior")
    }
}
