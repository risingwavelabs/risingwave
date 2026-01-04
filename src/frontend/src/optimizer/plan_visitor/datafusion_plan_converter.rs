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
    Aggregate, Expr as DFExpr, ExprSchemable, Join, JoinConstraint, LogicalPlan as DFLogicalPlan,
    LogicalPlan, TableScan, Values, build_join_schema,
};
use datafusion::prelude::lit;
use datafusion_common::{Column, DFSchema, NullEquality, ScalarValue};
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bail_not_implemented;

use crate::datafusion::{
    ColumnTrait, ConcatColumns, IcebergTableProvider, InputColumns, convert_agg_call,
    convert_column_order, convert_expr, convert_join_type, convert_window_expr,
};
use crate::error::{ErrorCode, Result as RwResult};
use crate::optimizer::plan_node::generic::{GenericPlanRef, TopNLimit};
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

    fn visit_logical_agg(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalAgg,
    ) -> Self::Result {
        // TODO: support grouping sets, rollup, cube
        // Risingwave will convert it to logical expand first, then aggregate
        // But datafusion doesn't have logical expand node, so we need to use other way to implement it
        let rw_input = plan.input();
        let df_input = self.visit(plan.input())?;
        let input_columns = InputColumns::new(df_input.schema().as_ref(), rw_input.schema());
        let group_expr = plan
            .group_key()
            .indices()
            .map(|index| DFExpr::Column(input_columns.column(index)))
            .collect_vec();
        let aggr_expr = plan
            .agg_calls()
            .iter()
            .map(|agg_call| {
                Ok(DFExpr::AggregateFunction(convert_agg_call(
                    agg_call,
                    &input_columns,
                )?))
            })
            .collect::<RwResult<Vec<DFExpr>>>()?;

        let aggregate = Aggregate::try_new(df_input, group_expr, aggr_expr)?;
        Ok(Arc::new(LogicalPlan::Aggregate(aggregate)))
    }

    fn visit_logical_filter(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalFilter,
    ) -> Self::Result {
        let rw_input = plan.input();
        let df_input = self.visit(rw_input.clone())?;
        let input_columns = InputColumns::new(df_input.schema().as_ref(), rw_input.schema());
        let predicate = match plan.predicate().as_expr_unless_true() {
            Some(expr) => convert_expr(&expr, &input_columns)?,
            None => lit(true),
        };

        let filter = datafusion::logical_expr::Filter::try_new(predicate, df_input)?;
        Ok(Arc::new(LogicalPlan::Filter(filter)))
    }

    fn visit_logical_project(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalProject,
    ) -> Self::Result {
        let rw_input = plan.input();
        let df_input = self.visit(rw_input.clone())?;

        let input_columns = InputColumns::new(df_input.schema().as_ref(), rw_input.schema());
        let mut df_exprs = plan
            .exprs()
            .iter()
            .map(|e| convert_expr(e, &input_columns))
            .collect::<RwResult<Vec<_>>>()?;

        // DataFusion requires unique expression names in projection.
        let mut duplicated_exprs = HashMap::new();
        for expr in &mut df_exprs {
            let (relation, field) = expr.to_field(df_input.schema())?;
            let count = duplicated_exprs
                .entry((relation.clone(), field.name().clone()))
                .or_insert(0);
            *count += 1;
            if *count > 1 {
                let take_expr = std::mem::replace(expr, DFExpr::Literal(ScalarValue::Null, None));
                *expr = take_expr.alias_qualified(relation, format!("{}@{}", field.name(), count));
            }
        }

        let projection = datafusion::logical_expr::Projection::try_new(df_exprs, df_input)?;
        Ok(Arc::new(LogicalPlan::Projection(projection)))
    }

    fn visit_logical_join(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalJoin,
    ) -> Self::Result {
        let rw_left = plan.left();
        let rw_right = plan.right();
        let df_left = self.visit(plan.left())?;
        let df_right = self.visit(plan.right())?;
        let concat_columns = ConcatColumns::new(
            df_left.schema(),
            rw_left.schema(),
            df_right.schema(),
            rw_right.schema(),
        );

        // Convert join type from RisingWave to DataFusion
        let df_join_type = convert_join_type(plan.join_type())?;

        // Extract equijoin conditions - get equal join key pairs
        let left_col_num = rw_left.schema().len();
        let right_col_num = rw_right.schema().len();
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

        let join_schema = build_join_schema(df_left.schema(), df_right.schema(), &df_join_type)?;
        let null_equality = if null_equals_null {
            NullEquality::NullEqualsNull
        } else {
            NullEquality::NullEqualsNothing
        };
        let join = Join {
            left: df_left,
            right: df_right,
            on: join_on_exprs,
            filter,
            join_type: df_join_type,
            join_constraint: JoinConstraint::On,
            schema: Arc::new(join_schema),
            null_equality,
        };
        if plan.output_indices_are_trivial() {
            return Ok(Arc::new(LogicalPlan::Join(join)));
        }

        let projection_exprs = plan
            .output_indices()
            .iter()
            .map(|&idx| DFExpr::Column(Column::from(join.schema.qualified_field(idx))))
            .collect_vec();
        let projection = datafusion::logical_expr::Projection::try_new(
            projection_exprs,
            Arc::new(LogicalPlan::Join(join)),
        )?;
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
        let input_columns = InputColumns::new(&df_schema, rw_schema);

        let mut df_rows: Vec<Vec<DFExpr>> = Vec::with_capacity(plan.rows().len());
        for row in plan.rows() {
            let mut df_row: Vec<DFExpr> = Vec::with_capacity(row.len());
            for expr in row {
                df_row.push(convert_expr(expr, &input_columns)?);
            }
            df_rows.push(df_row);
        }

        Ok(Arc::new(LogicalPlan::Values(Values {
            schema: Arc::new(df_schema),
            values: df_rows,
        })))
    }

    fn visit_logical_limit(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalLimit,
    ) -> Self::Result {
        let input_plan = self.visit(plan.input())?;
        let offset: i64 = plan.offset().try_into().map_err(|_| {
            ErrorCode::InternalError(
                format!("DataFusionPlanConverter: limit offset {} is too large to convert to i64. DataFusion limit offset can only support i64.", plan.offset()),
            )
        })?;
        let limit: i64 = plan.limit().try_into().map_err(|_| {
            ErrorCode::InternalError(
                format!("DataFusionPlanConverter: limit {} is too large to convert to i64. DataFusion limit can only support i64.", plan.limit()),
            )
        })?;

        let limit = datafusion::logical_expr::Limit {
            skip: Some(Box::new(lit(offset))),
            fetch: Some(Box::new(lit(limit))),
            input: input_plan,
        };
        Ok(Arc::new(LogicalPlan::Limit(limit)))
    }

    fn visit_logical_top_n(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalTopN,
    ) -> Self::Result {
        let rw_input = plan.input();
        let df_input = self.visit(rw_input.clone())?;

        let TopNLimit::Simple(limit) = plan.limit_attr() else {
            bail_not_implemented!(
                "DataFusionPlanConverter: LogicalTopN with_ties is not supported"
            );
        };
        if !plan.group_key().is_empty() {
            bail_not_implemented!(
                "DataFusionPlanConverter: LogicalTopN with a non-empty group key is not supported. This may arise from DISTINCT ON or correlated joins."
            );
        }
        let offset = plan.offset();

        let input_columns = InputColumns::new(df_input.schema().as_ref(), rw_input.schema());
        let sort_expr = plan
            .topn_order()
            .column_orders
            .iter()
            .map(|order| convert_column_order(order, &input_columns))
            .collect_vec();
        let fetch = offset.saturating_add(limit).min(usize::MAX as _) as usize;
        let sort = datafusion::logical_expr::Sort {
            expr: sort_expr,
            input: df_input,
            fetch: Some(fetch),
        };
        let mut result = Arc::new(LogicalPlan::Sort(sort));

        if offset > 0 {
            let limit = datafusion::logical_expr::Limit {
                skip: Some(Box::new(lit(offset))),
                fetch: Some(Box::new(lit(limit))),
                input: result,
            };
            result = Arc::new(LogicalPlan::Limit(limit));
        }

        Ok(result)
    }

    fn visit_logical_over_window(
        &mut self,
        plan: &crate::optimizer::plan_node::LogicalOverWindow,
    ) -> Self::Result {
        let rw_input = plan.input();
        let df_input = self.visit(rw_input.clone())?;

        let input_columns = InputColumns::new(df_input.schema().as_ref(), rw_input.schema());
        let df_exprs = plan
            .window_functions()
            .iter()
            .map(|wf| convert_window_expr(wf, &input_columns))
            .collect::<RwResult<Vec<_>>>()?;

        let window_plan = datafusion::logical_expr::Window::try_new(df_exprs, df_input)?;
        Ok(Arc::new(LogicalPlan::Window(window_plan)))
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
