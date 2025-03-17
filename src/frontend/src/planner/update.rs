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

use fixedbitset::FixedBitSet;
use risingwave_common::types::{DataType, Scalar};
use risingwave_pb::expr::expr_node::Type;

use super::Planner;
use crate::binder::{BoundUpdate, UpdateProject};
use crate::error::Result;
use crate::expr::{ExprImpl, ExprType, FunctionCall, InputRef, Literal};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::{LogicalProject, LogicalUpdate, generic};
use crate::optimizer::property::{Order, RequiredDist};
use crate::optimizer::{PlanRef, PlanRoot};

impl Planner {
    pub(super) fn plan_update(&mut self, update: BoundUpdate) -> Result<PlanRoot> {
        let returning = !update.returning_list.is_empty();

        let scan = self.plan_base_table(&update.table)?;
        let input = if let Some(expr) = update.selection {
            self.plan_where(scan, expr)?
        } else {
            scan
        };
        let old_schema_len = input.schema().len();

        // Extend table scan with updated columns.
        let with_new: PlanRef = {
            let mut plan = input;

            let mut exprs: Vec<ExprImpl> = plan
                .schema()
                .data_types()
                .into_iter()
                .enumerate()
                .map(|(index, data_type)| InputRef::new(index, data_type).into())
                .collect();

            exprs.extend(update.exprs);

            // Substitute subqueries into `LogicalApply`s.
            if exprs.iter().any(|e| e.has_subquery()) {
                (plan, exprs) = self.substitute_subqueries(plan, exprs)?;
            }

            LogicalProject::new(plan, exprs).into()
        };

        let mut olds = Vec::new();
        let mut news = Vec::new();

        for (i, col) in update.table.table_catalog.columns().iter().enumerate() {
            // Skip generated columns and system columns.
            if !col.can_dml() {
                continue;
            }
            let data_type = col.data_type();

            let old: ExprImpl = InputRef::new(i, data_type.clone()).into();

            let mut new: ExprImpl = match (update.projects.get(&i)).map(|p| p.offset(old_schema_len)) {
                Some(UpdateProject::Simple(j)) => InputRef::new(j, data_type.clone()).into(),
                Some(UpdateProject::Composite(j, field)) => FunctionCall::new_unchecked(
                    Type::Field,
                    vec![
                        InputRef::new(j, with_new.schema().data_types()[j].clone()).into(), // struct
                        Literal::new(Some((field as i32).to_scalar_value()), DataType::Int32)
                            .into(),
                    ],
                    data_type.clone(),
                )
                .into(),

                None => old.clone(),
            };
            if !col.nullable() {
                new = FunctionCall::new_unchecked(
                    ExprType::CheckNotNull,
                    vec![
                        new,
                        ExprImpl::literal_varchar(col.name().to_owned()),
                        ExprImpl::literal_varchar(update.table_name.clone()),
                    ],
                    data_type.clone(),
                )
                .into();
            }

            olds.push(old);
            news.push(new);
        }

        let mut plan: PlanRef = LogicalUpdate::from(generic::Update::new(
            with_new,
            update.table_name.clone(),
            update.table_id,
            update.table_version_id,
            olds,
            news,
            returning,
        ))
        .into();

        if returning {
            plan = LogicalProject::create(plan, update.returning_list);
        }

        // For update, frontend will only schedule one task so do not need this to be single.
        let dist = RequiredDist::Any;
        let mut out_fields = FixedBitSet::with_capacity(plan.schema().len());
        out_fields.insert_range(..);
        let out_names = if returning {
            update.returning_schema.expect("If returning list is not empty, should provide returning schema in BoundDelete.").names()
        } else {
            plan.schema().names()
        };

        let root = PlanRoot::new_with_logical_plan(plan, dist, Order::any(), out_fields, out_names);
        Ok(root)
    }
}
