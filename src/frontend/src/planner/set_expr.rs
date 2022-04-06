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

use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;

use crate::binder::{BoundSetExpr, Relation};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::PlanRef;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_set_expr(&mut self, set_expr: BoundSetExpr) -> Result<PlanRef> {
        match set_expr {
            BoundSetExpr::Select(s) => {
                let table = {
                    if let Relation::BaseTable(table) = s.from.as_ref().unwrap() {
                        &table.table_desc.columns
                    } else {
                        return Err(RwError::from(InternalError("not have index".to_string())));
                    }
                };
                for i in 0..s.select_items.len() {
                    let item = &s.select_items[i];
                    let name = s.aliases[i].as_ref().unwrap();
                    if let DataType::Struct { .. } = item.return_type() {
                        if let ExprImpl::InputRef(expr) = item {
                            self.map.insert(
                                name.clone(),
                                table
                                    .get(expr.index())
                                    .ok_or_else(|| {
                                        RwError::from(InternalError("not have index".to_string()))
                                    })?
                                    .clone(),
                            );
                        }
                    }
                }
                self.plan_select(*s)
            }
            BoundSetExpr::Values(v) => self.plan_values(*v),
        }
    }
}
