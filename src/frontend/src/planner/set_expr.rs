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

use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;

use crate::binder::{BoundSelect, BoundSetExpr, Relation};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::PlanRef;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_set_expr(&mut self, set_expr: BoundSetExpr) -> Result<PlanRef> {
        match set_expr {
            BoundSetExpr::Select(s) => {
                self.store_struct_column(&s)?;
                self.plan_select(*s)
            }
            BoundSetExpr::Values(v) => self.plan_values(*v),
        }
    }

    fn store_struct_column(&mut self, select: &BoundSelect) -> Result<()> {
        // TODO: Support other relation type.
        let table = Self::extract_source(select.from.as_ref().unwrap())?;

        // For every struct select item, store its alias name or column name
        // and column desc in the map. This map will be used in StreamMaterialize::create.
        for i in 0..select.select_items.len() {
            let item = &select.select_items[i];
            if let DataType::Struct { .. } = item.return_type() {
                if let ExprImpl::InputRef(expr) = item {
                    let mut column = table
                        .get(expr.index())
                        .ok_or_else(|| RwError::from(InternalError("not found index".to_string())))?
                        .clone();

                    let name = {
                        match select.aliases.get(i).ok_or_else(|| {
                            RwError::from(InternalError("index out of range".to_string()))
                        })? {
                            Some(name) => {
                                column.change_prefix_name(column.name.clone(), name.clone());
                                name.clone()
                            }
                            None => column.name.clone(),
                        }
                    };
                    self.name_to_column_desc.insert(name, column);
                }
            }
        }
        Ok(())
    }

    fn extract_source(_relation: &Relation) -> Result<Vec<ColumnDesc>> {
        unimplemented!()
        // match relation {
        //     // Relation::BaseTable(table) => Ok(table.table_catalog.columns.clone()),
        //     // Relation::Join(join) => {
        //     //     let mut left_table = Self::extract_table(&join.left)?;
        //     //     let mut right_table = Self::extract_table(&join.left)?;
        //     //     left_table.append(&mut right_table);
        //     //     Ok(left_table)
        //     // }
        //     _ => {
        //         unimplemented!()
        //     }
        // }
    }
}
