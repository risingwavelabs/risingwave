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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;

use crate::binder::{BoundSelect, BoundSetExpr, Relation};
use crate::expr::{Expr, ExprImpl};
use crate::optimizer::plan_node::PlanRef;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_set_expr(
        &mut self,
        set_expr: BoundSetExpr,
    ) -> Result<(PlanRef, HashMap<String, ColumnDesc>)> {
        match set_expr {
            BoundSetExpr::Select(s) => {
                let column_name_to_desc = self.store_struct_column(&s)?;
                Ok((self.plan_select(*s)?, column_name_to_desc))
            }
            BoundSetExpr::Values(v) => Ok((self.plan_values(*v)?, HashMap::new())),
        }
    }

    /// For every struct select item, store its `alias_name` and `column_desc` in the map.
    fn store_struct_column(&mut self, select: &BoundSelect) -> Result<HashMap<String, ColumnDesc>> {
        let table: Vec<ColumnDesc> = {
            match &select.from {
                Some(relation) => Self::extract_column_descs(relation)?,
                None => vec![],
            }
        };
        let mut column_name_to_desc = HashMap::new();

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
                                // Change struct column desc and field descs prefix names to alias.
                                column.change_prefix_name(column.name.clone(), name.clone());
                                name.clone()
                            }
                            None => column.name.clone(),
                        }
                    };
                    column_name_to_desc.insert(name, column);
                }
            }
        }
        Ok(column_name_to_desc)
    }

    /// Extract `column_descs` from relation.
    fn extract_column_descs(relation: &Relation) -> Result<Vec<ColumnDesc>> {
        // TODO: Support other relation type.
        match relation {
            Relation::Source(source) => Ok(source
                .catalog
                .columns
                .iter()
                .map(|c| c.column_desc.clone())
                .collect_vec()),
            _ => Ok(vec![]),
        }
    }
}
