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

use risingwave_sqlparser::ast::SetExpr;

use crate::sqlreduce::passes::{Ast, Transform, extract_query, extract_query_mut};

/// Remove individual expressions from the GROUP BY clause.
///
/// Example:
/// ```sql
/// SELECT a, COUNT(*) FROM t GROUP BY a, b;
/// ```
/// May be reduced to:
/// ```sql
/// SELECT a, COUNT(*) FROM t GROUP BY b;
/// ```
pub struct GroupByRemove;

impl Transform for GroupByRemove {
    fn name(&self) -> String {
        "groupby_remove".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for i in 0..select.group_by.len() {
                reduction_points.push(i);
            }
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                select.group_by.remove(i);
            }
        }
        ast.clone()
    }
}

/// Remove individual items from the ORDER BY clause.
///
/// Example:
/// ```sql
/// SELECT * FROM t ORDER BY a, b;
/// ```
/// May be reduced to:
/// ```sql
/// SELECT * FROM t ORDER BY b;
/// ```
pub struct OrderByRemove;

impl Transform for OrderByRemove {
    fn name(&self) -> String {
        "orderby_remove".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast) {
            for i in 0..query.order_by.len() {
                reduction_points.push(i);
            }
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast) {
            for i in reduction_points {
                query.order_by.remove(i);
            }
        }
        ast.clone()
    }
}

/// Remove the entire WHERE clause from the query.
///
/// Example:
/// ```sql
/// SELECT * FROM t WHERE a > 1;
/// ```
/// Will be reduced to:
/// ```sql
/// SELECT * FROM t;
/// ```
pub struct WhereRemove;

impl Transform for WhereRemove {
    fn name(&self) -> String {
        "where_remove".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
            && select.selection.is_some()
        {
            reduction_points.push(0);
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for _ in reduction_points {
                select.selection = None;
            }
        }
        tracing::info!("where_remove ast: {:?}", ast);
        ast.clone()
    }
}

/// Remove individual table sources from the FROM clause.
///
/// Example:
/// ```sql
/// SELECT * FROM t1, t2;
/// ```
/// May be reduced to:
/// ```sql
/// SELECT * FROM t2;
/// ```
pub struct FromRemove;

impl Transform for FromRemove {
    fn name(&self) -> String {
        "from_remove".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for i in 0..select.from.len() {
                reduction_points.push(i);
            }
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                select.from.remove(i);
            }
        }
        ast.clone()
    }
}

/// Remove individual items from the SELECT list.
///
/// Example:
/// ```sql
/// SELECT a, b, c FROM t;
/// ```
/// May be reduced to:
/// ```sql
/// SELECT a, c FROM t;
/// ```
pub struct SelectItemRemove;

impl Transform for SelectItemRemove {
    fn name(&self) -> String {
        "select_item_remove".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for i in 0..select.projection.len() {
                reduction_points.push(i);
            }
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                select.projection.remove(i);
            }
        }
        ast.clone()
    }
}

pub struct HavingRemove;

impl Transform for HavingRemove {
    fn name(&self) -> String {
        "having_remove".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
            && select.having.is_some()
        {
            reduction_points.push(0);
        }

        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for _ in reduction_points {
                select.having = None;
            }
        }
        ast.clone()
    }
}
