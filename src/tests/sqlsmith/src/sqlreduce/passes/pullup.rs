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

use risingwave_sqlparser::ast::{Array, Expr, Query, SelectItem, SetExpr, Statement};

use crate::sqlreduce::passes::{Ast, Transform, extract_query, extract_query_mut};

/// Replace binary expressions in SELECT projections with their right-hand operand.
///
/// This transformation simplifies binary operations by discarding the left-hand side
/// and preserving only the right-hand expression. It is useful for reducing SQL size
/// when correctness (e.g., failure reproduction) is maintained without full computation.
///
/// Example:
/// ```sql
/// SELECT 1 + 2 + 3;
/// ```
/// Will be reduced to:
/// ```sql
/// SELECT 3;
/// ```
pub struct BinaryOperatorPullup;

impl Transform for BinaryOperatorPullup {
    fn name(&self) -> String {
        "binary_operator_pullup".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for (i, item) in select.projection.iter().enumerate() {
                if let SelectItem::UnnamedExpr(expr) = item
                    && let Expr::BinaryOp { .. } = expr
                {
                    reduction_points.push(i);
                }
            }
        }

        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                if let SelectItem::UnnamedExpr(ref mut expr) = select.projection[i]
                    && let Expr::BinaryOp { right, .. } = expr
                {
                    *expr = *right.clone();
                }
            }
        }

        ast.clone()
    }
}

/// Simplify `CASE` expressions in SELECT projections by pulling up the first `THEN` result.
///
/// This transformation replaces a full `CASE` expression with the expression of the first
/// `WHEN ... THEN ...` branch. It is used to simplify the query structure under the
/// assumption that correctness (e.g., failure reproduction) is preserved.
///
/// Example:
/// ```sql
/// SELECT CASE
///          WHEN a = 1 THEN 'foo'
///          WHEN a = 2 THEN 'bar'
///          ELSE 'baz'
///        END;
/// ```
/// Will be reduced to:
/// ```sql
/// SELECT 'foo';
/// ```
pub struct CasePullup;

impl Transform for CasePullup {
    fn name(&self) -> String {
        "case_pullup".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for (i, item) in select.projection.iter().enumerate() {
                if let SelectItem::UnnamedExpr(expr) = item
                    && let Expr::Case { .. } = expr
                {
                    reduction_points.push(i);
                }
            }
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                if let SelectItem::UnnamedExpr(ref mut expr) = select.projection[i]
                    && let Expr::Case { results, .. } = expr
                {
                    *expr = results[0].clone();
                }
            }
        }

        ast.clone()
    }
}

/// Simplify `ROW` expressions in SELECT projections by pulling up the first element.
///
/// This transformation replaces a `ROW(...)` expression with its first component,
/// reducing the structure without preserving full semantics. It is intended for
/// query reduction purposes where correctness (e.g., error reproduction) remains.
///
/// Example:
/// ```sql
/// SELECT ROW(1, 2, 3);
/// ```
/// Will be reduced to:
/// ```sql
/// SELECT 1;
/// ```
pub struct RowPullup;

impl Transform for RowPullup {
    fn name(&self) -> String {
        "row_pullup".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for (i, item) in select.projection.iter().enumerate() {
                if let SelectItem::UnnamedExpr(expr) = item
                    && let Expr::Row { .. } = expr
                {
                    reduction_points.push(i);
                }
            }
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                if let SelectItem::UnnamedExpr(ref mut expr) = select.projection[i]
                    && let Expr::Row(elements) = expr
                {
                    *expr = Expr::Row(vec![elements[0].clone()]);
                }
            }
        }
        ast.clone()
    }
}

/// Simplify array constructors in SELECT projections by pulling up the first element.
///
/// This transformation replaces an `ARRAY[...]` expression with its first element.
/// It helps reduce SQL complexity when the full array is not necessary for reproducing errors.
///
/// Example:
/// ```sql
/// SELECT ARRAY[1, 2, 3];
/// ```
/// Will be reduced to:
/// ```sql
/// SELECT ARRAY[1];
/// ```
pub struct ArrayPullup;

impl Transform for ArrayPullup {
    fn name(&self) -> String {
        "array_pullup".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for (i, item) in select.projection.iter().enumerate() {
                if let SelectItem::UnnamedExpr(expr) = item
                    && let Expr::Array { .. } = expr
                {
                    reduction_points.push(i);
                }
            }
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        if let Some(query) = extract_query_mut(ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                if let SelectItem::UnnamedExpr(ref mut expr) = select.projection[i]
                    && let Expr::Array(array) = expr
                    && let Some(elem) = array.elem.first()
                {
                    *expr = Expr::Array(Array {
                        elem: vec![elem.clone()],
                        named: array.named,
                    });
                }
            }
        }
        ast.clone()
    }
}

/// Simplify set operations (`UNION`, `INTERSECT`, etc.) by pulling up either side.
///
/// This transformation replaces a `SetOperation` expression with its left or right subquery,
/// effectively discarding the other half of the operation. It can be useful to isolate failure-inducing inputs.
///
/// Example:
/// ```sql
/// (SELECT 1) UNION (SELECT 2);
/// ```
/// May be reduced to:
/// ```sql
/// SELECT 1;
/// ```
/// or:
/// ```sql
/// SELECT 2;
/// ```
pub struct SetOperationPullup;

impl Transform for SetOperationPullup {
    fn name(&self) -> String {
        "set_operation_pullup".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::SetOperation { .. } = &query.body
        {
            reduction_points.push(0); // left
            reduction_points.push(1); // right
        }
        reduction_points
    }

    fn apply_on(&self, ast: &mut Ast, reduction_points: Vec<usize>) -> Ast {
        let mut new_ast = ast.clone();
        if let Some(query) = extract_query_mut(&mut new_ast)
            && let SetExpr::SetOperation { left, right, .. } = &mut query.body
        {
            if reduction_points.contains(&0) {
                // left
                new_ast = Statement::Query(Box::new(Query {
                    body: *left.clone(),
                    ..query.clone()
                }));
            } else if reduction_points.contains(&1) {
                // right
                new_ast = Statement::Query(Box::new(Query {
                    body: *right.clone(),
                    ..query.clone()
                }));
            }
        }
        new_ast
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_sql;

    #[test]
    fn test_binary_operator_pullup_with_single_binary() {
        let sql = "SELECT 1 + 2 + 3;";
        let ast = parse_sql(sql);
        let reduction_points = BinaryOperatorPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0]);

        let new_ast = BinaryOperatorPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT 3;")[0].clone());
    }

    #[test]
    fn test_binary_operator_pullup_with_multiple_binary() {
        let sql = "SELECT 1 + 2 + 3, 4 + 5 + 6;";
        let ast = parse_sql(sql);
        let reduction_points = BinaryOperatorPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0, 1]);

        let new_ast = BinaryOperatorPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT 3, 6;")[0].clone());
    }

    #[test]
    fn test_case_pullup_with_single_when() {
        let sql = "SELECT CASE WHEN 1 = 1 THEN 1 ELSE 2 END;";
        let ast = parse_sql(sql);
        let reduction_points = CasePullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0]);

        let new_ast = CasePullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT 1;")[0].clone());
    }

    #[test]
    fn test_row_pullup_with_single_row() {
        let sql = "SELECT ROW(1, 2, 3);";
        let ast = parse_sql(sql);
        let reduction_points = RowPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0]);

        let new_ast = RowPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT ROW(1);")[0].clone());
    }

    #[test]
    fn test_row_pullup_with_multiple_rows() {
        let sql = "SELECT ROW(1, 2, 3), ROW(4, 5, 6);";
        let ast = parse_sql(sql);
        let reduction_points = RowPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0, 1]);

        let new_ast = RowPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT ROW(1), ROW(4);")[0].clone());
    }

    #[test]
    fn test_array_pullup_with_single_array() {
        let sql = "SELECT ARRAY[1, 2, 3];";
        let ast = parse_sql(sql);
        let reduction_points = ArrayPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0]);

        let new_ast = ArrayPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT ARRAY[1];")[0].clone());
    }

    #[test]
    fn test_array_pullup_with_multiple_arrays() {
        let sql = "SELECT ARRAY[1, 2, 3], ARRAY[4, 5, 6];";
        let ast = parse_sql(sql);
        let reduction_points = ArrayPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0, 1]);

        let new_ast = ArrayPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT ARRAY[1], ARRAY[4];")[0].clone());
    }

    #[test]
    fn test_case_pullup_with_multiple_when() {
        let sql = "SELECT CASE WHEN 1 = 1 THEN 1 WHEN 2 = 2 THEN 2 ELSE 3 END;";
        let ast = parse_sql(sql);
        let reduction_points = CasePullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0]);

        let new_ast = CasePullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT 1;")[0].clone());
    }

    #[test]
    fn test_set_operation_pullup_union() {
        let sql = "SELECT 1 UNION SELECT 2;";
        let ast = parse_sql(sql);
        let reduction_points = SetOperationPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0, 1]);

        let new_ast = SetOperationPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT 1;")[0].clone());
    }

    #[test]
    fn test_set_operation_pullup_intersect() {
        let sql = "SELECT 1 INTERSECT SELECT 2;";
        let ast = parse_sql(sql);
        let reduction_points = SetOperationPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0, 1]);

        let new_ast = SetOperationPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT 1;")[0].clone());
    }

    #[test]
    fn test_set_operation_pullup_except() {
        let sql = "SELECT 1 EXCEPT SELECT 2;";
        let ast = parse_sql(sql);
        let reduction_points = SetOperationPullup.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0, 1]);

        let new_ast = SetOperationPullup.apply_on(&mut ast[0].clone(), reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT 1;")[0].clone());
    }
}
