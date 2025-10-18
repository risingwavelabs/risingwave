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

use risingwave_sqlparser::ast::{Expr, SelectItem, SetExpr, Value};

use crate::sqlreduce::passes::{Ast, Transform, extract_query, extract_query_mut};

/// Replace scalar constants in SELECT projections with canonical placeholders.
///
/// This transformation replaces literal values (e.g., numbers, strings, booleans)
/// in the projection list with fixed placeholders. It simplifies the query while
/// preserving structure, which is useful for debugging or failure reduction.
///
/// Replacement rules:
/// - Number → 1
/// - String → 'a'
/// - Boolean → true
/// - Null → remains null
/// - Other values → replaced with null
///
/// Example:
/// ```sql
/// SELECT 42, 'hello', false, null;
/// ```
/// Will be reduced to:
/// ```sql
/// SELECT 1, 'a', true, null;
/// ```
pub struct ScalarReplace;

impl ScalarReplace {
    fn replacement_for_value(v: &Value) -> Value {
        match v {
            Value::Number(_) => Value::Number("1".to_owned()),
            Value::Boolean(_) => Value::Boolean(true),
            Value::SingleQuotedString(_) => Value::SingleQuotedString("a".to_owned()),
            Value::Null => Value::Null,
            _ => Value::Null,
        }
    }
}

impl Transform for ScalarReplace {
    fn name(&self) -> String {
        "scalar_replace".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for (i, item) in select.projection.iter().enumerate() {
                if let SelectItem::UnnamedExpr(Expr::Value(_)) = item {
                    reduction_points.push(i);
                }
            }
        }
        reduction_points
    }

    fn apply_on(&self, mut ast: Ast, reduction_points: &[usize]) -> Ast {
        if let Some(query) = extract_query_mut(&mut ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                if let SelectItem::UnnamedExpr(Expr::Value(ref mut v)) = select.projection[*i] {
                    let new_v = Self::replacement_for_value(v);
                    *v = new_v;
                }
            }
        }
        ast.clone()
    }
}

/// Replace all expressions in SELECT projections with NULL.
///
/// This transformation simplifies a query by replacing each unnamed expression
/// in the SELECT list with `NULL`. It helps isolate whether the structure or
/// presence of an expression is causing a failure, rather than its actual value.
///
/// Example:
/// ```sql
/// SELECT a + b, 42, 'hello';
/// ```
/// Will be reduced to:
/// ```sql
/// SELECT NULL, NULL, NULL;
/// ```
///
/// Note: This transformation discards all computation and semantic meaning
/// of expressions in the projection. It is intended purely for structural minimization.
pub struct NullReplace;

impl Transform for NullReplace {
    fn name(&self) -> String {
        "null_replace".to_owned()
    }

    fn get_reduction_points(&self, ast: Ast) -> Vec<usize> {
        let mut reduction_points = Vec::new();
        if let Some(query) = extract_query(&ast)
            && let SetExpr::Select(select) = &query.body
        {
            for (i, _) in select.projection.iter().enumerate() {
                reduction_points.push(i);
            }
        }
        reduction_points
    }

    fn apply_on(&self, mut ast: Ast, reduction_points: &[usize]) -> Ast {
        if let Some(query) = extract_query_mut(&mut ast)
            && let SetExpr::Select(select) = &mut query.body
        {
            for i in reduction_points {
                select.projection[*i] = SelectItem::UnnamedExpr(Expr::Value(Value::Null));
            }
        }
        ast.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse_sql;

    #[test]
    fn test_scalar_replace() {
        let sql = "SELECT 42, 'hello', false, null;";
        let ast = parse_sql(sql);
        let reduction_points = ScalarReplace.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0, 1, 2, 3]);

        let new_ast = ScalarReplace.apply_on(ast[0].clone(), &reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT 1, 'a', true, null;")[0].clone());
    }

    #[test]
    fn test_null_replace() {
        let sql = "SELECT a, b, c;";
        let ast = parse_sql(sql);
        let reduction_points = NullReplace.get_reduction_points(ast[0].clone());
        assert_eq!(reduction_points, vec![0, 1, 2]);

        let new_ast = NullReplace.apply_on(ast[0].clone(), &reduction_points);
        assert_eq!(new_ast, parse_sql("SELECT NULL, NULL, NULL;")[0].clone());
    }
}
