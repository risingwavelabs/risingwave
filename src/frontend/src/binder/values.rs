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

use itertools::Itertools;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Values;

use super::bind_context::Clause;
use crate::binder::Binder;
use crate::expr::{least_restrictive, Expr as _, ExprImpl};

#[derive(Debug)]
pub struct BoundValues {
    pub rows: Vec<Vec<ExprImpl>>,
    pub schema: Schema,
}

impl Binder {
    /// Bind [`Values`] with given `expected_types`. If no types are expected, a compatible type for
    /// all rows will be used.
    pub(super) fn bind_values(
        &mut self,
        values: Values,
        expected_types: Option<Vec<DataType>>,
    ) -> Result<BoundValues> {
        assert!(!values.0.is_empty());

        self.context.clause = Some(Clause::Values);
        let vec2d = values.0;
        let bound = vec2d
            .into_iter()
            .map(|vec| vec.into_iter().map(|expr| self.bind_expr(expr)).collect())
            .collect::<Result<Vec<Vec<_>>>>()?;
        self.context.clause = None;

        // Calculate column types.
        let types = match expected_types {
            Some(types) => {
                if types.len() != bound[0].len() {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "values length mismatched: expected {}, given {}",
                        types.len(),
                        bound[0].len(),
                    ))
                    .into());
                }
                types
            }
            None => {
                let mut types = bound[0]
                    .iter()
                    .map(|expr| expr.return_type())
                    .collect::<Vec<DataType>>();
                for vec in &bound {
                    for (expr, ty) in vec.iter().zip_eq(types.iter_mut()) {
                        if !expr.is_null() {
                            *ty = least_restrictive(ty.clone(), expr.return_type())?
                        }
                    }
                }
                types
            }
        };

        // Insert casts.
        let rows = bound
            .into_iter()
            .map(|vec| {
                vec.into_iter()
                    .zip_eq(types.iter().cloned())
                    // When `types` are from `INSERT`, cast-ability has not been checked yet.
                    // When `types` are from `least_restrictive`, it's always ok.
                    // Because `least_restrictive` uses implicit cast, all of which allowed in
                    // assign context.
                    .map(|(expr, ty)| expr.cast_assign(ty))
                    .try_collect()
            })
            .try_collect()?;

        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());
        Ok(BoundValues { rows, schema })
    }
}

#[cfg(test)]
mod tests {

    use itertools::zip_eq;
    use risingwave_sqlparser::ast::{Expr, Value};

    use super::*;
    use crate::binder::test_utils::mock_binder;

    #[test]
    fn test_bind_values() {
        let mut binder = mock_binder();

        // Test i32 -> decimal.
        let expr1 = Expr::Value(Value::Number("1".to_string(), false));
        let expr2 = Expr::Value(Value::Number("1.1".to_string(), false));
        let values = Values(vec![vec![expr1], vec![expr2]]);
        let res = binder.bind_values(values, None).unwrap();

        let types = vec![DataType::Decimal];
        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());

        assert_eq!(res.schema, schema);
        for vec in res.rows {
            for (expr, ty) in zip_eq(vec, schema.data_types()) {
                assert_eq!(expr.return_type(), ty);
            }
        }
    }
}
