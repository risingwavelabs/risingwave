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
use crate::expr::{align_types, ExprImpl};

#[derive(Debug, Clone)]
pub struct BoundValues {
    pub rows: Vec<Vec<ExprImpl>>,
    pub schema: Schema,
}

impl BoundValues {
    /// The schema returned of this [`BoundValues`].
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
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
        let mut bound = vec2d
            .into_iter()
            .map(|vec| vec.into_iter().map(|expr| self.bind_expr(expr)).collect())
            .collect::<Result<Vec<Vec<_>>>>()?;
        self.context.clause = None;

        let num_columns = bound[0].len();
        if bound.iter().any(|row| row.len() != num_columns) {
            return Err(
                ErrorCode::BindError("VALUES lists must all be the same length".into()).into(),
            );
        }
        // Calculate column types.
        let types = match expected_types {
            Some(types) => {
                bound = bound
                    .into_iter()
                    .map(|vec| Self::cast_on_insert(types.clone(), vec))
                    .try_collect()?;

                types
            }
            None => (0..num_columns)
                .map(|col_index| align_types(bound.iter_mut().map(|row| &mut row[col_index])))
                .try_collect()?,
        };

        let schema = Schema::new(types.into_iter().map(Field::unnamed).collect());
        Ok(BoundValues {
            rows: bound,
            schema,
        })
    }
}

#[cfg(test)]
mod tests {

    use itertools::zip_eq;
    use risingwave_sqlparser::ast::{Expr, Value};

    use super::*;
    use crate::binder::test_utils::mock_binder;
    use crate::expr::Expr as _;

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
