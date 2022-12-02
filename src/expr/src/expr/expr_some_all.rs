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

use std::sync::Arc;

use itertools::{multizip, Itertools};
use risingwave_common::array::{ArrayBuilder, ArrayRef, BoolArrayBuilder, DataChunk};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, Datum, Scalar, ScalarImpl};
use risingwave_pb::expr::expr_node::Type;

use super::{BoxedExpression, Expression};
use crate::Result;

#[derive(Debug)]
pub struct SomeAllExpression {
    left_expr: BoxedExpression,
    right_expr: BoxedExpression,
    expr_type: Type,
    func: BoxedExpression,
}

impl SomeAllExpression {
    pub fn new(
        left_expr: BoxedExpression,
        right_expr: BoxedExpression,
        expr_type: Type,
        func: BoxedExpression,
    ) -> Self {
        SomeAllExpression {
            left_expr,
            right_expr,
            expr_type,
            func,
        }
    }

    fn resolve_scalar_vec(&self, scalar_vec: Vec<Option<ScalarImpl>>) -> Option<ScalarImpl> {
        let boolean_vec = scalar_vec
            .into_iter()
            .map(|scalar_ref| scalar_ref.map(|s| s.into_bool()))
            .collect::<Vec<_>>();
        match self.expr_type {
            Type::Some => {
                if boolean_vec.iter().any(|b| b.unwrap_or(false)) {
                    Some(true).map(|b| b.to_scalar_value())
                } else if boolean_vec.iter().any(|b| b.is_none()) {
                    None
                } else {
                    Some(false).map(|b| b.to_scalar_value())
                }
            }
            Type::All => {
                if boolean_vec.iter().all(|b| b.unwrap_or(false)) {
                    Some(true).map(|b| b.to_scalar_value())
                } else if boolean_vec.iter().any(|b| !b.unwrap_or(true)) {
                    Some(false).map(|b| b.to_scalar_value())
                } else {
                    None
                }
            }
            _ => unreachable!(),
        }
    }

    fn compare_left_right(
        &self,
        datum_left: Option<ScalarImpl>,
        datum_right: Option<ScalarImpl>,
    ) -> Result<Datum> {
        if let Some(array) = datum_right {
            match array {
                ScalarImpl::List(array) => {
                    let scalar_vec = array
                        .values()
                        .iter()
                        .map(|d| {
                            self.func
                                .eval_row(&Row::new(vec![datum_left.clone(), d.clone()]))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(self.resolve_scalar_vec(scalar_vec))
                }
                _ => unreachable!(),
            }
        } else {
            Ok(None)
        }
    }
}

impl Expression for SomeAllExpression {
    fn return_type(&self) -> DataType {
        DataType::Boolean
    }

    fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
        let arr_left = self.left_expr.eval_checked(data_chunk)?;
        let arr_right = self.right_expr.eval_checked(data_chunk)?;

        let bitmap = data_chunk.get_visibility_ref();
        let mut output_array = BoolArrayBuilder::new(data_chunk.capacity());
        match bitmap {
            Some(bitmap) => {
                for ((left, right), visible) in
                    multizip((arr_left.iter(), arr_right.iter())).zip_eq(bitmap.iter())
                {
                    if !visible {
                        output_array.append_null();
                        continue;
                    }

                    output_array.append(
                        self.compare_left_right(
                            left.map(|s| s.into_scalar_impl()),
                            right.map(|s| s.into_scalar_impl()),
                        )?
                        .map(|s| s.into_bool()),
                    );
                }
                Ok(Arc::new(output_array.finish().into()))
            }
            None => {
                for (left, right) in multizip((arr_left.iter(), arr_right.iter())) {
                    output_array.append(
                        self.compare_left_right(
                            left.map(|s| s.into_scalar_impl()),
                            right.map(|s| s.into_scalar_impl()),
                        )?
                        .map(|s| s.into_bool()),
                    );
                }
                Ok(Arc::new(output_array.finish().into()))
            }
        }
    }

    fn eval_row(&self, row: &Row) -> Result<Datum> {
        let datum_left = self.left_expr.eval_row(row)?;
        let datum_right = self.right_expr.eval_row(row)?;
        self.compare_left_right(datum_left, datum_right)
    }
}
