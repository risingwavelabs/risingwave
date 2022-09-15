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

use std::convert::TryFrom;
use std::sync::Arc;

use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, ArrayMeta, ArrayRef, DataChunk, ListArrayBuilder, ListValue, Row,
    StructArrayBuilder, StructValue,
};
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct NestedConstructExpression {
    data_type: DataType,
    elements: Vec<BoxedExpression>,
}

impl Expression for NestedConstructExpression {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let columns = self
            .elements
            .iter()
            .map(|e| e.eval_checked(input))
            .collect::<Result<Vec<_>>>()?;

        if let DataType::Struct(t) = &self.data_type {
            let mut builder = StructArrayBuilder::with_meta(
                input.capacity(),
                ArrayMeta::Struct {
                    children: t.fields.clone().into(),
                },
            );
            builder.append_array_refs(columns, input.capacity())?;
            Ok(Arc::new(ArrayImpl::Struct(builder.finish())))
        } else if let DataType::List { datatype } = &self.data_type {
            let columns = columns.into_iter().map(Column::new).collect();
            let chunk = DataChunk::new(columns, input.vis().clone());
            let mut builder = ListArrayBuilder::with_meta(
                input.capacity(),
                ArrayMeta::List {
                    datatype: datatype.clone(),
                },
            );
            chunk.rows_with_holes().try_for_each(|row| {
                if let Some(row) = row {
                    builder.append_row_ref(row)
                } else {
                    builder.append_null()
                }
            })?;
            Ok(Arc::new(ArrayImpl::List(builder.finish())))
        } else {
            Err(ExprError::UnsupportedFunction(
                "expects struct or list type".to_string(),
            ))
        }
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let datums = self
            .elements
            .iter()
            .map(|e| e.eval_row(input))
            .collect::<Result<Vec<Datum>>>()?;
        if let DataType::Struct { .. } = &self.data_type {
            Ok(Some(StructValue::new(datums).to_scalar_value()))
        } else if let DataType::List { datatype: _ } = &self.data_type {
            Ok(Some(ListValue::new(datums).to_scalar_value()))
        } else {
            Err(ExprError::UnsupportedFunction(
                "expects struct or list type".to_string(),
            ))
        }
    }
}

impl NestedConstructExpression {
    pub fn new(data_type: DataType, elements: Vec<BoxedExpression>) -> Self {
        NestedConstructExpression {
            data_type,
            elements,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for NestedConstructExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!([Type::Array, Type::Row].contains(&prost.get_expr_type().unwrap()));

        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };
        let elements = func_call_node
            .children
            .iter()
            .map(expr_build_from_prost)
            .collect::<Result<Vec<BoxedExpression>>>()?;
        Ok(NestedConstructExpression::new(ret_type, elements))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, ListValue, Row};
    use risingwave_common::types::{DataType, Scalar, ScalarImpl};

    use super::NestedConstructExpression;
    use crate::expr::{BoxedExpression, Expression, LiteralExpression};

    #[test]
    fn test_eval_array_expr() {
        let expr = NestedConstructExpression {
            data_type: DataType::List {
                datatype: DataType::Int32.into(),
            },
            elements: vec![i32_expr(1.into()), i32_expr(2.into())],
        };

        let arr = expr.eval(&DataChunk::new_dummy(2)).unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_eval_row_array_expr() {
        let expr = NestedConstructExpression {
            data_type: DataType::List {
                datatype: DataType::Int32.into(),
            },
            elements: vec![i32_expr(1.into()), i32_expr(2.into())],
        };

        let scalar_impl = expr.eval_row(&Row::new(vec![])).unwrap().unwrap();
        let expected = ListValue::new(vec![Some(1.into()), Some(2.into())]).to_scalar_value();
        assert_eq!(expected, scalar_impl);
    }

    fn i32_expr(v: ScalarImpl) -> BoxedExpression {
        Box::new(LiteralExpression::new(DataType::Int32, Some(v)))
    }
}
