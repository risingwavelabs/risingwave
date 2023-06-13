// Copyright 2023 RisingWave Labs
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

use std::convert::TryFrom;
use std::sync::Arc;

use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, ListArrayBuilder, ListValue, StructArray,
    StructValue,
};
use risingwave_common::row::OwnedRow;
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

#[async_trait::async_trait]
impl Expression for NestedConstructExpression {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let mut columns = Vec::with_capacity(self.elements.len());
        for e in &self.elements {
            columns.push(e.eval_checked(input).await?);
        }

        if let DataType::Struct(ty) = &self.data_type {
            let array = StructArray::new(ty.clone(), columns, input.vis().to_bitmap());
            Ok(Arc::new(ArrayImpl::Struct(array)))
        } else if let DataType::List { .. } = &self.data_type {
            let chunk = DataChunk::new(columns, input.vis().clone());
            let mut builder = ListArrayBuilder::with_type(input.capacity(), self.data_type.clone());
            for row in chunk.rows_with_holes() {
                if let Some(row) = row {
                    builder.append_row_ref(row);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(ArrayImpl::List(builder.finish())))
        } else {
            Err(ExprError::UnsupportedFunction(
                "expects struct or list type".to_string(),
            ))
        }
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let mut datums = Vec::with_capacity(self.elements.len());
        for e in &self.elements {
            datums.push(e.eval_row(input).await?);
        }
        if let DataType::Struct { .. } = &self.data_type {
            Ok(Some(StructValue::new(datums).to_scalar_value()))
        } else if let DataType::List(_) = &self.data_type {
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
        ensure!([Type::Array, Type::Row].contains(&prost.get_function_type().unwrap()));

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
    use risingwave_common::array::{DataChunk, ListValue};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, Scalar, ScalarImpl};

    use super::NestedConstructExpression;
    use crate::expr::{BoxedExpression, Expression, LiteralExpression};

    #[tokio::test]
    async fn test_eval_array_expr() {
        let expr = NestedConstructExpression {
            data_type: DataType::List(DataType::Int32.into()),
            elements: vec![i32_expr(1.into()), i32_expr(2.into())],
        };

        let arr = expr.eval(&DataChunk::new_dummy(2)).await.unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[tokio::test]
    async fn test_eval_row_array_expr() {
        let expr = NestedConstructExpression {
            data_type: DataType::List(DataType::Int32.into()),
            elements: vec![i32_expr(1.into()), i32_expr(2.into())],
        };

        let scalar_impl = expr
            .eval_row(&OwnedRow::new(vec![]))
            .await
            .unwrap()
            .unwrap();
        let expected = ListValue::new(vec![Some(1.into()), Some(2.into())]).to_scalar_value();
        assert_eq!(expected, scalar_impl);
    }

    fn i32_expr(v: ScalarImpl) -> BoxedExpression {
        Box::new(LiteralExpression::new(DataType::Int32, Some(v)))
    }
}
