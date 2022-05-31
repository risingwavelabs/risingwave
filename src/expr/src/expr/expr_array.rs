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
};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_common::{ensure, try_match_expand};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};

#[derive(Debug)]
pub struct ArrayExpression {
    element_type: Box<DataType>,
    elements: Vec<BoxedExpression>,
}

impl Expression for ArrayExpression {
    fn return_type(&self) -> DataType {
        DataType::List {
            datatype: self.element_type.clone(),
        }
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let columns = self
            .elements
            .iter()
            .map(|e| {
                let array = e.eval(input)?;
                Ok(Column::new(array))
            })
            .collect::<Result<Vec<Column>>>()?;
        let chunk = DataChunk::new(columns, input.visibility().cloned());

        let mut builder = ListArrayBuilder::with_meta(
            input.capacity(),
            ArrayMeta::List {
                datatype: self.element_type.clone(),
            },
        )?;
        chunk
            .rows()
            .try_for_each(|row| builder.append_row_ref(row))?;
        builder.finish().map(|a| Arc::new(ArrayImpl::List(a)))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let datums = self
            .elements
            .iter()
            .map(|e| e.eval_row(input))
            .collect::<Result<Vec<Datum>>>()?;
        Ok(Some(ListValue::new(datums).to_scalar_value()))
    }
}

impl ArrayExpression {
    pub fn new(ret_type: DataType, elements: Vec<BoxedExpression>) -> Self {
        let element_type = match ret_type {
            DataType::List { datatype } => datatype,
            _ => unreachable!(),
        };
        ArrayExpression {
            element_type,
            elements,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for ArrayExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::Array);

        let ret_type = DataType::from(prost.get_return_type()?);
        let func_call_node = try_match_expand!(prost.get_rex_node().unwrap(), RexNode::FuncCall)?;
        let elements = func_call_node
            .children
            .iter()
            .map(expr_build_from_prost)
            .collect::<Result<Vec<BoxedExpression>>>()?;
        Ok(ArrayExpression::new(ret_type, elements))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{DataChunk, ListValue, Row};
    use risingwave_common::types::{DataType, Scalar, ScalarImpl};

    use super::ArrayExpression;
    use crate::expr::{BoxedExpression, Expression, LiteralExpression};

    #[test]
    fn test_eval_array_expr() {
        let expr = ArrayExpression {
            element_type: Box::new(DataType::Int32),
            elements: vec![i32_expr(1.into()), i32_expr(2.into())],
        };

        let arr = expr.eval(&DataChunk::new_dummy(2)).unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn test_eval_row_array_expr() {
        let expr = ArrayExpression {
            element_type: Box::new(DataType::Int32),
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
