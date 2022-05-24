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
    ArrayBuilder, ArrayImpl, ArrayMeta, ArrayRef, DataChunk, ListArrayBuilder,
};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::{ensure, try_match_expand};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression, ArrayExpression};
use risingwave_common::types::Datum;

#[derive(Debug)]
pub struct ArrayAccessExpression {
    array: BoxedExpression,//TODO(nanderstabel) change to ArrayExpression
    indexs: Vec<usize>,
}

impl Expression for ArrayAccessExpression {
    fn return_type(&self) -> DataType {

        //TODO(nanderstabel) account for more nested layers
        match self.array.return_type() {
            DataType::List { datatype } => *datatype,
            _ => unreachable!()
        }
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {

        let array = self.array.eval(input)?;

        let mut builder = self.return_type().create_array_builder(input.cardinality())?;

        let len = array.len();
        for i in 0..len {
            if let ScalarImpl::List(value) = array.datum_at(i).unwrap() {
                let datum = value.values()[self.indexs[0]].clone();
                builder.append_datum(&datum)?;
            }
        }
        
        builder.finish().map(|a| Arc::new(a))
    }
}

impl ArrayAccessExpression {
    pub fn new(array: BoxedExpression, indexs: Vec<usize>) -> Self {
        ArrayAccessExpression {
            array,
            indexs
        }
    }
}
use crate::expr::LiteralExpression;
impl<'a> TryFrom<&'a ExprNode> for ArrayAccessExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::ArrayAccess);

        let func_call_node = try_match_expand!(prost.get_rex_node().unwrap(), RexNode::FuncCall)?;
        let mut children = func_call_node.children.iter();

        if let Some(node) = children.next() {
            // First child node must be an Array Type.
            ensure!(node.get_expr_type()? == Type::Array);
            let array = expr_build_from_prost(node)?;

            //TODO(nanderstabel): fix potential unwrap() on Err + refactor
            let indexs = children
                .map(|e| LiteralExpression::try_from(e).unwrap().literal())
                .collect::<Vec<Datum>>();
            let indexs = indexs
                .iter()
                .map(|i| usize::try_from(i.clone().unwrap().into_int32()).unwrap())
                .collect();
                
            return Ok(ArrayAccessExpression::new(array, indexs))
        }
        // TODO(nanderstabel): refactor
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::DataChunk;
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::ArrayAccessExpression;
    use crate::expr::{BoxedExpression, Expression, LiteralExpression, ArrayExpression};

    #[test]
    fn test_eval_array_access_expr() {
        use risingwave_common::array::I32Array;
        use risingwave_common::array;

        let array_expr = ArrayExpression::new(
            DataType::List { datatype: Box::new(DataType::Int32) }, 
            vec![i32_expr(11.into()), i32_expr(22.into())]
        );

        let expr = ArrayAccessExpression {
            array: Box::new(array_expr),
            indexs: vec![0]
        };

        let arr = expr.eval(&DataChunk::new_dummy(3)).unwrap();
        assert_eq!(*arr, array! { I32Array, [Some(11), Some(11), Some(11)] }.into());
    }

    fn i32_expr(v: ScalarImpl) -> BoxedExpression {
        Box::new(LiteralExpression::new(DataType::Int32, Some(v)))
    }
}
