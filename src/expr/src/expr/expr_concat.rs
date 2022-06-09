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

use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, Row, Utf8ArrayBuilder,
};
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct ConcatExpression {
    string_exprs: Vec<BoxedExpression>,
}

impl Expression for ConcatExpression {
    fn return_type(&self) -> DataType {
        DataType::Varchar
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let string_columns = self
            .string_exprs
            .iter()
            .map(|c| c.eval(input))
            .collect::<Result<Vec<_>>>()?;
        let string_columns_ref = string_columns
            .iter()
            .map(|c| c.as_utf8())
            .collect::<Vec<_>>();

        let row_len = input.cardinality();
        let mut builder = Utf8ArrayBuilder::new(row_len).map_err(ExprError::Array)?;

        for row_idx in 0..row_len {
            let mut row_string_builder = builder.writer().begin();
            for string_column in &string_columns_ref {
                if let Some(string) = string_column.value_at(row_idx) {
                    row_string_builder
                        .write_ref(string)
                        .map_err(ExprError::Array)?;
                }
            }

            builder = row_string_builder
                .finish()
                .map_err(ExprError::Array)?
                .into_inner();
        }
        Ok(Arc::new(ArrayImpl::from(
            builder.finish().map_err(ExprError::Array)?,
        )))
    }

    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let strings = self
            .string_exprs
            .iter()
            .map(|c| c.eval_row(input))
            .collect::<Result<Vec<_>>>()?;
        let string = strings
            .into_iter()
            .flatten()
            .map(|s| s.into_utf8())
            .collect::<String>();

        Ok(Some(string.to_scalar_value()))
    }
}

impl ConcatExpression {
    pub fn new(string_exprs: Vec<BoxedExpression>) -> Self {
        ConcatExpression { string_exprs }
    }
}

impl<'a> TryFrom<&'a ExprNode> for ConcatExpression {
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type().unwrap() == Type::Concat);

        let ret_type = DataType::from(prost.get_return_type().unwrap());
        assert_eq!(ret_type, DataType::Varchar);

        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };

        let children = &func_call_node.children;
        let string_exprs = children
            .iter()
            .map(expr_build_from_prost)
            .collect::<Result<Vec<_>>>()?;
        Ok(ConcatExpression::new(string_exprs))
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::{DataChunk, DataChunkTestExt, Row};
    use risingwave_common::types::{Scalar, ToOwnedDatum};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::expr_node::RexNode;
    use risingwave_pb::expr::expr_node::Type::Concat;
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use crate::expr::expr_concat::ConcatExpression;
    use crate::expr::test_utils::make_input_ref;
    use crate::expr::Expression;

    pub fn make_concat_function(children: Vec<ExprNode>) -> ExprNode {
        ExprNode {
            expr_type: Concat as i32,
            return_type: Some(ProstDataType {
                type_name: TypeName::Varchar as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
        }
    }

    #[test]
    fn test_eval_concat_expr() {
        let input_node1 = make_input_ref(0, TypeName::Varchar);
        let input_node2 = make_input_ref(1, TypeName::Varchar);
        let input_node3 = make_input_ref(2, TypeName::Varchar);
        let concat_expr = ConcatExpression::try_from(&make_concat_function(vec![
            input_node1,
            input_node2,
            input_node3,
        ]))
        .unwrap();

        let chunk = DataChunk::from_pretty(
            "
            T T T
            a 1 2.01
            . 1 2.01
            . . 2.01
            . . .",
        );

        let actual = concat_expr.eval(&chunk).unwrap();
        let actual = actual
            .iter()
            .map(|r| r.map(|s| s.into_utf8()))
            .collect_vec();

        let expected = vec![Some("a12.01"), Some("12.01"), Some("2.01"), Some("")];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_eval_row_concat_expr() {
        let input_node1 = make_input_ref(0, TypeName::Varchar);
        let input_node2 = make_input_ref(1, TypeName::Varchar);
        let input_node3 = make_input_ref(2, TypeName::Varchar);
        let concat_expr = ConcatExpression::try_from(&make_concat_function(vec![
            input_node1,
            input_node2,
            input_node3,
        ]))
        .unwrap();

        let chunk = DataChunk::from_pretty(
            "
            T T T
            a 1 2.01
            . 1 2.01
            . . 2.01
            . . .",
        );
        let row_inputs = chunk
            .rows()
            .map(|r| Row::new(r.values().map(|d| d.to_owned_datum()).collect_vec()))
            .collect_vec();

        let expected = vec![Some("a12.01"), Some("12.01"), Some("2.01"), Some("")];

        for (i, row) in row_inputs.iter().enumerate() {
            let actual = concat_expr.eval_row(row).unwrap();
            let expected = expected[i].map(|s| s.to_string().to_scalar_value());

            assert_eq!(actual, expected);
        }
    }
}
