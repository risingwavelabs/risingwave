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
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, Utf8ArrayBuilder,
};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::{ensure, try_match_expand};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};

#[derive(Debug)]
pub struct ConcatWsExpression {
    return_type: DataType,
    sep_expr: BoxedExpression,
    string_exprs: Vec<BoxedExpression>,
}

impl Expression for ConcatWsExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let sep_column = self.sep_expr.eval(input)?;
        let sep_column = sep_column.as_utf8();

        let string_columns = self
            .string_exprs
            .iter()
            .map(|c| c.eval(input))
            .collect::<Result<Vec<_>>>()?;

        let mut builder = Utf8ArrayBuilder::new(input.cardinality())?;
        let row_len = string_columns[0].len();

        for row_idx in 0..row_len {
            let sep = sep_column.value_at(row_idx);
            if sep.is_none() {
                builder.append(None)?;
                continue;
            }
            let sep = sep.unwrap();

            let mut res = String::from("");
            let mut found_first = false;

            for string_column in &string_columns {
                let string_column = string_column.as_utf8();
                if let Some(string) = string_column.value_at(row_idx) {
                    if !found_first {
                        found_first = true;
                        res.push_str(string);
                    } else {
                        res.push_str(sep);
                        res.push_str(string);
                    }
                }
            }
            builder.append(Some(&res))?;
        }
        Ok(Arc::new(ArrayImpl::from(builder.finish()?)))
    }
}

impl ConcatWsExpression {
    pub fn new(
        return_type: DataType,
        sep_expr: BoxedExpression,
        string_exprs: Vec<BoxedExpression>,
    ) -> Self {
        ConcatWsExpression {
            return_type,
            sep_expr,
            string_exprs,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for ConcatWsExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::ConcatWs);

        let ret_type = DataType::from(prost.get_return_type()?);
        let func_call_node = try_match_expand!(prost.get_rex_node().unwrap(), RexNode::FuncCall)?;

        let children = &func_call_node.children;
        let sep_expr = expr_build_from_prost(&children[0])?;

        let string_exprs = children[1..]
            .iter()
            .map(expr_build_from_prost)
            .collect::<Result<Vec<_>>>()?;
        Ok(ConcatWsExpression::new(ret_type, sep_expr, string_exprs))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{DataChunk, Utf8Array};
    use risingwave_common::types::ScalarImpl;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::expr_node::RexNode;
    use risingwave_pb::expr::expr_node::Type::ConcatWs;
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use crate::expr::expr_concat_ws::ConcatWsExpression;
    use crate::expr::test_utils::make_input_ref;
    use crate::expr::Expression;

    pub fn make_concat_ws_function(children: Vec<ExprNode>, ret: TypeName) -> ExprNode {
        ExprNode {
            expr_type: ConcatWs as i32,
            return_type: Some(ProstDataType {
                type_name: ret as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
        }
    }

    #[test]
    fn test_eval_concat_ws_expr() {
        let input_node1 = make_input_ref(0, TypeName::Varchar);
        let input_node2 = make_input_ref(1, TypeName::Varchar);
        let input_node3 = make_input_ref(2, TypeName::Varchar);

        let array = Utf8Array::from_slice(&[Some(","), None, Some(","), Some(",")])
            .map(|x| Arc::new(x.into()))
            .unwrap();
        let col1 = Column::new(array);
        let array = Utf8Array::from_slice(&[Some("a"), Some("a"), None, None])
            .map(|x| Arc::new(x.into()))
            .unwrap();
        let col2 = Column::new(array);
        let array = Utf8Array::from_slice(&[Some("b"), Some("b"), Some("a"), None])
            .map(|x| Arc::new(x.into()))
            .unwrap();
        let col3 = Column::new(array);

        let data_chunk = DataChunk::builder().columns(vec![col1, col2, col3]).build();

        let nullif_expr = ConcatWsExpression::try_from(&make_concat_ws_function(
            vec![input_node1, input_node2, input_node3],
            TypeName::Varchar,
        ))
        .unwrap();
        let res = nullif_expr.eval(&data_chunk).unwrap();
        assert_eq!(res.datum_at(0), Some(ScalarImpl::Utf8("a,b".to_string())));
        assert_eq!(res.datum_at(1), None);
        assert_eq!(res.datum_at(2), Some(ScalarImpl::Utf8("a".to_string())));
        assert_eq!(res.datum_at(3), Some(ScalarImpl::Utf8("".to_string())));
    }
}
