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
use std::fmt::Write;
use std::sync::Arc;

use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, Utf8ArrayBuilder,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost as expr_build_from_prost, BoxedExpression, Expression};
use crate::{bail, ensure, ExprError, Result};

#[derive(Debug)]
pub struct ConcatWsExpression {
    return_type: DataType,
    sep_expr: BoxedExpression,
    string_exprs: Vec<BoxedExpression>,
}

#[async_trait::async_trait]
impl Expression for ConcatWsExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let sep_column = self.sep_expr.eval_checked(input).await?;
        let sep_column = sep_column.as_utf8();

        let mut string_columns = Vec::with_capacity(self.string_exprs.len());
        for expr in &self.string_exprs {
            string_columns.push(expr.eval_checked(input).await?);
        }
        let string_columns_ref = string_columns
            .iter()
            .map(|c| c.as_utf8())
            .collect::<Vec<_>>();

        let row_len = input.capacity();
        let vis = input.vis();
        let mut builder = Utf8ArrayBuilder::new(row_len);

        for row_idx in 0..row_len {
            if !vis.is_set(row_idx) {
                builder.append(None);
                continue;
            }
            let sep = match sep_column.value_at(row_idx) {
                Some(sep) => sep,
                None => {
                    builder.append(None);
                    continue;
                }
            };

            let mut writer = builder.writer().begin();

            let mut string_columns = string_columns_ref.iter();
            for string_column in string_columns.by_ref() {
                if let Some(string) = string_column.value_at(row_idx) {
                    writer.write_str(string).unwrap();
                    break;
                }
            }

            for string_column in string_columns {
                if let Some(string) = string_column.value_at(row_idx) {
                    writer.write_str(sep).unwrap();
                    writer.write_str(string).unwrap();
                }
            }

            writer.finish();
        }
        Ok(Arc::new(ArrayImpl::from(builder.finish())))
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        let sep = self.sep_expr.eval_row(input).await?;
        let sep = match sep {
            Some(sep) => sep,
            None => return Ok(None),
        };

        let mut strings = Vec::with_capacity(self.string_exprs.len());
        for expr in &self.string_exprs {
            strings.push(expr.eval_row(input).await?);
        }
        let mut final_string = String::new();

        let mut strings_iter = strings.iter();
        if let Some(string) = strings_iter.by_ref().flatten().next() {
            final_string.push_str(string.as_utf8())
        }

        for string in strings_iter.flatten() {
            final_string.push_str(sep.as_utf8());
            final_string.push_str(string.as_utf8());
        }

        Ok(Some(final_string.into()))
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
    type Error = ExprError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_function_type().unwrap() == Type::ConcatWs);

        let ret_type = DataType::from(prost.get_return_type().unwrap());
        let RexNode::FuncCall(func_call_node) = prost.get_rex_node().unwrap() else {
            bail!("Expected RexNode::FuncCall");
        };

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
    use itertools::Itertools;
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::Datum;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::PbDataType;
    use risingwave_pb::expr::expr_node::RexNode;
    use risingwave_pb::expr::expr_node::Type::ConcatWs;
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use crate::expr::expr_concat_ws::ConcatWsExpression;
    use crate::expr::test_utils::make_input_ref;
    use crate::expr::Expression;

    pub fn make_concat_ws_function(children: Vec<ExprNode>, ret: TypeName) -> ExprNode {
        ExprNode {
            function_type: ConcatWs as i32,
            return_type: Some(PbDataType {
                type_name: ret as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall { children })),
        }
    }

    #[tokio::test]
    async fn test_eval_concat_ws_expr() {
        let input_node1 = make_input_ref(0, TypeName::Varchar);
        let input_node2 = make_input_ref(1, TypeName::Varchar);
        let input_node3 = make_input_ref(2, TypeName::Varchar);
        let input_node4 = make_input_ref(3, TypeName::Varchar);
        let concat_ws_expr = ConcatWsExpression::try_from(&make_concat_ws_function(
            vec![input_node1, input_node2, input_node3, input_node4],
            TypeName::Varchar,
        ))
        .unwrap();

        let chunk = DataChunk::from_pretty(
            "
            T T T T
            , a b c
            . a b c
            , . b c
            , . . .
            . . . .",
        );

        let actual = concat_ws_expr.eval(&chunk).await.unwrap();
        let actual = actual
            .iter()
            .map(|r| r.map(|s| s.into_utf8()))
            .collect_vec();

        let expected = vec![Some("a,b,c"), None, Some("b,c"), Some(""), None];

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn test_eval_row_concat_ws_expr() {
        let input_node1 = make_input_ref(0, TypeName::Varchar);
        let input_node2 = make_input_ref(1, TypeName::Varchar);
        let input_node3 = make_input_ref(2, TypeName::Varchar);
        let input_node4 = make_input_ref(3, TypeName::Varchar);
        let concat_ws_expr = ConcatWsExpression::try_from(&make_concat_ws_function(
            vec![input_node1, input_node2, input_node3, input_node4],
            TypeName::Varchar,
        ))
        .unwrap();

        let row_inputs = vec![
            vec![Some(","), Some("a"), Some("b"), Some("c")],
            vec![None, Some("a"), Some("b"), Some("c")],
            vec![Some(","), None, Some("b"), Some("c")],
            vec![Some(","), None, None, None],
            vec![None, None, None, None],
        ];

        let expected = vec![Some("a,b,c"), None, Some("b,c"), Some(""), None];

        for (i, row_input) in row_inputs.iter().enumerate() {
            let datum_vec: Vec<Datum> = row_input.iter().map(|e| e.map(|s| s.into())).collect();
            let row = OwnedRow::new(datum_vec);

            let result = concat_ws_expr.eval_row(&row).await.unwrap();
            let expected = expected[i].map(|s| s.into());

            assert_eq!(result, expected);
        }
    }
}
