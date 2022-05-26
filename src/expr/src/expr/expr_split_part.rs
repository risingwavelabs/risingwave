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

use risingwave_common::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk, Utf8ArrayBuilder,
};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::{ensure, try_match_expand};
use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::expr::{build_from_prost, BoxedExpression, Expression};

#[derive(Debug)]
pub struct SplitPartExpression {
    return_type: DataType,
    string_expr: BoxedExpression,
    delimiter_expr: BoxedExpression,
    nth_expr: BoxedExpression,
}

impl Expression for SplitPartExpression {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let string_columns = self.string_expr.eval(input)?;
        let string_columns = string_columns.as_utf8();

        let delimiter_columns = self.delimiter_expr.eval(input)?;
        let delimiter_columns = delimiter_columns.as_utf8();

        let nth_columns = self.nth_expr.eval(input)?;
        let nth_columns = nth_columns.as_int64();

        let row_len = input.cardinality();
        let mut builder = Utf8ArrayBuilder::new(row_len)?;

        for i in 0..row_len {
            let string_column = string_columns.value_at(i);
            let delimiter_column = delimiter_columns.value_at(i);
            let nth_column = nth_columns.value_at(i);

            // FIXME
            let string_column = string_column.unwrap();
            let delimiter_column = delimiter_column.unwrap();
            let nth_column = nth_column.unwrap();

            let mut split = string_column.split(delimiter_column);
            let nth_value = match nth_column.cmp(&0) {
                std::cmp::Ordering::Equal => None,
                std::cmp::Ordering::Greater => split.nth(nth_column as usize - 1),
                std::cmp::Ordering::Less => {
                    let split = split.collect::<Vec<_>>();
                    let nth_column = (split.len() as i64 + nth_column) as usize;
                    split.get(nth_column).map(<&str>::clone)
                }
            };
            builder.append(nth_value)?;
        }

        Ok(Arc::new(ArrayImpl::from(builder.finish()?)))
    }
}

impl SplitPartExpression {
    pub fn new(
        return_type: DataType,
        string_expr: BoxedExpression,
        delimiter_expr: BoxedExpression,
        nth_expr: BoxedExpression,
    ) -> Self {
        SplitPartExpression {
            return_type,
            string_expr,
            delimiter_expr,
            nth_expr,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for SplitPartExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::SplitPart);

        let return_type = DataType::from(prost.get_return_type()?);
        let func_call_node = try_match_expand!(prost.get_rex_node().unwrap(), RexNode::FuncCall)?;

        let children = &func_call_node.children;
        let string_expr = build_from_prost(&children[0])?;
        let delimiter_expr = build_from_prost(&children[1])?;
        let nth_expr = build_from_prost(&children[2])?;

        Ok(SplitPartExpression::new(
            return_type,
            string_expr,
            delimiter_expr,
            nth_expr,
        ))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::RexNode;
    use risingwave_pb::expr::expr_node::Type::SplitPart;
    use risingwave_pb::expr::{ExprNode, FunctionCall};

    use crate::expr::expr_split_part::SplitPartExpression;
    use crate::expr::test_utils::make_input_ref;
    use crate::expr::Expression;

    #[test]
    fn test_eval_split_part_expr() {
        let chunk = DataChunk::from_pretty(
            "
            T                T    I
            abc~@~def~@~ghi  ~@~  2
            abc,def,ghi,jkl  ,    -2",
        );

        let in_n0 = make_input_ref(0, TypeName::Varchar);
        let in_n1 = make_input_ref(1, TypeName::Varchar);
        let in_n2 = make_input_ref(2, TypeName::Int64);

        let expr_node = ExprNode {
            expr_type: SplitPart as i32,
            return_type: Some(DataType {
                type_name: TypeName::Varchar as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::FuncCall(FunctionCall {
                children: vec![in_n0, in_n1, in_n2],
            })),
        };
        let split_part_expr = SplitPartExpression::try_from(&expr_node).unwrap();

        let actual = split_part_expr.eval(&chunk).unwrap();
        let actual = actual
            .iter()
            .map(|xs| xs.map(|x| x.into_utf8()))
            .collect::<Vec<_>>();

        let expected = vec![Some("def"), Some("ghi")];

        assert_eq!(expected, actual);
    }
}
