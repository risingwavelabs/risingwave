use std::convert::TryFrom;
use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk};
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
        let sep = self.sep_expr.eval(input)?;
        let string_columns = self
            .string_exprs
            .iter()
            .map(|c| c.eval(input))
            .collect::<Result<Vec<_>>>()?;

        let mut builder = self.return_type.create_array_builder(input.cardinality())?;
        let len = string_columns[0].len(); // length of each column

        for i in 0..len { // iterates between each row
            let mut data = None;

            // for each item in a row
            for column in &string_columns {
                let datum = column.datum_at(i);

                if datum.is_some() {
                    data = datum;
                    break;
                }
            }
            builder.append_datum(&data)?;
        }
        Ok(Arc::new(builder.finish()?))
    }
}

impl ConcatWsExpression {
    pub fn new(return_type: DataType, sep_expr: BoxedExpression, string_exprs: Vec<BoxedExpression>) -> Self {
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
