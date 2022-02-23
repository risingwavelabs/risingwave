use std::convert::TryFrom;

use risingwave_pb::expr::expr_node::{RexNode, Type};
use risingwave_pb::expr::ExprNode;

use crate::array::{ArrayRef, DataChunk};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::Expression;
use crate::types::DataType;

/// `InputRefExpression` references to a column in input relation
#[derive(Debug)]
pub struct InputRefExpression {
    return_type: DataType,
    idx: usize,
}

impl Expression for InputRefExpression {
    fn return_type(&self) -> DataType {
        self.return_type
    }

    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        Ok(input.column_at(self.idx).array())
    }
}

impl InputRefExpression {
    pub fn new(return_type: DataType, idx: usize) -> Self {
        InputRefExpression { return_type, idx }
    }

    pub fn eval_immut(&self, input: &DataChunk) -> Result<ArrayRef> {
        Ok(input.column_at(self.idx).array())
    }
}

impl<'a> TryFrom<&'a ExprNode> for InputRefExpression {
    type Error = RwError;

    fn try_from(prost: &'a ExprNode) -> Result<Self> {
        ensure!(prost.get_expr_type()? == Type::InputRef);

        let ret_type = DataType::from(prost.get_return_type()?);
        if let RexNode::InputRef(input_ref_node) = prost.get_rex_node()? {
            Ok(Self {
                return_type: ret_type,
                idx: input_ref_node.column_idx as usize,
            })
        } else {
            Err(RwError::from(ErrorCode::NotImplementedError(
                "expects a input ref node".to_string(),
            )))
        }
    }
}
