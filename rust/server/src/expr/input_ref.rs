use std::convert::TryFrom;

use protobuf::Message;

use risingwave_proto::expr::{ExprNode, ExprNode_ExprNodeType, InputRefExpr};

use crate::array2::{ArrayRef, DataChunk};
use crate::error::ErrorCode::ProtobufError;
use crate::error::{Result, RwError};
use crate::expr::Expression;
use crate::types::{build_from_proto, DataType, DataTypeRef};

pub(super) struct InputRefExpression {
    return_type: DataTypeRef,
    idx: usize,
}

impl Expression for InputRefExpression {
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        Ok(input.column_at(self.idx)?.array)
    }
}

impl InputRefExpression {
    pub fn new(return_type: DataTypeRef, idx: usize) -> Self {
        InputRefExpression { return_type, idx }
    }
}

impl<'a> TryFrom<&'a ExprNode> for InputRefExpression {
    type Error = RwError;

    fn try_from(proto: &'a ExprNode) -> Result<Self> {
        ensure!(proto.get_expr_type() == ExprNode_ExprNodeType::INPUT_REF);

        let data_type = build_from_proto(proto.get_return_type())?;

        let input_ref_node =
            InputRefExpr::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
        Ok(Self {
            return_type: data_type,
            idx: input_ref_node.column_idx as usize,
        })
    }
}
