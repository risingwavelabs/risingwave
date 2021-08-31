use crate::array::{ArrayRef, DataChunk};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::expr::build_from_proto_option;
use crate::expr::BoxedExpression;
use crate::expr::Expression;
use crate::types::{build_from_proto as type_build_from_proto, is_arithmetic_operator};
use crate::types::{ArithmeticOperatorKind, DataType, DataTypeRef};
use crate::vector_op::vec_arithmetic;
use protobuf::Message;
use risingwave_proto::expr::ExprNode;
use risingwave_proto::expr::FunctionCall;
use std::convert::TryFrom;

pub(super) struct ArithmeticExpression {
    return_type: DataTypeRef,
    operator_type: ArithmeticOperatorKind,
    left_child: BoxedExpression,
    right_child: BoxedExpression,
}

impl Expression for ArithmeticExpression {
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.left_child.return_type_ref().clone()
    }

    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        let left_array = self.left_child.eval(input)?;
        let right_array = self.right_child.eval(input)?;
        vec_arithmetic::vector_arithmetic_impl(self.operator_type, left_array, right_array)
    }
}

impl ArithmeticExpression {
    pub(crate) fn new(
        return_type: DataTypeRef,
        operator_type: ArithmeticOperatorKind,
        left_child: BoxedExpression,
        right_child: BoxedExpression,
    ) -> Self {
        ArithmeticExpression {
            return_type,
            operator_type,
            left_child,
            right_child,
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for ArithmeticExpression {
    type Error = RwError;

    fn try_from(proto: &'a ExprNode) -> Result<Self> {
        let expr_type = &proto.get_expr_type();
        ensure!(is_arithmetic_operator(expr_type));
        let data_type = type_build_from_proto(proto.get_return_type())?;
        let operator_type = ArithmeticOperatorKind::try_from(expr_type);

        let proto_content =
            FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
        ensure!(proto_content.get_children().len() == 2);
        let left_child_proto = proto_content.get_children().get(0);
        let right_child_proto = proto_content.get_children().get(1);

        let left_expr = build_from_proto_option(left_child_proto)?;
        let right_expr = build_from_proto_option(right_child_proto)?;

        match operator_type {
            Ok(o_type) => Ok(Self {
                return_type: data_type,
                operator_type: o_type,
                left_child: left_expr,
                right_child: right_expr,
            }),
            _ => Err(InternalError("Not arithmetic expression.".to_string()).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{DataChunk, PrimitiveArray};
    use crate::error::Result;
    use crate::expr::input_ref::InputRefExpression;
    use crate::types::Int32Type;
    use crate::util::downcast_ref;
    use std::sync::Arc;

    #[test]
    fn test_arithmetic_expr_eval() -> Result<()> {
        // Let this test succeed.
        let left_type = Arc::new(Int32Type::new(false));
        let left_expr = InputRefExpression::new(left_type, 0);
        let right_type = Arc::new(Int32Type::new(false));
        let right_expr = InputRefExpression::new(right_type, 1);
        let mut test_expr = ArithmeticExpression {
            return_type: Arc::new(Int32Type::new(false)),
            operator_type: ArithmeticOperatorKind::Plus,
            left_child: Box::new(left_expr),
            right_child: Box::new(right_expr),
        };

        let arr_1 = PrimitiveArray::<Int32Type>::from_slice(vec![1, 2, 33333, 4, 5])?;
        let arr_2 = PrimitiveArray::<Int32Type>::from_slice(vec![7, 8, 66666, 4, 3])?;
        let expect_values = vec![8, 10, 99999, 8, 8];

        assert_eq!(arr_1.len(), 5);
        assert_eq!(arr_2.len(), 5);

        let arrays = vec![arr_1, arr_2];

        let chunk = DataChunk::builder().cardinality(5).arrays(arrays).build();

        let result_arr = test_expr.eval(&chunk)?;

        assert_eq!(result_arr.len(), 5);
        let result_ref: &PrimitiveArray<Int32Type> = downcast_ref(&*result_arr)?;
        let result_slice = result_ref.as_slice();
        for _i in 0..5 {
            assert_eq!(result_slice[_i], expect_values[_i]);
        }
        Ok(())
    }
}
