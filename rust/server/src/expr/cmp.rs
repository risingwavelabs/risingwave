use std::convert::TryFrom;
use std::sync::Arc;

use protobuf::Message;

use risingwave_proto::expr::{ExprNode, FunctionCall};

use crate::array2::ArrayRef;
use crate::array2::DataChunk;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::expr::build_from_proto as expr_build_from_proto;
use crate::expr::BoxedExpression;
use crate::expr::Expression;
use crate::types::{build_from_proto as type_build_from_proto, DataType, DataTypeRef};
use crate::vector_op::cmp::{vec_eq, vec_geq, vec_gt, vec_leq, vec_lt, vec_neq};
use risingwave_proto::expr::ExprNode_ExprNodeType::{
    EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUAL,
};

pub enum CompareOperatorKind {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}
pub(super) struct CompareExpression {
    return_type: DataTypeRef,
    kind: CompareOperatorKind,
    lhs: BoxedExpression,
    rhs: BoxedExpression,
}

impl Expression for CompareExpression {
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        let lhs = self.lhs.eval(input)?;
        let rhs = self.rhs.eval(input)?;
        let res = match self.kind {
            CompareOperatorKind::Equal => vec_eq(lhs.as_ref(), rhs.as_ref()),
            CompareOperatorKind::NotEqual => vec_neq(lhs.as_ref(), rhs.as_ref()),
            CompareOperatorKind::GreaterThan => vec_gt(lhs.as_ref(), rhs.as_ref()),
            CompareOperatorKind::GreaterThanOrEqual => vec_geq(lhs.as_ref(), rhs.as_ref()),
            CompareOperatorKind::LessThan => vec_lt(lhs.as_ref(), rhs.as_ref()),
            CompareOperatorKind::LessThanOrEqual => vec_leq(lhs.as_ref(), rhs.as_ref()),
        };
        res.map(|x| Arc::new(x.into()))
    }
}

impl<'a> TryFrom<&'a ExprNode> for CompareExpression {
    type Error = RwError;

    fn try_from(proto: &'a ExprNode) -> Result<Self> {
        let function_call_node =
            FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
        ensure!(
            function_call_node.get_children().len() == 2,
            "cmp expression mast have exactly two child"
        );
        // we have checked that it has 2 children
        let lhs = expr_build_from_proto(function_call_node.get_children().get(0).unwrap())?;
        let rhs = expr_build_from_proto(function_call_node.get_children().get(1).unwrap())?;
        let kind = match proto.get_expr_type() {
            EQUAL => CompareOperatorKind::Equal,
            NOT_EQUAL => CompareOperatorKind::NotEqual,
            GREATER_THAN => CompareOperatorKind::GreaterThan,
            GREATER_THAN_OR_EQUAL => CompareOperatorKind::GreaterThanOrEqual,
            LESS_THAN => CompareOperatorKind::LessThan,
            LESS_THAN_OR_EQUAL => CompareOperatorKind::LessThanOrEqual,
            _ => return Err(InternalError("unsupported compare operator".to_string()).into()),
        };
        let return_type = type_build_from_proto(proto.get_return_type())?;
        Ok(Self {
            return_type,
            kind,
            rhs,
            lhs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::column::Column;
    use crate::array2::Array;
    use crate::array2::{ArrayImpl::Bool, PrimitiveArray};
    use crate::types::Int32Type;
    use pb_construct::make_proto;
    use protobuf::well_known_types::Any as AnyProto;
    use protobuf::RepeatedField;
    use risingwave_proto::data::DataType as DataTypeProto;
    use risingwave_proto::expr::ExprNode_ExprNodeType;
    use risingwave_proto::expr::ExprNode_ExprNodeType::INPUT_REF;
    use risingwave_proto::expr::InputRefExpr;
    #[test]
    fn test_execute() {
        mock_execute(EQUAL, Box::new(|x, y| x == y));
        mock_execute(NOT_EQUAL, Box::new(|x, y| x != y));
        mock_execute(LESS_THAN, Box::new(|x, y| x < y));
        mock_execute(LESS_THAN_OR_EQUAL, Box::new(|x, y| x <= y));
        mock_execute(GREATER_THAN, Box::new(|x, y| x > y));
        mock_execute(GREATER_THAN_OR_EQUAL, Box::new(|x, y| x >= y));
    }

    fn mock_execute(kind: ExprNode_ExprNodeType, mut f: Box<dyn FnMut(i32, i32) -> bool>) {
        let mut lhs = Vec::<Option<i32>>::new();
        let mut rhs = Vec::<Option<i32>>::new();
        let mut target = Vec::<Option<bool>>::new();
        for i in 0..100 {
            if i % 2 == 0 {
                lhs.push(Some(i));
                rhs.push(None);
                target.push(None);
            } else if i % 3 == 0 {
                lhs.push(Some(i));
                rhs.push(Some(i + 1));
                target.push(Some(f(i, i + 1)));
            } else if i % 5 == 0 {
                lhs.push(Some(i + 1));
                rhs.push(Some(i));
                target.push(Some(f(i + 1, i)));
            } else {
                lhs.push(Some(i));
                rhs.push(Some(i));
                target.push(Some(f(i, i)));
            }
        }

        let col1 = create_column(&lhs).unwrap();
        let col2 = create_column(&rhs).unwrap();
        let data_chunk = DataChunk::builder()
            .cardinality(2)
            .columns([col1, col2].to_vec())
            .build();
        let expr = make_expression(kind);
        let mut vec_excutor = CompareExpression::try_from(&expr).unwrap();
        let res = vec_excutor.eval(&data_chunk).unwrap();
        if let Bool(array) = res.as_ref() {
            assert_eq!(
                array
                    .iter()
                    .enumerate()
                    .all(|(idx, res)| res == target[idx]),
                true
            );
        } else {
            assert!(false);
        }
    }
    fn make_expression(kind: ExprNode_ExprNodeType) -> ExprNode {
        let lhs = make_inputref(0);
        let rhs = make_inputref(1);
        make_proto!(ExprNode, {
          expr_type: kind,
          body: AnyProto::pack(
            &make_proto!(FunctionCall, {
              children: RepeatedField::from_slice(&[lhs, rhs])
            })
          ).unwrap(),
          return_type: make_proto!(DataTypeProto, {
            type_name: risingwave_proto::data::DataType_TypeName::BOOLEAN
          })
        })
    }

    fn make_inputref(idx: i32) -> ExprNode {
        make_proto!(ExprNode, {
          expr_type: INPUT_REF,
          body: AnyProto::pack(
            &make_proto!(InputRefExpr, {column_idx: idx})
          ).unwrap(),
          return_type: make_proto!(DataTypeProto, {
            type_name: risingwave_proto::data::DataType_TypeName::INT32
          })
        })
    }

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        let data_type = Arc::new(Int32Type::new(false));
        Ok(Column { array, data_type })
    }
}
