// use std::convert::TryFrom;
//
// use protobuf::Message;
//
// use risingwave_proto::expr::{ExprNode, FunctionCall};
//
// use crate::array2::{ArrayRef};
// use crate::array2::DataChunk;
// use crate::error::ErrorCode::{InternalError, ProtobufError};
// use crate::error::{Result, RwError};
// use crate::expr::build_from_proto as expr_build_from_proto;
// use crate::expr::BoxedExpression;
// use crate::expr::Expression;
// use crate::types::{build_from_proto as type_build_from_proto, DataType, DataTypeRef};
// use crate::vector_op::cmp::{vec_eq, vec_geq, vec_gt, vec_leq, vec_lt, vec_neq};
// use risingwave_proto::expr::ExprNode_ExprNodeType::{
//   EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, NOT_EQUAL,
// };
// use crate::array2::ArrayImpl;
//
// pub enum CompareOperatorKind {
//   Equal,
//   NotEqual,
//   GreaterThan,
//   GreaterThanOrEqual,
//   LessThan,
//   LessThanOrEqual,
// }
// pub(super) struct CompareExpression {
//   return_type: DataTypeRef,
//   kind: CompareOperatorKind,
//   lhs: BoxedExpression,
//   rhs: BoxedExpression,
// }
//
// impl Expression for CompareExpression {
//   fn return_type(&self) -> &dyn DataType {
//     &*self.return_type
//   }
//
//   fn return_type_ref(&self) -> DataTypeRef {
//     self.return_type.clone()
//   }
//
//   fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
//     let lhs = self.lhs.eval(input)?;
//     let rhs = self.rhs.eval(input)?;
//     match self.kind {
//       CompareOperatorKind::Equal => vec_eq(lhs.into(), rhs.into()),
//       CompareOperatorKind::NotEqual => vec_neq(lhs.into(), rhs.into()),
//       CompareOperatorKind::GreaterThan => vec_gt(lhs.into(), rhs.into()),
//       CompareOperatorKind::GreaterThanOrEqual => vec_geq(lhs.into(), rhs.into()),
//       CompareOperatorKind::LessThan => vec_lt(lhs.into(), rhs.into()),
//       CompareOperatorKind::LessThanOrEqual => vec_leq(lhs.into(), rhs.into()),
//     }
//   }
// }
//
// impl<'a> TryFrom<&'a ExprNode> for CompareExpression {
//   type Error = RwError;
//
//   fn try_from(proto: &'a ExprNode) -> Result<Self> {
//     let function_call_node =
//       FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
//     ensure!(
//       function_call_node.get_children().len() == 2,
//       "cmp expression mast have exactly two child"
//     );
//     // we have checked that it has 2 children
//     let lhs = expr_build_from_proto(function_call_node.get_children().get(0).unwrap())?;
//     let rhs = expr_build_from_proto(function_call_node.get_children().get(1).unwrap())?;
//     let kind = match proto.get_expr_type() {
//       EQUAL => CompareOperatorKind::Equal,
//       NOT_EQUAL => CompareOperatorKind::NotEqual,
//       GREATER_THAN => CompareOperatorKind::GreaterThan,
//       GREATER_THAN_OR_EQUAL => CompareOperatorKind::GreaterThanOrEqual,
//       LESS_THAN => CompareOperatorKind::LessThan,
//       LESS_THAN_OR_EQUAL => CompareOperatorKind::LessThanOrEqual,
//       _ => return Err(InternalError("unsupported compare operator".to_string()).into()),
//     };
//     let return_type = type_build_from_proto(proto.get_return_type())?;
//     Ok(Self {
//       return_type,
//       kind,
//       rhs,
//       lhs,
//     })
//   }
// }
//
// #[cfg(test)]
// mod tests {
//   use super::*;
//   use crate::array::{BoolArray, PrimitiveArray};
//   use crate::types::Int32Type;
//   use crate::util::downcast_ref;
//   use pb_construct::make_proto;
//   use protobuf::well_known_types::Any as AnyProto;
//   use protobuf::RepeatedField;
//   use risingwave_proto::data::DataType as DataTypeProto;
//   use risingwave_proto::expr::ExprNode_ExprNodeType;
//   use risingwave_proto::expr::ExprNode_ExprNodeType::INPUT_REF;
//   use risingwave_proto::expr::InputRefExpr;
//   #[test]
//   fn test_execute() {
//     mock_execute(&[1, 2, 3], &[1, 2, 4], EQUAL, &[true, true, false]);
//     mock_execute(&[1, 2, 3], &[1, 2, 4], NOT_EQUAL, &[false, false, true]);
//     mock_execute(&[1, 2, 3], &[1, 2, 4], LESS_THAN, &[false, false, true]);
//     mock_execute(
//       &[1, 2, 3],
//       &[1, 2, 2],
//       LESS_THAN_OR_EQUAL,
//       &[true, true, false],
//     );
//     mock_execute(&[1, 2, 3], &[1, 2, 2], GREATER_THAN, &[false, false, true]);
//     mock_execute(
//       &[1, 2, 3],
//       &[1, 2, 4],
//       GREATER_THAN_OR_EQUAL,
//       &[true, true, false],
//     );
//   }
//
//   fn mock_execute(lhs: &[i32], rhs: &[i32], kind: ExprNode_ExprNodeType, targets: &[bool]) {
//     let lhs = PrimitiveArray::<Int32Type>::from_slice(lhs).unwrap();
//     let rhs = PrimitiveArray::<Int32Type>::from_slice(rhs).unwrap();
//     let data_chunk = DataChunk::builder()
//       .cardinality(2)
//       .arrays([lhs, rhs].to_vec())
//       .build();
//     // make_proto!(Expression, );
//     let expr = make_expression(kind);
//     let mut vec_excutor = CompareExpression::try_from(&expr).unwrap();
//     let res = vec_excutor.eval(&data_chunk).unwrap();
//     let res = downcast_ref(res.as_ref()).unwrap() as &BoolArray;
//     let iter = res.as_iter().unwrap();
//     assert_eq!(
//       iter
//         .into_iter()
//         .enumerate()
//         .all(|(idx, res)| res == Some(targets[idx])),
//       true
//     );
//   }
//   fn make_expression(kind: ExprNode_ExprNodeType) -> ExprNode {
//     let lhs = make_inputref(0);
//     let rhs = make_inputref(1);
//     make_proto!(ExprNode, {
//       expr_type: kind,
//       body: AnyProto::pack(
//         &make_proto!(FunctionCall, {
//           children: RepeatedField::from_slice(&[lhs, rhs])
//         })
//       ).unwrap(),
//       return_type: make_proto!(DataTypeProto, {
//         type_name: risingwave_proto::data::DataType_TypeName::BOOLEAN
//       })
//     })
//   }
//
//   fn make_inputref(idx: i32) -> ExprNode {
//     make_proto!(ExprNode, {
//       expr_type: INPUT_REF,
//       body: AnyProto::pack(
//         &make_proto!(InputRefExpr, {column_idx: idx})
//       ).unwrap(),
//       return_type: make_proto!(DataTypeProto, {
//         type_name: risingwave_proto::data::DataType_TypeName::INT32
//       })
//     })
//   }
// }
