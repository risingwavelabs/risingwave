use std::convert::TryFrom;
use std::sync::Arc;

use protobuf::Message;

use risingwave_proto::expr::ExprNode_ExprNodeType;
use risingwave_proto::expr::{ExprNode, FunctionCall};

use crate::array2::ArrayRef;
use crate::array2::DataChunk;
use crate::error::ErrorCode::ProtobufError;
use crate::error::{Result, RwError};
use crate::expr::build_from_proto as expr_build_from_proto;
use crate::expr::BoxedExpression;
use crate::expr::Expression;
use crate::types::{build_from_proto as type_build_from_proto, DataType, DataTypeRef};
use crate::vector_op::substr::{vector_substr_end, vector_substr_start, vector_substr_start_end};

pub struct SubStrExpression {
    return_type: DataTypeRef,
    child: BoxedExpression,
    off: Option<BoxedExpression>,
    len: Option<BoxedExpression>,
}

impl Expression for SubStrExpression {
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        let array = self.child.eval(input)?;
        let mut off = None;
        if let Some(off_expr) = self.off.as_mut() {
            off = Some(off_expr.eval(input)?);
        }
        let mut len = None;
        if let Some(len_expr) = self.len.as_mut() {
            len = Some(len_expr.eval(input)?);
        }
        match (off, len) {
            (None, None) => Ok(array),
            (None, Some(len)) => {
                vector_substr_end(array.as_utf8(), len.as_int32()).map(|x| Arc::new(x.into()))
            }
            (Some(off), None) => {
                vector_substr_start(array.as_utf8(), off.as_int32()).map(|x| Arc::new(x.into()))
            }
            (Some(off), Some(len)) => {
                vector_substr_start_end(array.as_utf8(), off.as_int32(), len.as_int32())
                    .map(|x| Arc::new(x.into()))
            }
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for SubStrExpression {
    type Error = RwError;

    fn try_from(proto: &'a ExprNode) -> Result<Self> {
        ensure!(proto.expr_type == ExprNode_ExprNodeType::SUBSTR);
        let function_call_node =
            FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;
        let children = function_call_node.get_children();
        ensure!(
            (1..=3).contains(&children.len()),
            "parameters of substr should be string [off] [len]"
        );
        let child = expr_build_from_proto(&children[0])?;
        let mut off = None;
        let mut len = None;
        if function_call_node.get_children().len() == 2 {
            off = Some(expr_build_from_proto(&children[1])?);
        } else if function_call_node.get_children().len() == 3 {
            off = Some(expr_build_from_proto(&children[1])?);
            len = Some(expr_build_from_proto(&children[2])?);
        }
        let return_type = type_build_from_proto(proto.get_return_type())?;
        Ok(SubStrExpression {
            return_type,
            child,
            off,
            len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::column::Column;
    use crate::array2::UTF8Array;
    use crate::array2::{Array, DataChunk};
    use crate::types::Int32Type;
    use pb_construct::make_proto;
    use protobuf::well_known_types::Any as AnyProto;
    use protobuf::RepeatedField;
    use risingwave_proto::data::DataType as DataTypeProto;
    use risingwave_proto::expr::ExprNode_ExprNodeType::{CONSTANT_VALUE, INPUT_REF, SUBSTR};
    use risingwave_proto::expr::{ConstantValue, InputRefExpr};
    use risingwave_proto::expr::{ExprNode, FunctionCall};
    #[test]
    fn test_execute() {
        mock_execute(Some(1), None);
        mock_execute(Some(1), Some(10));
    }

    fn mock_execute(off: Option<usize>, len: Option<usize>) {
        let s = "abcdefghijklmnopqrstuvwxyz";
        let mut value = Vec::<Option<&str>>::new();
        let mut target = Vec::<Option<&str>>::new();
        let mut left = 0usize;
        let mut right = s.len();
        if let Some(off) = off {
            left = off;
        }
        if let Some(len) = len {
            right = left + len;
        }
        for i in 0..100 {
            if i % 2 == 0 {
                #[allow(clippy::clone_double_ref)]
                value.push(Some(s.clone()));
                target.push(Some(&s[left..right]));
            } else {
                value.push(None);
                target.push(None);
            }
        }

        let col = create_column(&value).unwrap();
        let data_chunk = DataChunk::builder()
            .cardinality(100)
            .columns(vec![col])
            .build();
        let expr = make_expression(off, len);
        let mut vec_excutor = SubStrExpression::try_from(&expr).unwrap();
        let res = vec_excutor.eval(&data_chunk).unwrap();
        for (x, y) in res.as_utf8().iter().zip(target.iter()) {
            assert_eq!(x, *y);
        }
    }
    fn make_expression(off: Option<usize>, len: Option<usize>) -> ExprNode {
        let mut expr_vec = vec![make_inputref(0)];
        if let Some(off) = off {
            expr_vec.push(make_literal(off as i32))
        }
        if let Some(len) = len {
            expr_vec.push(make_literal(len as i32))
        }
        make_proto!(ExprNode, {
          expr_type: SUBSTR,
          body: AnyProto::pack(
            &make_proto!(FunctionCall, {
              children: RepeatedField::from_slice(expr_vec.as_ref())
            })
          ).unwrap(),
          return_type: make_proto!(DataTypeProto, {
            type_name: risingwave_proto::data::DataType_TypeName::BOOLEAN
          })
        })
    }

    fn make_literal(literal: i32) -> ExprNode {
        make_proto!(ExprNode, {
          expr_type: CONSTANT_VALUE,
          body: AnyProto::pack(
              &make_proto!(ConstantValue, {body: literal.to_be_bytes().to_vec()})
            ).unwrap(),
          return_type: make_proto!(DataTypeProto, {
              type_name: risingwave_proto::data::DataType_TypeName::INT32
            })
          }
        )
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

    fn create_column(vec: &[Option<&str>]) -> Result<Column> {
        let array = UTF8Array::from_slice(vec).map(|x| Arc::new(x.into()))?;
        let data_type = Arc::new(Int32Type::new(false));
        Ok(Column::new(array, data_type))
    }
}
