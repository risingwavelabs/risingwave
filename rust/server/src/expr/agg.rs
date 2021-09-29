use crate::array2::{ArrayRef, DataChunk};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{build_from_proto as expr_build_from_proto, BoxedExpression, Expression};
use crate::types::{build_from_proto as type_build_from_proto, DataType, DataTypeRef};
use crate::vector_op::agg::{self, BoxedAggState};
use protobuf::Message as _;
use risingwave_proto::expr::{ExprNode, ExprNode_ExprNodeType, FunctionCall};
use std::convert::TryFrom;

#[derive(Debug)]
pub enum AggKind {
    Min,
    Max,
    Sum,
    Count,
    Avg,
}

pub struct AggExpression {
    return_type: DataTypeRef,
    agg_kind: AggKind,
    child: BoxedExpression,
}

impl Expression for AggExpression {
    fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    fn eval(&mut self, _input: &DataChunk) -> Result<ArrayRef> {
        Err(
            ErrorCode::InternalError("AggExpression shall not be evaluated as normal.".into())
                .into(),
        )
    }
}

impl AggExpression {
    pub fn create_agg_state(&self) -> Result<BoxedAggState> {
        agg::create_agg_state(
            self.child.return_type_ref(),
            &self.agg_kind,
            self.return_type_ref(),
        )
    }
    pub fn eval_child(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        let child_output = self.child.eval(input)?;
        // ensure!(self.child.return_type().data_type_kind() == child_output.data_type().data_type_kind());
        Ok(child_output)
    }
}

impl<'a> TryFrom<ExprNode_ExprNodeType> for AggKind {
    type Error = RwError;

    fn try_from(proto: ExprNode_ExprNodeType) -> Result<Self> {
        match proto {
            ExprNode_ExprNodeType::MIN => Ok(AggKind::Min),
            ExprNode_ExprNodeType::MAX => Ok(AggKind::Max),
            ExprNode_ExprNodeType::SUM => Ok(AggKind::Sum),
            ExprNode_ExprNodeType::AVG => Ok(AggKind::Avg),
            ExprNode_ExprNodeType::COUNT => Ok(AggKind::Count),
            _ => Err(ErrorCode::InternalError("Unrecognized agg.".into()).into()),
        }
    }
}

impl<'a> TryFrom<&'a ExprNode> for AggExpression {
    type Error = RwError;

    fn try_from(proto: &'a ExprNode) -> Result<Self> {
        let agg_kind = AggKind::try_from(proto.get_expr_type())?;
        let data_type = type_build_from_proto(proto.get_return_type())?;
        let function_call_node =
            FunctionCall::parse_from_bytes(proto.get_body().get_value()).map_err(ProtobufError)?;

        ensure!(
            function_call_node.get_children().len() == 1,
            "Agg expression can only have exactly one child"
        );

        match function_call_node.get_children().get(0) {
            Some(child_expr_node) => {
                let child_expr = expr_build_from_proto(child_expr_node)?;
                Ok(Self {
                    return_type: data_type,
                    agg_kind,
                    child: child_expr,
                })
            }

            None => Err(InternalError(
                "Agg expression can only have exactly one child".to_string(),
            )
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::{column::Column, Array as _, I32Array, I64Array};
    use crate::array_nonnull;
    use crate::expr::InputRefExpression;
    use crate::types::{Int32Type, Int64Type};
    use std::sync::Arc;

    #[test]
    fn eval_sum_int32() -> Result<()> {
        let t32 = Arc::new(Int32Type::new(false));
        let t64 = Arc::new(Int64Type::new(false));
        let mut e = AggExpression {
            return_type: t64.clone(),
            agg_kind: AggKind::Sum,
            child: Box::new(InputRefExpression::new(t32.clone(), 0)),
        };

        let a = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let chunk = DataChunk::builder()
            .columns(vec![Column::new(a, t32)])
            .build();
        let mut s = e.create_agg_state()?;
        s.update(e.eval_child(&chunk)?.as_ref())?;
        let mut builder = t64.create_array_builder(1)?;
        s.output(&mut builder)?;
        let o = builder.finish()?;

        let a: &I64Array = (&o).into();
        let s = a.iter().collect::<Vec<_>>();
        assert_eq!(s, vec![Some(6)]);

        Ok(())
    }
}
