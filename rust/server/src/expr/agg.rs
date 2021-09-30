use crate::array2::{ArrayRef, DataChunk};
use crate::error::{ErrorCode, Result, RwError};
use crate::expr::{Expression, InputRefExpression};
use crate::types::{build_from_proto as type_build_from_proto, DataType, DataTypeRef};
use crate::vector_op::agg::{self, BoxedAggState};
use risingwave_proto::expr::{AggCall, AggCall_Type};
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
    argument: InputRefExpression,
}

impl AggExpression {
    pub fn return_type(&self) -> &dyn DataType {
        &*self.return_type
    }

    pub fn return_type_ref(&self) -> DataTypeRef {
        self.return_type.clone()
    }

    pub fn create_agg_state(&self) -> Result<BoxedAggState> {
        agg::create_agg_state(
            self.argument.return_type_ref(),
            &self.agg_kind,
            self.return_type_ref(),
        )
    }
    pub fn eval_child(&mut self, input: &DataChunk) -> Result<ArrayRef> {
        let child_output = self.argument.eval(input)?;
        // ensure!(self.child.return_type().data_type_kind() == child_output.data_type().data_type_kind());
        Ok(child_output)
    }
}

impl<'a> TryFrom<AggCall_Type> for AggKind {
    type Error = RwError;

    fn try_from(proto: AggCall_Type) -> Result<Self> {
        match proto {
            AggCall_Type::MIN => Ok(AggKind::Min),
            AggCall_Type::MAX => Ok(AggKind::Max),
            AggCall_Type::SUM => Ok(AggKind::Sum),
            AggCall_Type::AVG => Ok(AggKind::Avg),
            AggCall_Type::COUNT => Ok(AggKind::Count),
            _ => Err(ErrorCode::InternalError("Unrecognized agg.".into()).into()),
        }
    }
}

impl<'a> TryFrom<&'a AggCall> for AggExpression {
    type Error = RwError;

    fn try_from(proto: &'a AggCall) -> Result<Self> {
        ensure!(
            proto.get_args().len() == 1,
            "Agg expression can only have exactly one child"
        );
        let arg = &proto.get_args()[0];
        Ok(AggExpression {
            return_type: type_build_from_proto(proto.get_return_type())?,
            agg_kind: AggKind::try_from(proto.get_field_type())?,
            argument: InputRefExpression::new(
                type_build_from_proto(arg.get_field_type())?,
                arg.get_input().get_column_idx() as usize,
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::{column::Column, Array as _, I32Array, I64Array};
    use crate::array_nonnull;
    use crate::types::{Int32Type, Int64Type};
    use std::sync::Arc;

    #[test]
    fn eval_sum_int32() -> Result<()> {
        let t32 = Arc::new(Int32Type::new(false));
        let t64 = Arc::new(Int64Type::new(false));
        let mut e = AggExpression {
            return_type: t64.clone(),
            agg_kind: AggKind::Sum,
            argument: InputRefExpression::new(t32.clone(), 0),
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
