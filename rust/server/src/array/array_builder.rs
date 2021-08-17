use crate::array::ArrayRef;
use crate::error::Result;
use crate::expr::{Datum, ExpressionOutput};

pub(crate) trait ArrayBuilder {
    fn append(&mut self, datum: &Datum) -> Result<()>;
    fn append_expr_output(&mut self, output: ExpressionOutput) -> Result<()> {
        match output {
            ExpressionOutput::Array(_) => todo!(),
            ExpressionOutput::Literal(datum) => self.append(datum),
        }
    }
    fn finish(self: Box<Self>) -> Result<ArrayRef>;
}

pub(crate) type BoxedArrayBuilder = Box<dyn ArrayBuilder>;

#[cfg(test)]
mod tests {
    use crate::array::{ArrayBuilder, PrimitiveArrayBuilder};
    use crate::error::Result;
    use crate::expr::Datum;
    use crate::types::Int32Type;
    use std::sync::Arc;

    #[test]
    fn test_array_builder_i32() -> Result<()> {
        let mut i32_builder = Box::new(PrimitiveArrayBuilder::<Int32Type>::new(
            Arc::new(Int32Type::new(false)),
            1,
        ));
        i32_builder.append(&Datum::Int32(1))?;
        let arr = i32_builder.finish()?;
        assert_eq!(arr.len(), 1);
        Ok(())
    }
}
