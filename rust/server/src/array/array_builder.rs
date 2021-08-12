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
