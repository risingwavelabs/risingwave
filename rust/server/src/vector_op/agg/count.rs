use super::{AggFunctionConcrete, AggStateConcrete, Aggregator};
use crate::array2::*;
use crate::error::Result;

/// `AggCount` counts all input data.
pub struct AggCount {
    result: usize,
}

impl<A> AggStateConcrete<A> for AggCount
where
    A: Array,
{
    fn update_concrete(&mut self, input: &A) -> Result<()> {
        self.result += input.len();
        Ok(())
    }
}

impl AggFunctionConcrete<PrimitiveArrayBuilder<i64>> for AggCount {
    fn output_concrete(&self, builder: &mut PrimitiveArrayBuilder<i64>) -> Result<()> {
        builder.append(Some(self.result as i64))?;
        Ok(())
    }
}

impl AggCount {
    pub fn new() -> Self {
        Self { result: 0 }
    }
}

impl Aggregator for AggCount {
    fn update(&mut self, input: &ArrayImpl) -> Result<()> {
        match input {
            ArrayImpl::Int16(i) => self.update_concrete(i),
            ArrayImpl::Int32(i) => self.update_concrete(i),
            ArrayImpl::Int64(i) => self.update_concrete(i),
            ArrayImpl::Float32(i) => self.update_concrete(i),
            ArrayImpl::Float64(i) => self.update_concrete(i),
            ArrayImpl::UTF8(i) => self.update_concrete(i),
            ArrayImpl::Bool(i) => self.update_concrete(i),
        }
    }

    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        match builder {
            ArrayBuilderImpl::Int64(b) => self.output_concrete(b),
            _ => panic!("type mismatch"),
        }
    }
}
