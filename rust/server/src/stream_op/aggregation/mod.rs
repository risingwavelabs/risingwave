mod foldable;
pub use foldable::*;

mod row_count;
pub use row_count::*;

use super::{Op, Ops};
use crate::array2::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl};
use crate::buffer::Bitmap;
use crate::error::Result;

/// `StreamingAggState` records a state of streaming expression. For example,
/// there will be `StreamingAggCompare` and `StreamingAggSum`.
pub trait StreamingAggState<A: Array>: Send + Sync + 'static {
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &A,
    ) -> Result<()>;
}

/// `StreamingAggFunction` allows us to get output from a streaming state.
pub trait StreamingAggFunction<B: ArrayBuilder>: Send + Sync + 'static {
    fn get_output_concrete(&self, builder: &mut B) -> Result<()>;
}

/// `StreamingAggStateImpl` erases the associated type information of
/// `StreamingAggState` and `StreamingAggFunction`. You should manually
/// implement this trait for necessary types.
pub trait StreamingAggStateImpl: Send + Sync + 'static {
    fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        data: &ArrayImpl,
    ) -> Result<()>;

    fn get_output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;

    fn new_builder(&self) -> ArrayBuilderImpl;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::array2::from_builder;

    pub fn get_output_from_state<O, S>(state: &mut S) -> O::ArrayType
    where
        O: ArrayBuilder,
        S: StreamingAggFunction<O>,
    {
        from_builder(|builder| state.get_output_concrete(builder)).unwrap()
    }

    pub fn get_output_from_impl_state(state: &mut impl StreamingAggStateImpl) -> ArrayImpl {
        let mut builder = state.new_builder();
        state.get_output(&mut builder).unwrap();
        builder.finish().unwrap()
    }
}
