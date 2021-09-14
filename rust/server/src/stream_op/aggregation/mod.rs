mod foldable;
pub use foldable::*;

use super::{Op, Ops};
use crate::array2::{Array, ArrayBuilder, ArrayBuilderImpl, ArrayImpl};
use crate::buffer::Bitmap;
use crate::error::Result;

/// `StreamingAggState` records a state of streaming expression. For example,
/// there will be `StreamingAggCompare` and `StreamingAggSum`.
pub trait StreamingAggState<A: Array> {
    fn apply_batch_concrete(&mut self, ops: Ops<'_>, skip: Option<&Bitmap>, data: &A)
        -> Result<()>;
}

/// `StreamingAggFunction` allows us to get output from a streaming state.
pub trait StreamingAggFunction<B: ArrayBuilder> {
    fn get_output_concrete(&self, builder: &mut B) -> Result<()>;
}

/// `StreamingAggStateImpl` erases the associated type information of
/// `StreamingAggState` and `StreamingAggFunction`. You should manually
/// implement this trait for necessary types.
pub trait StreamingAggStateImpl: Send + Sync + 'static {
    fn apply_batch(&mut self, ops: Ops<'_>, skip: Option<&Bitmap>, data: &ArrayImpl) -> Result<()>;

    fn get_output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;

    fn new_builder(&self) -> ArrayBuilderImpl;
}
