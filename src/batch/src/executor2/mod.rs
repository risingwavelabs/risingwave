pub mod executor_wrapper;
mod filter;
pub use filter::*;
mod trace;
use futures::stream::BoxStream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
pub use trace::*;

use crate::executor::executor2_wrapper::Executor2Wrapper;
use crate::executor::{BoxedExecutor, ExecutorBuilder};

pub type BoxedExecutor2 = Box<dyn Executor2>;
pub type BoxedDataChunkStream = BoxStream<'static, Result<DataChunk>>;

pub struct ExecutorInfo {
    pub schema: Schema,
    pub id: String,
}

/// Refactoring of `Executor` using `Stream`.
pub trait Executor2: Send + 'static {
    /// Returns the schema of the executor's return data.
    ///
    /// Schema must be available before `init`.
    fn schema(&self) -> &Schema;

    /// Identity string of the executor
    fn identity(&self) -> &str;

    /// Executes to return the data chunk stream.
    ///
    /// The implementation should guaranteed that each `DataChunk`'s cardinality is not zero.
    fn execute(self: Box<Self>) -> BoxedDataChunkStream;
}

/// Every Executor should impl this trait to provide a static method to build a `BoxedExecutor2`
/// from proto and global environment.
pub trait BoxedExecutor2Builder {
    fn new_boxed_executor2(source: &ExecutorBuilder) -> Result<BoxedExecutor2>;

    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        Ok(Box::new(Executor2Wrapper::from(Self::new_boxed_executor2(
            source,
        )?)))
    }
}
