mod cache_clear;
mod epoch_check;
mod schema_check;
mod trace;
use std::fmt::Debug;

use async_trait::async_trait;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

pub use self::cache_clear::*;
pub use self::epoch_check::*;
pub use self::schema_check::*;
pub use self::trace::*;
use super::{Executor, Message};

/// [`DebugExecutor`] is an abstraction of wrapper executors, generally used for debug purpose. Data
/// related functions are mostly delegated to the `input` executor.
#[async_trait]
pub trait DebugExecutor: Send + Debug + 'static {
    async fn next(&mut self) -> Result<Message>;

    fn input(&self) -> &dyn Executor;

    fn input_mut(&mut self) -> &mut dyn Executor;
}

#[async_trait]
impl<E> Executor for E
where
    E: DebugExecutor,
{
    async fn next(&mut self) -> Result<Message> {
        DebugExecutor::next(self).await
    }

    fn schema(&self) -> &Schema {
        self.input().schema()
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        self.input().pk_indices()
    }

    fn identity(&self) -> &str {
        self.input().identity()
    }

    fn logical_operator_info(&self) -> &str {
        self.input().logical_operator_info()
    }

    fn clear_cache(&mut self) -> Result<()> {
        self.input_mut().clear_cache()
    }

    fn init(&mut self, epoch: u64) -> Result<()> {
        self.input_mut().init(epoch)
    }
}
