use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::RwError;

use crate::executor::BoxedExecutor;
use crate::executor2::{BoxedDataChunkStream, Executor2, ExecutorInfo};

/// A wrapper to convert `Executor` to `Executor2`
pub struct ExecutorWrapper {
    info: ExecutorInfo,
    executor: BoxedExecutor,
}

impl Executor2 for ExecutorWrapper {
    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn identity(&self) -> &str {
        self.info.id.as_str()
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl From<BoxedExecutor> for ExecutorWrapper {
    fn from(executor: BoxedExecutor) -> Self {
        let info = ExecutorInfo {
            schema: executor.schema().to_owned(),
            id: executor.identity().to_string(),
        };

        Self { info, executor }
    }
}

impl ExecutorWrapper {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(mut self: Box<Self>) {
        self.executor.open().await?;

        while let Some(d) = self.executor.next().await? {
            yield d;
        }

        self.executor.close().await?;
    }
}
