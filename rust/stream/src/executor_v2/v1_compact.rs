use std::fmt;

use async_trait::async_trait;
use futures::StreamExt;
pub use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

pub use super::{BoxedMessageStream, ExecutorV1, Message, PkIndices, PkIndicesRef};

pub struct StreamExecutorV1 {
    pub(super) stream: BoxedMessageStream,

    pub(super) schema: Schema,
    pub(super) pk_indices: PkIndices,
    pub(super) identity: String,
}

impl fmt::Debug for StreamExecutorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamExecutor")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .field("identity", &self.identity)
            .finish()
    }
}

#[async_trait]
impl ExecutorV1 for StreamExecutorV1 {
    async fn next(&mut self) -> Result<Message> {
        self.stream.next().await.unwrap()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn logical_operator_info(&self) -> &str {
        // FIXME: use identity temporally.
        &self.identity
    }
}
