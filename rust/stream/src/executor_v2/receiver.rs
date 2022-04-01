use futures::channel::mpsc::Receiver;
use futures::StreamExt;
use futures_async_stream::{for_await, try_stream};
use risingwave_common::catalog::Schema;
use tracing_futures::Instrument;

use crate::executor_v2::error::TracedStreamExecutorError;
use crate::executor_v2::{BoxedMessageStream, Executor, Message, PkIndices, PkIndicesRef};
/// `ReceiverExecutor` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverExecutor` running in the background, so as to push
/// messages down to the executors.
pub struct ReceiverExecutor {
    schema: Schema,
    pk_indices: PkIndices,
    receiver: Receiver<Message>,
    /// Logical Operator Info
    op_info: String,
}

impl std::fmt::Debug for ReceiverExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiverExecutor")
            .field("schema", &self.schema)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

impl ReceiverExecutor {
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        receiver: Receiver<Message>,
        op_info: String,
    ) -> Self {
        Self {
            schema,
            pk_indices,
            receiver,
            op_info,
        }
    }
}

impl Executor for ReceiverExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "ReceiverExecutor"
    }
}

impl ReceiverExecutor {
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(mut self) {
        // TODO: Rewrite it in a more stream way be replace the channel to stream channel.
        yield self
            .receiver
            .next()
            .instrument(tracing::trace_span!("idle"))
            .await
            .expect(
                "upstream channel closed unexpectedly, please check error in upstream executors",
            );
    }

    pub fn new_from_v1(
        schema: Schema,
        pk_indices: PkIndices,
        receiver: Receiver<Message>,
        op_info: String,
    ) -> Self {
        Self {
            schema,
            pk_indices,
            receiver,
            op_info,
        }
    }
}
