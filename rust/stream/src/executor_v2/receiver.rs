use async_stream::stream;
use futures::channel::mpsc::Receiver;
use futures::{Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;

use crate::executor_v2::error::TracedStreamExecutorError;
use crate::executor_v2::{
    BoxedMessageStream, Executor, ExecutorInfo, Message, PkIndices, PkIndicesRef,
};

/// `ReceiverExecutor` is used along with a channel. After creating a mpsc channel,
/// there should be a `ReceiverExecutor` running in the background, so as to push
/// messages down to the executors.
pub struct ReceiverExecutor {
    receiver: Receiver<Message>,
    /// Logical Operator Info
    info: ExecutorInfo,
}

impl std::fmt::Debug for ReceiverExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceiverExecutor")
            .field("schema", &self.info.schema)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}

impl ReceiverExecutor {
    pub fn new(schema: Schema, pk_indices: PkIndices, receiver: Receiver<Message>) -> Self {
        Self {
            receiver,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "ReceiverExecutor".to_string(),
            },
        }
    }
}

impl Executor for ReceiverExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

impl ReceiverExecutor {
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self) {
        let stream = Self::stream(self.receiver);
        #[for_await]
        for val in stream {
            yield val;
        }
    }

    /// Drains message in receiver and convert it into stream.
    fn stream(recv: Receiver<Message>) -> impl Stream<Item = Message> {
        stream! {
            for await value in recv {
                yield value;
            }
        }
    }
}
