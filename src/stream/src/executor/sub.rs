use futures::{FutureExt, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{BoxedExecutor, Executor, ExecutorInfo, MessageStreamItem};
use crate::task::ActorSubHandle;

struct RxExecutor {
    info: ExecutorInfo,

    rx: mpsc::Receiver<MessageStreamItem>,
}

impl Executor for RxExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        ReceiverStream::new(self.rx).boxed()
    }

    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

pub fn pair(input: BoxedExecutor) -> (ActorSubHandle, BoxedExecutor) {
    let (tx, rx) = mpsc::channel(4);
    let info = input.info();
    let rx = RxExecutor { info, rx }.boxed();

    let fut = async move {
        let mut input = input.execute();
        while let Some(item) = input.next().await {
            tx.send(item).await.unwrap();
        }
    }
    .boxed();

    (fut, rx)
}
