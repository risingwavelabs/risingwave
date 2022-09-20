use futures::{Future, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use super::{BoxedExecutor, Executor, ExecutorInfo, MessageStreamItem};

/// Handle used to drive the subtask.
pub type SubtaskHandle = impl Future<Output = ()> + Send + 'static;

/// The thin wrapper for subtask-wrapped executor, containing a channel to receive the messages from
/// the subtask.
pub struct SubtaskRxExecutor {
    info: ExecutorInfo,

    rx: mpsc::Receiver<MessageStreamItem>,
}

impl Executor for SubtaskRxExecutor {
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

/// Wrap an executor into a subtask and a thin receiver executor, connected by a channel with a
/// buffer size of 1.
///
/// Used when there're multiple stateful executors in an actor. These subtasks can be concurrently
/// executed to improve the I/O performance, while the computing resource can be still bounded to a
/// single thread.
pub fn wrap(input: BoxedExecutor) -> (SubtaskHandle, SubtaskRxExecutor) {
    let (tx, rx) = mpsc::channel(1);
    let rx_executor = SubtaskRxExecutor {
        info: ExecutorInfo {
            identity: "SubtaskRxExecutor".to_owned(),
            ..input.info()
        },
        rx,
    };

    let handle = async move {
        let mut input = input.execute();
        while let Some(item) = input.next().await {
            // The downstream must not be dropped, so we `unwrap` here.
            tx.send(item).await.unwrap();
        }
    };

    (handle, rx_executor)
}
