// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::{Future, StreamExt};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
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

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
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
            // It's possible that the downstream itself yields an error (e.g. from remote input) and
            // finishes, so we may fail to send the message. In this case, we can simply ignore the
            // send error and exit as well. If the message itself is another error, log it.
            if let Err(SendError(item)) = tx.send(item).await {
                match item {
                    Ok(_) => tracing::error!("actor downstream subtask failed"),
                    Err(e) => tracing::error!(
                        "after actor downstream subtask failed, another error occurs: {e}"
                    ),
                }
                break;
            }
        }
    };

    (handle, rx_executor)
}
