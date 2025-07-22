// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::Future;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;

use super::actor::spawn_blocking_drop_stream;
use crate::executor::prelude::*;

/// Handle used to drive the subtask.
pub type SubtaskHandle = impl Future<Output = ()> + Send + 'static;

/// The thin wrapper for subtask-wrapped executor, containing a channel to receive the messages from
/// the subtask.
pub struct SubtaskRxExecutor {
    rx: mpsc::Receiver<MessageStreamItem>,
}

impl Execute for SubtaskRxExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        ReceiverStream::new(self.rx).boxed()
    }
}

/// Wrap an executor into a subtask and a thin receiver executor, connected by a channel with a
/// buffer size of 1.
///
/// Used when there're multiple stateful executors in an actor. These subtasks can be concurrently
/// executed to improve the I/O performance, while the computing resource can be still bounded to a
/// single thread.
#[define_opaque(SubtaskHandle)]
pub fn wrap(input: Executor, actor_id: ActorId) -> (SubtaskHandle, SubtaskRxExecutor) {
    let (tx, rx) = mpsc::channel(1);
    let rx_executor = SubtaskRxExecutor { rx };

    let handle = async move {
        let mut input = input.execute();

        while let Some(item) = input.next().await {
            // Decide whether to stop the subtask. We explicitly do this instead of relying on the
            // termination of the input stream, because we don't want to exhaust the stream, which
            // causes the stream dropped in the scope of the current async task and blocks other
            // actors. See `spawn_blocking_drop_stream` for more details.
            let to_stop = match &item {
                Ok(Message::Barrier(barrier)) => barrier.is_stop(actor_id),
                Ok(_) => false,
                Err(_) => true,
            };

            // It's possible that the downstream itself yields an error (e.g. from remote input) and
            // finishes, so we may fail to send the message. In this case, we can simply ignore the
            // send error and exit as well. If the message itself is another error, log it.
            if let Err(SendError(item)) = tx.send(item).await {
                match item {
                    Ok(_) => tracing::error!("actor downstream subtask failed"),
                    Err(e) => tracing::error!(
                        error = %e.as_report(),
                        "after actor downstream subtask failed, another error occurs"
                    ),
                }
                break;
            }

            if to_stop {
                break;
            }
        }

        spawn_blocking_drop_stream(input).await;
    }
    .instrument_await("Subtask");

    (handle, rx_executor)
}
