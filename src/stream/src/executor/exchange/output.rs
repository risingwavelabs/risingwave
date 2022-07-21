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

use std::fmt::Debug;

use async_trait::async_trait;
use risingwave_common::error::{internal_error, Result};
use risingwave_common::util::addr::is_local_address;
use tokio::sync::mpsc::Sender;

use crate::executor::Message;
use crate::task::{ActorId, SharedContext};

/// `Output` provides an interface for `Dispatcher` to send data into downstream actors.
#[async_trait]
pub trait Output: Debug + Send + Sync + 'static {
    async fn send(&mut self, message: Message) -> Result<()>;

    /// The downstream actor id.
    fn actor_id(&self) -> ActorId;

    fn boxed(self) -> BoxedOutput
    where
        Self: Sized + 'static,
    {
        Box::new(self)
    }
}

pub type BoxedOutput = Box<dyn Output>;

/// `LocalOutput` sends data to a local channel.
pub struct LocalOutput {
    actor_id: ActorId,

    ch: Sender<Message>,
}

impl Debug for LocalOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalOutput")
            .field("actor_id", &self.actor_id)
            .finish()
    }
}

impl LocalOutput {
    pub fn new(actor_id: ActorId, ch: Sender<Message>) -> Self {
        Self { actor_id, ch }
    }
}

#[async_trait]
impl Output for LocalOutput {
    async fn send(&mut self, message: Message) -> Result<()> {
        self.ch
            .send(message)
            .await
            .map_err(|_| internal_error("failed to send"))
    }

    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// `RemoteOutput` compacts the data and send to a local buffer channel, which will be further sent
/// to the remote actor by [`ExchangeService`].
///
/// [`ExchangeService`]: risingwave_pb::task_service::exchange_service_server::ExchangeService
pub struct RemoteOutput {
    actor_id: ActorId,

    ch: Sender<Message>,
}

impl Debug for RemoteOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteOutput")
            .field("actor_id", &self.actor_id)
            .finish()
    }
}

impl RemoteOutput {
    pub fn new(actor_id: ActorId, ch: Sender<Message>) -> Self {
        Self { actor_id, ch }
    }
}

#[async_trait]
impl Output for RemoteOutput {
    async fn send(&mut self, message: Message) -> Result<()> {
        let message = match message {
            Message::Chunk(chk) => Message::Chunk(chk.compact()?),
            _ => message,
        };

        self.ch
            .send(message)
            .await
            .map_err(|_| internal_error("failed to send"))
    }

    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// Create a [`LocalOutput`] or [`RemoteOutput`] instance for the current actor id and the
/// downstream actor id. Used by dispatchers.
pub fn new_output(
    context: &SharedContext,
    actor_id: ActorId,
    down_id: ActorId,
) -> Result<BoxedOutput> {
    let downstream_addr = context.get_actor_info(&down_id)?.get_host()?.into();
    let tx = context.take_sender(&(actor_id, down_id))?;

    let output = if is_local_address(&context.addr, &downstream_addr) {
        LocalOutput::new(down_id, tx).boxed()
    } else {
        RemoteOutput::new(down_id, tx).boxed()
    };

    Ok(output)
}
