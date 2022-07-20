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

    fn actor_id(&self) -> ActorId;
}

pub type BoxedOutput = Box<dyn Output>;

/// `LocalOutput` sends data to a local `mpsc::Channel`
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
            .map_err(|_| internal_error("failed to send"))?;
        Ok(())
    }

    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

/// `RemoteOutput` forwards data to`ExchangeServiceImpl`
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
            .map_err(|_| internal_error("failed to send"))?;

        Ok(())
    }

    fn actor_id(&self) -> ActorId {
        self.actor_id
    }
}

pub fn new_output(
    context: &SharedContext,
    actor_id: ActorId,
    down_id: ActorId,
) -> Result<BoxedOutput> {
    let downstream_addr = context.get_actor_info(&down_id)?.get_host()?.into();
    let tx = context.take_sender(&(actor_id, down_id))?;
    if is_local_address(&context.addr, &downstream_addr) {
        // if this is a local downstream actor
        Ok(Box::new(LocalOutput::new(down_id, tx)) as BoxedOutput)
    } else {
        Ok(Box::new(RemoteOutput::new(down_id, tx)) as BoxedOutput)
    }
}
