use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};

use crate::executor::Message;

const INITIAL_PERMITS: usize = 32768;
pub const BATCHED_PERMITS: usize = 4096;
const MAX_CHUNK_PERMITS: usize = INITIAL_PERMITS - BATCHED_PERMITS;

pub struct MessageWithPermits {
    pub message: Message,
    pub permits: u32,
}

pub fn channel() -> (Sender, Receiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    let permits = Arc::new(Semaphore::new(INITIAL_PERMITS));
    (
        Sender {
            tx,
            permits: permits.clone(),
        },
        Receiver { rx, permits },
    )
}

pub struct Sender {
    tx: mpsc::UnboundedSender<MessageWithPermits>,
    permits: Arc<Semaphore>,
}

impl Sender {
    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        let permits = match &message {
            Message::Chunk(c) => c.cardinality().clamp(1, MAX_CHUNK_PERMITS),
            Message::Barrier(_) | Message::Watermark(_) => 0,
        } as u32;
        self.permits.acquire_many(permits).await.unwrap().forget();

        self.tx
            .send(MessageWithPermits { message, permits })
            .map_err(|e| mpsc::error::SendError(e.0.message))
    }
}

pub struct Receiver {
    rx: mpsc::UnboundedReceiver<MessageWithPermits>,
    permits: Arc<Semaphore>,
}

impl Receiver {
    pub async fn recv(&mut self) -> Option<Message> {
        let MessageWithPermits { message, permits } = self.recv_with_permits().await?;
        self.permits.add_permits(permits as usize);
        Some(message)
    }

    pub fn try_recv(&mut self) -> Result<Message, mpsc::error::TryRecvError> {
        let MessageWithPermits { message, permits } = self.rx.try_recv()?;
        self.permits.add_permits(permits as usize);
        Ok(message)
    }

    pub async fn recv_with_permits(&mut self) -> Option<MessageWithPermits> {
        self.rx.recv().await
    }

    pub fn permits(&self) -> Arc<Semaphore> {
        self.permits.clone()
    }
}
