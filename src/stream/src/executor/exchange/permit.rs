// Copyright 2023 RisingWave Labs
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

//! Channel implementation for permit-based back-pressure.

use std::sync::Arc;

use tokio::sync::{mpsc, Semaphore};

use crate::executor::Message;

pub type Permits = u32;

/// Message with its required permits.
///
/// We store the `permits` in the struct instead of implying it from the `message` so that the
/// permit number is totally determined by the sender and the downstream only needs to give the
/// `permits` back verbatim, in case the version of the upstream and the downstream are different.
pub struct MessageWithPermits {
    pub message: Message,
    pub permits: Permits,
}

const BARRIER_PERMITS: usize = 2;

/// Create a channel for the exchange service
pub fn channel(initial_permits: usize, batched_permits: usize) -> (Sender, Receiver) {
    // Use an unbounded channel since we manage the permits manually.
    let (tx, rx) = mpsc::unbounded_channel();

    let permits = Arc::new(Semaphore::new(initial_permits));
    let max_chunk_permits: usize = initial_permits - batched_permits;

    let barrier_permits = Arc::new(Semaphore::new(BARRIER_PERMITS));
    (
        Sender {
            tx,
            permits: permits.clone(),
            barrier_permits: barrier_permits.clone(),
            max_chunk_permits,
        },
        Receiver {
            rx,
            permits,
            barrier_permits,
        },
    )
}

pub fn channel_for_test() -> (Sender, Receiver) {
    // Use an unbounded channel since we manage the permits manually.
    let (tx, rx) = mpsc::unbounded_channel();

    const INITIAL_PERMITS: usize = 8192;
    const BATCHED_PERMITS: usize = 1024;
    let permits = Arc::new(Semaphore::new(INITIAL_PERMITS));
    let max_chunk_permits: usize = INITIAL_PERMITS - BATCHED_PERMITS;

    let barrier_permits = Arc::new(Semaphore::new(BARRIER_PERMITS));
    (
        Sender {
            tx,
            permits: permits.clone(),
            barrier_permits: barrier_permits.clone(),
            max_chunk_permits,
        },
        Receiver {
            rx,
            permits,
            barrier_permits,
        },
    )
}

/// The sender of the exchange service with permit-based back-pressure.
pub struct Sender {
    tx: mpsc::UnboundedSender<MessageWithPermits>,
    permits: Arc<Semaphore>,
    barrier_permits: Arc<Semaphore>,
    /// The maximum permits required by a chunk. If there're too many rows in a chunk, we only
    /// acquire these permits. [`BATCHED_PERMITS`] is subtracted to avoid deadlock with
    /// batching.
    max_chunk_permits: usize,
}

impl Sender {
    /// Send a message, waiting until there are enough permits.
    ///
    /// Returns error if the receive half of the channel is closed, including the message passed.
    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        let permits = match &message {
            Message::Chunk(c) => {
                let p = c.cardinality().clamp(1, self.max_chunk_permits);
                if p == self.max_chunk_permits {
                    tracing::warn!(cardinality = c.cardinality(), "large chunk in exchange")
                }
                p
            }
            Message::Barrier(_) | Message::Watermark(_) => 0,
        } as Permits;

        // The semaphore should never be closed.
        if permits > 0 {
            self.permits.acquire_many(permits).await.unwrap().forget();
        }

        if message.as_barrier().is_some() {
            self.barrier_permits.acquire().await.unwrap().forget();
        }

        self.tx
            .send(MessageWithPermits { message, permits })
            .map_err(|e| mpsc::error::SendError(e.0.message))
    }
}

/// The receiver of the exchange service with permit-based back-pressure.
pub struct Receiver {
    rx: mpsc::UnboundedReceiver<MessageWithPermits>,
    permits: Arc<Semaphore>,
    barrier_permits: Arc<Semaphore>,
}

impl Receiver {
    /// Receive the next message for this receiver, with the permits of this message added back.
    /// Used for local exchange.
    ///
    /// Returns `None` if the channel has been closed.
    pub async fn recv(&mut self) -> Option<Message> {
        let MessageWithPermits { message, permits } = self.recv_raw().await?;
        self.permits.add_permits(permits as usize);
        if message.as_barrier().is_some() {
            self.barrier_permits.add_permits(1);
        }
        Some(message)
    }

    /// Try to receive the next message for this receiver, with the permits of this message added
    /// back.
    ///
    /// Returns error if the channel is currently empty.
    pub fn try_recv(&mut self) -> Result<Message, mpsc::error::TryRecvError> {
        let MessageWithPermits { message, permits } = self.rx.try_recv()?;
        self.permits.add_permits(permits as usize);
        if message.as_barrier().is_some() {
            self.barrier_permits.add_permits(1);
        }
        Ok(message)
    }

    /// Receive the next message and its permits for this receiver, **without** adding the permits
    /// back. Used for remote exchange where the permits should be manually added according to the
    /// downstream actor.
    ///
    /// Returns `None` if the channel has been closed.
    pub async fn recv_raw(&mut self) -> Option<MessageWithPermits> {
        self.rx.recv().await
    }

    /// Get a reference to the `permits` semaphore.
    pub fn permits(&self) -> Arc<Semaphore> {
        self.permits.clone()
    }
}
