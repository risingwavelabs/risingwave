// Copyright 2024 RisingWave Labs
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

use std::sync::{Arc, LazyLock};

use prometheus::{register_int_gauge_vec_with_registry, IntGauge, IntGaugeVec};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::task_service::PbPermits;
use tokio::sync::{mpsc, AcquireError, Semaphore};

use crate::executor::Message;
use crate::task::UpDownFragmentIds;

/// Message with its required permits.
///
/// We store the `permits` in the struct instead of implying it from the `message` so that the
/// permit number is totally determined by the sender and the downstream only needs to give the
/// `permits` back verbatim, in case the version of the upstream and the downstream are different.
pub struct MessageWithPermits {
    pub message: Message,
    pub permits: Option<PbPermits>,
}

/// Create a channel for the exchange service.
pub fn channel(permits: PbPermits, fragment_ids: UpDownFragmentIds) -> (Sender, Receiver) {
    // Use an unbounded channel since we manage the permits manually.
    let (tx, rx) = mpsc::unbounded_channel();

    let initial_permits = permits.clone();
    let permits = Arc::new(Permits {
        records: Semaphore::new(permits.records as _),
        bytes: Semaphore::new(permits.bytes as _),
        barriers: Semaphore::new(permits.barriers as _),
    });

    let metric_memory_size =
        MEMORY_SIZE.with_label_values(&[&fragment_ids.0.to_string(), &fragment_ids.1.to_string()]);
    let metric_num_rows =
        NUM_ROWS.with_label_values(&[&fragment_ids.0.to_string(), &fragment_ids.1.to_string()]);

    (
        Sender {
            tx,
            permits: permits.clone(),
            initial_permits,
            metric_memory_size: metric_memory_size.clone(),
            metric_num_rows: metric_num_rows.clone(),
        },
        Receiver {
            rx,
            permits,
            metric_memory_size,
            metric_num_rows,
        },
    )
}

static MEMORY_SIZE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec_with_registry!(
        "stream_exchange_memory_size",
        "Total size of memory in the exchange channel",
        &["up_fragment_id", "down_fragment_id"],
        GLOBAL_METRICS_REGISTRY,
    )
    .unwrap()
});

static NUM_ROWS: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    register_int_gauge_vec_with_registry!(
        "stream_exchange_num_rows",
        "Total number of rows in the exchange channel",
        &["up_fragment_id", "down_fragment_id"],
        GLOBAL_METRICS_REGISTRY,
    )
    .unwrap()
});

/// The configuration for tests.
pub mod for_test {
    use super::PbPermits;

    pub const INITIAL_PERMITS: PbPermits = PbPermits {
        records: u32::MAX / 2,
        bytes: tokio::sync::Semaphore::MAX_PERMITS as u64 / 2,
        barriers: 1,
    };
}

pub fn channel_for_test() -> (Sender, Receiver) {
    channel(for_test::INITIAL_PERMITS, (0, 0))
}

/// Semaphore-based permits to control the back-pressure.
///
/// The number of messages in the exchange channel is limited by these semaphores.
pub struct Permits {
    /// The permits for records in chunks.
    records: Semaphore,
    /// The permits for total bytes in chunks.
    bytes: Semaphore,
    /// The permits for barriers.
    barriers: Semaphore,
}

impl Permits {
    /// Add permits back to the semaphores.
    pub fn add_permits(&self, permits: PbPermits) {
        self.records.add_permits(permits.records as usize);
        self.bytes.add_permits(permits.bytes as usize);
        self.barriers.add_permits(permits.barriers as usize);
    }

    /// Acquire permits from the semaphores.
    ///
    /// This function is cancellation-safe except for the fairness of waking.
    async fn acquire_permits(&self, permits: &PbPermits) -> Result<(), AcquireError> {
        self.records
            .acquire_many(permits.records as _)
            .await?
            .forget();
        self.bytes.acquire_many(permits.bytes as _).await?.forget();
        self.barriers
            .acquire_many(permits.barriers as _)
            .await?
            .forget();
        Ok(())
    }

    /// Close the semaphores so that all pending `acquire` will fail immediately.
    fn close(&self) {
        self.records.close();
        self.bytes.close();
        self.barriers.close();
    }
}

/// The sender of the exchange service with permit-based back-pressure.
pub struct Sender {
    tx: mpsc::UnboundedSender<MessageWithPermits>,
    permits: Arc<Permits>,

    /// The maximum permits required by a chunk.
    /// If the permits required by a chunk exceed this, the permits will be capped.
    initial_permits: PbPermits,
    metric_memory_size: IntGauge,
    metric_num_rows: IntGauge,
}

impl Sender {
    /// Send a message, waiting until there are enough permits.
    ///
    /// Returns error if the receive half of the channel is closed, including the message passed.
    pub async fn send(&self, message: Message) -> Result<(), mpsc::error::SendError<Message>> {
        // The semaphores should never be closed.
        let permits = match &message {
            Message::Chunk(c) => Some(PbPermits {
                records: (c.cardinality() as u32).min(self.initial_permits.records),
                bytes: (c.estimated_size() as u64).min(self.initial_permits.bytes),
                barriers: 0,
            }),
            Message::Barrier(_) => Some(PbPermits {
                records: 0,
                bytes: 0,
                barriers: 1,
            }),
            Message::Watermark(_) => None,
        };

        if let Some(permits) = &permits {
            if self.permits.acquire_permits(permits).await.is_err() {
                return Err(mpsc::error::SendError(message));
            }
        }

        if let Message::Chunk(chunk) = &message {
            self.metric_memory_size.add(chunk.estimated_size() as i64);
            self.metric_num_rows.add(chunk.cardinality() as i64);
        }

        self.tx
            .send(MessageWithPermits { message, permits })
            .map_err(|e| mpsc::error::SendError(e.0.message))
    }
}

/// The receiver of the exchange service with permit-based back-pressure.
pub struct Receiver {
    rx: mpsc::UnboundedReceiver<MessageWithPermits>,
    permits: Arc<Permits>,
    metric_memory_size: IntGauge,
    metric_num_rows: IntGauge,
}

impl Receiver {
    /// Receive the next message for this receiver, with the permits of this message added back.
    /// Used for local exchange.
    ///
    /// Returns `None` if the channel has been closed.
    pub async fn recv(&mut self) -> Option<Message> {
        let MessageWithPermits { message, permits } = self.recv_raw().await?;

        if let Some(permits) = permits {
            self.permits.add_permits(permits);
        }
        if let Message::Chunk(chunk) = &message {
            self.metric_memory_size.sub(chunk.estimated_size() as i64);
            self.metric_num_rows.sub(chunk.cardinality() as i64);
        }

        Some(message)
    }

    /// Try to receive the next message for this receiver, with the permits of this message added
    /// back.
    ///
    /// Returns error if the channel is currently empty.
    pub fn try_recv(&mut self) -> Result<Message, mpsc::error::TryRecvError> {
        let MessageWithPermits { message, permits } = self.rx.try_recv()?;

        if let Some(permits) = permits {
            self.permits.add_permits(permits);
        }
        if let Message::Chunk(chunk) = &message {
            self.metric_memory_size.sub(chunk.estimated_size() as i64);
            self.metric_num_rows.sub(chunk.cardinality() as i64);
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

    /// Get a reference to the inner [`Permits`] to manually add permits.
    pub fn permits(&self) -> Arc<Permits> {
        self.permits.clone()
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        // Close the `permits` semaphores so that all pending `acquire` on the sender side will fail
        // immediately.
        self.permits.close();
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::pin::pin;

    use futures::FutureExt;

    use super::*;
    use crate::executor::Barrier;

    #[test]
    fn test_channel_close() {
        let (tx, mut rx) = channel_for_test();

        let send = || {
            tx.send(Message::Barrier(Barrier::with_prev_epoch_for_test(
                514, 114,
            )))
        };

        assert_matches!(send().now_or_never(), Some(Ok(_))); // send successfully
        assert_matches!(rx.recv().now_or_never(), Some(Some(Message::Barrier(_)))); // recv successfully

        assert_matches!(send().now_or_never(), Some(Ok(_))); // send successfully
                                                             // do not recv, so that the channel is full

        let mut send_fut = pin!(send());
        assert_matches!((&mut send_fut).now_or_never(), None); // would block due to no permits
        drop(rx);
        assert_matches!(send_fut.now_or_never(), Some(Err(_))); // channel closed
    }
}
