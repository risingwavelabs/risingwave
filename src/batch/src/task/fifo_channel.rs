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

use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::array::DataChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use tokio::sync::mpsc;

use crate::error::BatchError::{Internal, SenderError};
use crate::error::{BatchError, BatchSharedResult, Result as BatchResult};
use crate::task::channel::{ChanReceiver, ChanReceiverImpl, ChanSender, ChanSenderImpl};
use crate::task::data_chunk_in_channel::DataChunkInChannel;
#[derive(Clone)]
pub struct FifoSender {
    sender: mpsc::Sender<BatchSharedResult<Option<DataChunkInChannel>>>,
}

impl Debug for FifoSender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FifoSender").finish()
    }
}

pub struct FifoReceiver {
    receiver: mpsc::Receiver<BatchSharedResult<Option<DataChunkInChannel>>>,
}

impl ChanSender for FifoSender {
    async fn send(&mut self, chunk: DataChunk) -> BatchResult<()> {
        let data = DataChunkInChannel::new(chunk);
        self.sender
            .send(Ok(Some(data)))
            .await
            .map_err(|_| SenderError)
    }

    async fn close(self, error: Option<Arc<BatchError>>) -> BatchResult<()> {
        let result = error.map(|e| Err(e)).unwrap_or(Ok(None));
        self.sender.send(result).await.map_err(|_| SenderError)
    }
}

impl ChanReceiver for FifoReceiver {
    async fn recv(&mut self) -> BatchSharedResult<Option<DataChunkInChannel>> {
        match self.receiver.recv().await {
            Some(data_chunk) => data_chunk,
            // Early close should be treated as error.
            None => Err(Arc::new(Internal(anyhow!("broken fifo_channel")))),
        }
    }
}

pub fn new_fifo_channel(output_channel_size: usize) -> (ChanSenderImpl, Vec<ChanReceiverImpl>) {
    let (s, r) = mpsc::channel(output_channel_size);
    (
        ChanSenderImpl::Fifo(FifoSender { sender: s }),
        vec![ChanReceiverImpl::Fifo(FifoReceiver { receiver: r })],
    )
}

mod tests {
    #[tokio::test]
    async fn test_recv_not_fail_on_closed_channel() {
        use crate::task::fifo_channel::new_fifo_channel;

        let (sender, mut receivers) = new_fifo_channel(64);
        assert_eq!(receivers.len(), 1);
        drop(sender);

        let receiver = receivers.get_mut(0).unwrap();
        assert!(receiver.recv().await.is_err());
    }
}
