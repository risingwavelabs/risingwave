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

use std::future::Future;

use risingwave_common::array::DataChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use tokio::sync::mpsc;

use crate::task::channel::{ChanReceiver, ChanReceiverImpl, ChanSender, ChanSenderImpl};
use crate::task::data_chunk_in_channel::DataChunkInChannel;
use crate::task::BOUNDED_BUFFER_SIZE;

pub struct FifoSender {
    sender: mpsc::Sender<Option<DataChunkInChannel>>,
}

pub struct FifoReceiver {
    receiver: mpsc::Receiver<Option<DataChunkInChannel>>,
}

impl ChanSender for FifoSender {
    type SendFuture<'a> = impl Future<Output = Result<()>>;

    fn send(&mut self, chunk: Option<DataChunk>) -> Self::SendFuture<'_> {
        async move {
            self.sender
                .send(chunk.map(DataChunkInChannel::Normal))
                .await
                .to_rw_result_with(|| "FifoSender::send".into())
        }
    }
}

impl ChanReceiver for FifoReceiver {
    type RecvFuture<'a> = impl Future<Output = Result<Option<DataChunkInChannel>>>;

    fn recv(&mut self) -> Self::RecvFuture<'_> {
        async move {
            match self.receiver.recv().await {
                Some(data_chunk) => Ok(data_chunk),
                // Early close should be treated as error.
                None => Err(InternalError("broken fifo_channel".to_string()).into()),
            }
        }
    }
}

pub fn new_fifo_channel() -> (ChanSenderImpl, Vec<ChanReceiverImpl>) {
    let (s, r) = mpsc::channel(BOUNDED_BUFFER_SIZE);
    (
        ChanSenderImpl::Fifo(FifoSender { sender: s }),
        vec![ChanReceiverImpl::Fifo(FifoReceiver { receiver: r })],
    )
}

mod tests {
    #[tokio::test]
    async fn test_recv_not_fail_on_closed_channel() {
        use crate::task::fifo_channel::new_fifo_channel;

        let (sender, mut receivers) = new_fifo_channel();
        assert_eq!(receivers.len(), 1);
        drop(sender);

        let receiver = receivers.get_mut(0).unwrap();
        assert!(receiver.recv().await.is_err());
    }
}
