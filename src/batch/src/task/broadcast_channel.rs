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
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::array::DataChunk;
use risingwave_pb::batch_plan::exchange_info::BroadcastInfo;
use risingwave_pb::batch_plan::*;
use tokio::sync::mpsc;

use crate::error::BatchError::{Internal, SenderError};
use crate::error::{BatchError, BatchSharedResult, Result as BatchResult};
use crate::task::channel::{ChanReceiver, ChanReceiverImpl, ChanSender, ChanSenderImpl};
use crate::task::data_chunk_in_channel::DataChunkInChannel;

/// `BroadcastSender` sends the same chunk to a number of `BroadcastReceiver`s.
#[derive(Clone)]
pub struct BroadcastSender {
    senders: Vec<mpsc::Sender<BatchSharedResult<Option<DataChunkInChannel>>>>,
    broadcast_info: BroadcastInfo,
}

impl Debug for BroadcastSender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastSender")
            .field("broadcast_info", &self.broadcast_info)
            .finish()
    }
}

impl ChanSender for BroadcastSender {
    async fn send(&mut self, chunk: DataChunk) -> BatchResult<()> {
        let broadcast_data_chunk = DataChunkInChannel::new(chunk);
        for sender in &self.senders {
            sender
                .send(Ok(Some(broadcast_data_chunk.clone())))
                .await
                .map_err(|_| SenderError)?
        }

        Ok(())
    }

    async fn close(self, error: Option<Arc<BatchError>>) -> BatchResult<()> {
        for sender in self.senders {
            sender
                .send(error.clone().map(Err).unwrap_or(Ok(None)))
                .await
                .map_err(|_| SenderError)?
        }

        Ok(())
    }
}

/// One or more `BroadcastReceiver`s corresponds to a single `BroadcastReceiver`
pub struct BroadcastReceiver {
    receiver: mpsc::Receiver<BatchSharedResult<Option<DataChunkInChannel>>>,
}

impl ChanReceiver for BroadcastReceiver {
    async fn recv(&mut self) -> BatchSharedResult<Option<DataChunkInChannel>> {
        match self.receiver.recv().await {
            Some(data_chunk) => data_chunk,
            // Early close should be treated as an error.
            None => Err(Arc::new(Internal(anyhow!("broken broadcast_channel")))),
        }
    }
}

pub fn new_broadcast_channel(
    shuffle: &ExchangeInfo,
    output_channel_size: usize,
) -> (ChanSenderImpl, Vec<ChanReceiverImpl>) {
    let broadcast_info = match shuffle.distribution {
        Some(exchange_info::Distribution::BroadcastInfo(ref v)) => v.clone(),
        _ => BroadcastInfo::default(),
    };

    let output_count = broadcast_info.count as usize;
    let mut senders = Vec::with_capacity(output_count);
    let mut receivers = Vec::with_capacity(output_count);
    for _ in 0..output_count {
        let (s, r) = mpsc::channel(output_channel_size);
        senders.push(s);
        receivers.push(r);
    }
    let channel_sender = ChanSenderImpl::Broadcast(BroadcastSender {
        senders,
        broadcast_info,
    });
    let channel_receivers = receivers
        .into_iter()
        .map(|receiver| ChanReceiverImpl::Broadcast(BroadcastReceiver { receiver }))
        .collect::<Vec<_>>();
    (channel_sender, channel_receivers)
}
