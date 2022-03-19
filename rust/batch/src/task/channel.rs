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
//
use risingwave_common::array::DataChunk;
use risingwave_common::error::Result;
use risingwave_pb::plan::exchange_info::DistributionMode as ShuffleDistributionMode;
use risingwave_pb::plan::ExchangeInfo;

use crate::task::broadcast_channel::new_broadcast_channel;
use crate::task::fifo_channel::new_fifo_channel;
use crate::task::hash_shuffle_channel::new_hash_shuffle_channel;

#[async_trait::async_trait]
pub trait ChanSender: Send {
    /// This function will block until there's enough resource to process the chunk.
    /// Currently, it will only be called from single thread.
    /// `None` is sent as a mark of the ending of channel.
    async fn send(&mut self, chunk: Option<DataChunk>) -> Result<()>;
}

#[async_trait::async_trait]
pub trait ChanReceiver: Send {
    /// Returns `None` if there's no more data to read.
    /// Otherwise it will wait until there's data.
    async fn recv(&mut self) -> Result<Option<DataChunk>>;
}

pub type BoxChanSender = Box<dyn ChanSender>;
pub type BoxChanReceiver = Box<dyn ChanReceiver>;

/// Output-channel is a synchronous, bounded single-producer-multiple-consumer queue.
/// The producer is the local task executor, the consumer is
/// [`ExchangeService`](risingwave_pb::task_service::exchange_service_server::ExchangeService).
/// The implementation depends on the shuffling strategy.
pub fn create_output_channel(
    shuffle: &ExchangeInfo,
) -> Result<(BoxChanSender, Vec<BoxChanReceiver>)> {
    match shuffle.get_mode()? {
        ShuffleDistributionMode::Single => Ok(new_fifo_channel()),
        ShuffleDistributionMode::Hash => Ok(new_hash_shuffle_channel(shuffle)),
        ShuffleDistributionMode::Broadcast => Ok(new_broadcast_channel(shuffle)),
    }
}
