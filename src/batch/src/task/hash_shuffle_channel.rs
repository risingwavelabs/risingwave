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

use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::ops::BitAnd;
use std::option::Option;

use risingwave_common::array::DataChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::util::hash_util::Crc32FastBuilder;
use risingwave_pb::batch_plan::exchange_info::HashInfo;
use risingwave_pb::batch_plan::*;
use tokio::sync::mpsc;

use crate::error::BatchError::SenderError;
use crate::error::Result as BatchResult;
use crate::task::channel::{ChanReceiver, ChanReceiverImpl, ChanSender, ChanSenderImpl};
use crate::task::data_chunk_in_channel::DataChunkInChannel;
use crate::task::BOUNDED_BUFFER_SIZE;

pub struct HashShuffleSender {
    senders: Vec<mpsc::Sender<Option<DataChunkInChannel>>>,
    hash_info: HashInfo,
}

impl Debug for HashShuffleSender {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashShuffleSender")
            .field("hash_info", &self.hash_info)
            .finish()
    }
}

pub struct HashShuffleReceiver {
    receiver: mpsc::Receiver<Option<DataChunkInChannel>>,
}

fn generate_hash_values(chunk: &DataChunk, hash_info: &HashInfo) -> BatchResult<Vec<usize>> {
    let output_count = hash_info.output_count as usize;

    let hasher_builder = Crc32FastBuilder {};

    let hash_values = chunk
        .get_hash_values(
            &hash_info
                .key
                .iter()
                .map(|idx| *idx as usize)
                .collect::<Vec<_>>(),
            hasher_builder,
        )
        .iter_mut()
        .map(|hash_value| hash_value.hash_code() as usize % output_count)
        .collect::<Vec<_>>();
    Ok(hash_values)
}

/// The returned chunks must have cardinality > 0.
fn generate_new_data_chunks(
    chunk: &DataChunk,
    hash_info: &exchange_info::HashInfo,
    hash_values: &[usize],
) -> Vec<DataChunk> {
    let output_count = hash_info.output_count as usize;
    let mut vis_maps = vec![vec![]; output_count];
    hash_values.iter().for_each(|hash| {
        for (sink_id, vis_map) in vis_maps.iter_mut().enumerate() {
            if *hash == sink_id {
                vis_map.push(true);
            } else {
                vis_map.push(false);
            }
        }
    });
    let mut res = Vec::with_capacity(output_count);
    for (sink_id, vis_map_vec) in vis_maps.into_iter().enumerate() {
        let vis_map: Bitmap = vis_map_vec.into_iter().collect();
        let vis_map = if let Some(visibility) = chunk.get_visibility_ref() {
            vis_map.bitand(visibility)
        } else {
            vis_map
        };
        let new_data_chunk = chunk.with_visibility(vis_map);
        trace!(
            "send to sink:{}, cardinality:{}",
            sink_id,
            new_data_chunk.cardinality()
        );
        res.push(new_data_chunk);
    }
    res
}

impl ChanSender for HashShuffleSender {
    type SendFuture<'a> = impl Future<Output = BatchResult<()>>;

    fn send(&mut self, chunk: Option<DataChunk>) -> Self::SendFuture<'_> {
        async move {
            match chunk {
                Some(c) => self.send_chunk(c).await,
                None => self.send_done().await,
            }
        }
    }
}

impl HashShuffleSender {
    async fn send_chunk(&mut self, chunk: DataChunk) -> BatchResult<()> {
        let hash_values = generate_hash_values(&chunk, &self.hash_info)?;
        let new_data_chunks = generate_new_data_chunks(&chunk, &self.hash_info, &hash_values);

        for (sink_id, new_data_chunk) in new_data_chunks.into_iter().enumerate() {
            trace!(
                "send to sink:{}, cardinality:{}",
                sink_id,
                new_data_chunk.cardinality()
            );
            // The reason we need to add this filter only in HashShuffleSender is that
            // `generate_new_data_chunks` may generate an empty chunk.
            if new_data_chunk.cardinality() > 0 {
                self.senders[sink_id]
                    .send(Some(DataChunkInChannel::new(new_data_chunk)))
                    .await
                    .map_err(|_| SenderError)?
            }
        }
        Ok(())
    }

    async fn send_done(&mut self) -> BatchResult<()> {
        for sender in &self.senders {
            sender.send(None).await.map_err(|_| SenderError)?
        }

        Ok(())
    }
}

impl ChanReceiver for HashShuffleReceiver {
    type RecvFuture<'a> = impl Future<Output = Result<Option<DataChunkInChannel>>>;

    fn recv(&mut self) -> Self::RecvFuture<'_> {
        async move {
            match self.receiver.recv().await {
                Some(data_chunk) => Ok(data_chunk),
                // Early close should be treated as error.
                None => Err(InternalError("broken hash_shuffle_channel".to_string()).into()),
            }
        }
    }
}

pub fn new_hash_shuffle_channel(shuffle: &ExchangeInfo) -> (ChanSenderImpl, Vec<ChanReceiverImpl>) {
    let hash_info = match shuffle.distribution {
        Some(exchange_info::Distribution::HashInfo(ref v)) => v.clone(),
        _ => exchange_info::HashInfo::default(),
    };

    let output_count = hash_info.output_count as usize;
    let mut senders = Vec::with_capacity(output_count);
    let mut receivers = Vec::with_capacity(output_count);
    for _ in 0..output_count {
        let (s, r) = mpsc::channel(BOUNDED_BUFFER_SIZE);
        senders.push(s);
        receivers.push(r);
    }
    let channel_sender = ChanSenderImpl::HashShuffle(HashShuffleSender { senders, hash_info });
    let channel_receivers = receivers
        .into_iter()
        .map(|receiver| ChanReceiverImpl::HashShuffle(HashShuffleReceiver { receiver }))
        .collect::<Vec<_>>();
    (channel_sender, channel_receivers)
}
