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

use std::collections::HashMap;
use std::num::NonZeroU32;

use await_tree::InstrumentAwait;
use governor::clock::MonotonicClock;
use governor::{Quota, RateLimiter};
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::row::Row;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::source::{BoxChunkSourceStream, SourceColumnDesc, SplitId};
use risingwave_pb::plan_common::additional_column::ColumnType;
use risingwave_pb::plan_common::AdditionalColumn;
pub use state_table_handler::*;

mod executor_core;
pub use executor_core::StreamSourceCore;

mod fs_source_executor;
#[expect(deprecated)]
pub use fs_source_executor::*;
mod source_executor;
pub use source_executor::*;
mod source_backfill_executor;
pub use source_backfill_executor::*;
mod list_executor;
pub use list_executor::*;
mod fetch_executor;
pub use fetch_executor::*;

mod source_backfill_state_table;
pub use source_backfill_state_table::BackfillStateTableHandler;

pub mod state_table_handler;
use futures_async_stream::try_stream;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::error::StreamExecutorError;
use crate::executor::{Barrier, Message};

/// Receive barriers from barrier manager with the channel, error on channel close.
#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn barrier_to_message_stream(mut rx: UnboundedReceiver<Barrier>) {
    while let Some(barrier) = rx.recv().instrument_await("receive_barrier").await {
        yield Message::Barrier(barrier);
    }
    bail!("barrier reader closed unexpectedly");
}

pub fn get_split_offset_mapping_from_chunk(
    chunk: &StreamChunk,
    split_idx: usize,
    offset_idx: usize,
) -> Option<HashMap<SplitId, String>> {
    let mut split_offset_mapping = HashMap::new();
    // All rows (including those visible or invisible) will be used to update the source offset.
    for i in 0..chunk.capacity() {
        let (_, row, _) = chunk.row_at(i);
        let split_id = row.datum_at(split_idx).unwrap().into_utf8().into();
        let offset = row.datum_at(offset_idx).unwrap().into_utf8();
        split_offset_mapping.insert(split_id, offset.to_string());
    }
    Some(split_offset_mapping)
}

pub fn get_split_offset_col_idx(
    column_descs: &[SourceColumnDesc],
) -> (Option<usize>, Option<usize>) {
    let mut split_idx = None;
    let mut offset_idx = None;
    for (idx, column) in column_descs.iter().enumerate() {
        match column.additional_column {
            AdditionalColumn {
                column_type: Some(ColumnType::Partition(_) | ColumnType::Filename(_)),
            } => {
                split_idx = Some(idx);
            }
            AdditionalColumn {
                column_type: Some(ColumnType::Offset(_)),
            } => {
                offset_idx = Some(idx);
            }
            _ => (),
        }
    }
    (split_idx, offset_idx)
}

pub fn prune_additional_cols(
    chunk: &StreamChunk,
    split_idx: usize,
    offset_idx: usize,
    column_descs: &[SourceColumnDesc],
) -> StreamChunk {
    chunk.project(
        &(0..chunk.dimension())
            .filter(|&idx| {
                (idx != split_idx && idx != offset_idx) || column_descs[idx].is_visible()
            })
            .collect_vec(),
    )
}

#[try_stream(ok = StreamChunk, error = ConnectorError)]
pub async fn apply_rate_limit(stream: BoxChunkSourceStream, rate_limit_rps: Option<u32>) {
    if let Some(limit) = rate_limit_rps
        && limit == 0
    {
        // block the stream until the rate limit is reset
        let future = futures::future::pending::<()>();
        future.await;
    }
    let get_rate_limiter = |rate_limit: u32| {
        let quota = Quota::per_second(NonZeroU32::new(rate_limit).unwrap());
        let clock = MonotonicClock;
        RateLimiter::direct_with_clock(quota, &clock)
    };
    let limiter = rate_limit_rps.map(get_rate_limiter);
    if rate_limit_rps.is_some() {
        tracing::info!(rate_limit = ?rate_limit_rps, "applied rate limit");
    }
    #[for_await]
    for batch in stream {
        let chunk: StreamChunk = batch?;
        let chunk_cardinality = chunk.cardinality();
        let Some(n) = NonZeroU32::new(chunk_cardinality as u32) else {
            // pass empty chunk
            yield chunk;
            continue;
        };
        if let Some(limiter) = &limiter {
            let limit = NonZeroU32::new(rate_limit_rps.unwrap()).unwrap();
            if n <= limit {
                // `InsufficientCapacity` should never happen because we have done the check
                limiter.until_n_ready(n).await.unwrap();
                yield chunk;
            } else {
                // Cut the chunk into smaller chunks
                for chunk in chunk.split(limit.get() as usize) {
                    let n = NonZeroU32::new(chunk.cardinality() as u32).unwrap();
                    // Ditto.
                    limiter.until_n_ready(n).await.unwrap();
                    yield chunk;
                }
            }
        } else {
            yield chunk;
        }
    }
}
