// Copyright 2025 RisingWave Labs
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
use std::time::Duration;

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::row::Row;
use risingwave_common_rate_limit::RateLimiter;
use risingwave_connector::AVAILABLE_ULIMIT_NOFILE;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::source::{
    BoxSourceChunkStream, BoxStreamingFileSourceChunkStream, SourceColumnDesc, SplitId,
};
use risingwave_pb::plan_common::AdditionalColumn;
use risingwave_pb::plan_common::additional_column::ColumnType;
pub use state_table_handler::*;

use crate::executor::StreamExecutorResult;

mod executor_core;
pub use executor_core::StreamSourceCore;

mod reader_stream;

mod source_executor;
pub use source_executor::*;
mod source_backfill_executor;
pub use source_backfill_executor::*;
mod fs_list_executor;
pub use fs_list_executor::*;
mod fs_fetch_executor;
pub use fs_fetch_executor::*;
mod iceberg_list_executor;
pub use iceberg_list_executor::*;
mod iceberg_fetch_executor;
pub use iceberg_fetch_executor::*;

mod source_backfill_state_table;
pub(crate) use source_backfill_state_table::BackfillStateTableHandler;

pub mod state_table_handler;
use futures_async_stream::try_stream;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

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
        split_offset_mapping.insert(split_id, offset.to_owned());
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
pub async fn apply_rate_limit(stream: BoxSourceChunkStream, rate_limit_rps: Option<u32>) {
    if rate_limit_rps == Some(0) {
        // block the stream until the rate limit is reset
        let future = futures::future::pending::<()>();
        future.await;
        unreachable!();
    }

    let limiter = RateLimiter::new(
        rate_limit_rps
            .inspect(|limit| tracing::info!(rate_limit = limit, "rate limit applied"))
            .into(),
    );

    #[for_await]
    for chunk in stream {
        let chunk = chunk?;
        yield process_chunk(chunk, rate_limit_rps, &limiter).await;
    }
}

#[try_stream(ok = Option<StreamChunk>, error = ConnectorError)]
pub async fn apply_rate_limit_with_for_streaming_file_source_reader(
    stream: BoxStreamingFileSourceChunkStream,
    rate_limit_rps: Option<u32>,
) {
    if rate_limit_rps == Some(0) {
        // block the stream until the rate limit is reset
        let future = futures::future::pending::<()>();
        future.await;
        unreachable!();
    }

    let limiter = RateLimiter::new(
        rate_limit_rps
            .inspect(|limit| tracing::info!(rate_limit = limit, "rate limit applied"))
            .into(),
    );

    #[for_await]
    for chunk in stream {
        let chunk_option = chunk?;
        match chunk_option {
            Some(chunk) => {
                let processed_chunk = process_chunk(chunk, rate_limit_rps, &limiter).await;
                yield Some(processed_chunk);
            }
            None => yield None,
        }
    }
}

async fn process_chunk(
    chunk: StreamChunk,
    rate_limit_rps: Option<u32>,
    limiter: &RateLimiter,
) -> StreamChunk {
    let chunk_size = chunk.capacity();

    if rate_limit_rps.is_none() || chunk_size == 0 {
        // no limit, or empty chunk
        return chunk;
    }

    let limit = rate_limit_rps.unwrap() as u64;
    let required_permits = chunk.compute_rate_limit_chunk_permits();
    if required_permits > limit {
        // This should not happen after the mentioned PR.
        tracing::error!(
            chunk_size,
            required_permits,
            limit,
            "unexpected large chunk size"
        );
    }

    limiter.wait(required_permits).await;
    chunk
}

pub fn get_infinite_backoff_strategy() -> impl Iterator<Item = Duration> {
    const BASE_DELAY: Duration = Duration::from_secs(1);
    const BACKOFF_FACTOR: u64 = 2;
    const MAX_DELAY: Duration = Duration::from_secs(10);
    ExponentialBackoff::from_millis(BASE_DELAY.as_millis() as u64)
        .factor(BACKOFF_FACTOR)
        .max_delay(MAX_DELAY)
        .map(jitter)
}

pub async fn check_fd_limit(barrier: &Barrier) -> StreamExecutorResult<()> {
    if let Some(extra_info) = barrier.get_connector_extra_info()
        && let Some(kafka_broker_size) = extra_info.kafka_broker_size
        && AVAILABLE_ULIMIT_NOFILE.load(std::sync::atomic::Ordering::SeqCst) != -1
    {
        tracing::debug!(
            "check_fd_limit: kafka_broker_size: {}, available_ulimit_nofile: {}",
            kafka_broker_size,
            AVAILABLE_ULIMIT_NOFILE.load(std::sync::atomic::Ordering::SeqCst)
        );
        // test and set `AVAILABLE_ULIMIT_NOFILE` and make the operation atomic
        // if AVAILABLE_ULIMIT_NOFILE - 2 * kafka_broker_size < 0, return error
        if AVAILABLE_ULIMIT_NOFILE
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |current| {
                    if current - 3 * (kafka_broker_size as i64) < 0 {
                        None
                    } else {
                        Some(current - 3 * kafka_broker_size as i64)
                    }
                },
            )
            .is_err()
        {
            return Err(StreamExecutorError::connector_error(anyhow!(
                "not enough file descriptors, please consider increasing nodes."
            )));
        }
    }
    Ok(())
}
