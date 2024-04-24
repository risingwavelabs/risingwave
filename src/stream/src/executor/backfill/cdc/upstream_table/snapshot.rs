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

use std::future::Future;
use std::num::NonZeroU32;

use futures::{pin_mut, Stream};
use futures_async_stream::try_stream;
use governor::clock::MonotonicClock;
use governor::{Quota, RateLimiter};
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::source::cdc::external::{CdcOffset, ExternalTableReader};

use super::external::ExternalStorageTable;
use crate::common::rate_limit::limited_chunk_size;
use crate::executor::backfill::utils::iter_chunks;
use crate::executor::{StreamExecutorError, StreamExecutorResult, INVALID_EPOCH};

pub trait UpstreamTableRead {
    fn snapshot_read(
        &self,
        args: SnapshotReadArgs,
        limit: u32,
    ) -> impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send + '_;

    fn current_binlog_offset(
        &self,
    ) -> impl Future<Output = StreamExecutorResult<Option<CdcOffset>>> + Send + '_;
}

#[derive(Debug, Default)]
pub struct SnapshotReadArgs {
    pub epoch: u64,
    pub current_pos: Option<OwnedRow>,
    pub ordered: bool,
    pub rate_limit_rps: Option<u32>,
}

impl SnapshotReadArgs {
    pub fn new_for_cdc(current_pos: Option<OwnedRow>, rate_limit_rps: Option<u32>) -> Self {
        Self {
            epoch: INVALID_EPOCH,
            current_pos,
            ordered: false,
            rate_limit_rps,
        }
    }
}

/// A wrapper of upstream table for snapshot read
/// because we need to customize the snapshot read for managed upstream table (e.g. mv, index)
/// and external upstream table.
pub struct UpstreamTableReader<T> {
    inner: T,
}

impl<T> UpstreamTableReader<T> {
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn new(table: T) -> Self {
        Self { inner: table }
    }
}

impl UpstreamTableRead for UpstreamTableReader<ExternalStorageTable> {
    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read(&self, args: SnapshotReadArgs, read_limit: u32) {
        let primary_keys = self
            .inner
            .pk_indices()
            .iter()
            .map(|idx| {
                let f = &self.inner.schema().fields[*idx];
                f.name.clone()
            })
            .collect_vec();

        tracing::debug!(
            "snapshot_read primary keys: {:?}, current_pos: {:?}",
            primary_keys,
            args.current_pos
        );

        let row_stream = self.inner.table_reader().snapshot_read(
            self.inner.schema_table_name(),
            args.current_pos,
            primary_keys,
            read_limit,
        );

        pin_mut!(row_stream);

        let mut builder = DataChunkBuilder::new(
            self.inner.schema().data_types(),
            limited_chunk_size(args.rate_limit_rps),
        );
        let chunk_stream = iter_chunks(row_stream, &mut builder);

        if args.rate_limit_rps == Some(0) {
            // If limit is 0, we should not read any data from the upstream table.
            // Keep waiting util the stream is rebuilt.
            let future = futures::future::pending::<()>();
            future.await;
            unreachable!();
        }

        let limiter = args.rate_limit_rps.map(|limit| {
            tracing::info!(rate_limit = limit, "rate limit applied");
            RateLimiter::direct_with_clock(
                Quota::per_second(NonZeroU32::new(limit).unwrap()),
                &MonotonicClock,
            )
        });

        #[for_await]
        for chunk in chunk_stream {
            let chunk = chunk?;
            let chunk_size = chunk.capacity();

            if args.rate_limit_rps.is_none() || chunk_size == 0 {
                // no limit, or empty chunk
                yield Some(chunk);
                continue;
            }

            // Apply rate limit, see `risingwave_stream::executor::source::apply_rate_limit` for more.
            // May be should be refactored to a common function later.
            let limiter = limiter.as_ref().unwrap();
            let limit = args.rate_limit_rps.unwrap();

            // Because we produce chunks with limited-sized data chunk builder and all rows
            // are `Insert`s, the chunk size should never exceed the limit.
            assert!(chunk_size <= limit as usize);

            // `InsufficientCapacity` should never happen because we have check the cardinality
            limiter
                .until_n_ready(NonZeroU32::new(chunk_size as u32).unwrap())
                .await
                .unwrap();
            yield Some(chunk);
        }

        yield None;
    }

    async fn current_binlog_offset(&self) -> StreamExecutorResult<Option<CdcOffset>> {
        let binlog = self.inner.table_reader().current_cdc_offset();
        let binlog = binlog.await?;
        Ok(Some(binlog))
    }
}
