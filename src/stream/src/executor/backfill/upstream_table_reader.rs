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

use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::row;
use risingwave_common::row::OwnedRow;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::get_second;
use risingwave_storage::StateStore;

use crate::executor::backfill::external_table::ExternalStorageTable;
use crate::executor::backfill::utils::{compute_bounds, iter_chunks};
use crate::executor::StreamExecutorResult;

#[derive(Debug, Default)]
pub struct SnapshotReadArgs {
    pub epoch: u64,
    pub current_pos: Option<OwnedRow>,
    pub ordered: bool,
    pub chunk_size: usize,
}

impl SnapshotReadArgs {
    pub fn new(
        epoch: u64,
        current_pos: Option<OwnedRow>,
        ordered: bool,
        chunk_size: usize,
    ) -> Self {
        Self {
            epoch,
            current_pos,
            ordered,
            chunk_size,
        }
    }
}

pub trait UpstreamTable {
    fn identity(&self) -> &str;
}

/// A wrapper of upstream table for snapshot read
pub struct UpstreamTableReader<T: UpstreamTable> {
    inner: T,
}

impl<T: UpstreamTable> UpstreamTableReader<T> {
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn new(table: T) -> Self {
        Self { inner: table }
    }
}

impl<S: StateStore> UpstreamTable for StorageTable<S> {
    fn identity(&self) -> &str {
        "StorageTable"
    }
}

impl<S: StateStore> UpstreamTable for ExternalStorageTable<S> {
    fn identity(&self) -> &str {
        "ExternalStorageTable"
    }
}

pub trait UpstreamSnapshotRead {
    type SnapshotStream<'a>: Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send + 'a
    where
        Self: 'a;

    fn snapshot_read(&self, args: SnapshotReadArgs) -> Self::SnapshotStream<'_>;
}

pub trait UpstreamBinlogOffsetRead {
    fn current_binlog_offset(&self) -> Option<String>;
}

// TODO: we can customize the snapshot read for different kind of table
impl<S: StateStore> UpstreamSnapshotRead for UpstreamTableReader<StorageTable<S>> {
    type SnapshotStream<'a> = impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + 'a;

    fn snapshot_read(&self, args: SnapshotReadArgs) -> Self::SnapshotStream<'_> {
        #[try_stream]
        async move {
            let range_bounds = compute_bounds(self.inner.pk_indices(), args.current_pos);
            let range_bounds = match range_bounds {
                None => {
                    yield None;
                    return Ok(());
                }
                Some(range_bounds) => range_bounds,
            };

            // We use uncommitted read here, because we have already scheduled the
            // `BackfillExecutor` together with the upstream mv.
            let iter = self
                .inner
                .batch_iter_with_pk_bounds(
                    HummockReadEpoch::NoWait(args.epoch),
                    row::empty(),
                    range_bounds,
                    args.ordered,
                    PrefetchOptions::new_for_exhaust_iter(),
                )
                .await?
                .map(get_second);

            pin_mut!(iter);

            #[for_await]
            for chunk in iter_chunks(iter, self.inner.schema(), args.chunk_size) {
                yield chunk?;
            }
        }
    }
}

impl<S: StateStore> UpstreamSnapshotRead for UpstreamTableReader<ExternalStorageTable<S>> {
    type SnapshotStream<'a> = impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + 'a;

    fn snapshot_read(&self, _args: SnapshotReadArgs) -> Self::SnapshotStream<'_> {
        #[try_stream]
        async move {
            yield None;
        }
    }
}

impl<S: StateStore> UpstreamBinlogOffsetRead for UpstreamTableReader<ExternalStorageTable<S>> {
    fn current_binlog_offset(&self) -> Option<String> {
        todo!()
    }
}
