use std::future::Future;

use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::row;
use risingwave_common::row::OwnedRow;
use risingwave_connector::source::external::{BinlogOffset, ExternalTableReader};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::get_second;
use risingwave_storage::StateStore;

use crate::executor::backfill::upstream_table::external::ExternalStorageTable;
use crate::executor::backfill::utils::{compute_bounds, iter_chunks};
use crate::executor::{StreamExecutorResult, INVALID_EPOCH};

pub trait UpstreamTableRead {
    type BinlogOffsetFuture<'a>: Future<Output = StreamExecutorResult<Option<BinlogOffset>>>
        + Send
        + 'a
    where
        Self: 'a;
    type SnapshotStream<'a>: Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + Send + 'a
    where
        Self: 'a;

    fn snapshot_read(&self, args: SnapshotReadArgs) -> Self::SnapshotStream<'_>;

    fn current_binlog_offset(&self) -> Self::BinlogOffsetFuture<'_>;
}

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

    pub fn new_for_cdc(current_pos: Option<OwnedRow>, chunk_size: usize) -> Self {
        Self {
            epoch: INVALID_EPOCH,
            current_pos,
            ordered: false,
            chunk_size,
        }
    }
}

/// A wrapper of upstream table for snapshot read
/// becasue we need to customize the snapshot read for managed upsream table (e.g. mv, index)
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

impl<S: StateStore> UpstreamTableRead for UpstreamTableReader<StorageTable<S>> {
    type BinlogOffsetFuture<'a> =
        impl Future<Output = StreamExecutorResult<Option<BinlogOffset>>> + 'a;
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

    fn current_binlog_offset(&self) -> Self::BinlogOffsetFuture<'_> {
        async move { Ok(None) }
    }
}

impl UpstreamTableRead for UpstreamTableReader<ExternalStorageTable> {
    type BinlogOffsetFuture<'a> =
        impl Future<Output = StreamExecutorResult<Option<BinlogOffset>>> + 'a;
    type SnapshotStream<'a> = impl Stream<Item = StreamExecutorResult<Option<StreamChunk>>> + 'a;

    fn snapshot_read(&self, args: SnapshotReadArgs) -> Self::SnapshotStream<'_> {
        #[try_stream]
        async move {
            let primary_keys = self
                .inner
                .pk_indices()
                .iter()
                .map(|idx| {
                    let f = &self.inner.schema().fields[*idx];
                    f.name.clone()
                })
                .collect_vec();

            tracing::debug!("snapshot_read primary keys: {:?}", primary_keys);

            let row_stream = self.inner.table_reader().snapshot_read(
                self.inner.schema_table_name(),
                args.current_pos,
                primary_keys,
            );

            pin_mut!(row_stream);
            let chunk_stream = iter_chunks(row_stream, self.inner.schema(), args.chunk_size);
            #[for_await]
            for chunk in chunk_stream {
                yield chunk?;
            }
        }
    }

    fn current_binlog_offset(&self) -> Self::BinlogOffsetFuture<'_> {
        async move {
            let binlog = self.inner.table_reader().current_binlog_offset();
            let binlog = binlog.await?;
            Ok(Some(binlog))
        }
    }
}
