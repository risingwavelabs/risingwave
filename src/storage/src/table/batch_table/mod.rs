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

use async_trait::async_trait;
use futures::pin_mut;
use futures_async_stream::try_stream;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::row;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common::util::value_encoding::EitherSerde;
use risingwave_hummock_sdk::HummockReadEpoch;

use crate::store::PrefetchOptions;
use crate::table::batch_table::external_table::ExternalUpstreamTable;
use crate::table::batch_table::storage_table::StorageTable;
use crate::table::get_second;
use crate::StateStore;

pub mod external_table;
pub mod storage_table;

#[derive(Debug, Default)]
pub struct SnapshotReadArgs {
    epoch: u64,
    current_pos: Option<OwnedRow>,
    ordered: bool,
    chunk_size: usize,
}

#[async_trait]
pub trait SnapshotRead {
    // TODO: use GAT to make this method async return a future
    async fn snapshot_read(&self, args: SnapshotReadArgs);
}

pub trait UpstreamTable {
    fn pk_serializer(&self) -> &OrderedRowSerde;

    fn schema(&self) -> &Schema;

    fn pk_indices(&self) -> &[usize];

    fn output_indices(&self) -> &[usize];

    fn pk_in_output_indices(&self) -> Option<Vec<usize>>;

    fn table_id(&self) -> TableId;
}

#[async_trait]
trait UpstreamCdcTable {
    async fn get_current_wal_offset(&self) -> Option<String>;
}

/// Used in backfill executor
pub struct UpstreamTableFacade<T: UpstreamTable> {
    inner: T,
}

impl<T: UpstreamTable> UpstreamTableFacade<T> {
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T: UpstreamTable> UpstreamTable for UpstreamTableFacade<T> {
    fn pk_serializer(&self) -> &OrderedRowSerde {
        self.inner().pk_serializer()
    }

    fn schema(&self) -> &Schema {
        self.inner().schema()
    }

    fn pk_indices(&self) -> &[usize] {
        self.inner().pk_indices()
    }

    fn output_indices(&self) -> &[usize] {
        self.inner().output_indices()
    }

    fn pk_in_output_indices(&self) -> Option<Vec<usize>> {
        self.inner().pk_in_output_indices()
    }

    fn table_id(&self) -> TableId {
        self.inner().table_id()
    }
}

// TODO: we can customize the snapshot read for different kind of table
impl<S: StateStore> SnapshotRead for UpstreamTableFacade<StorageTable<S>> {
    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read(&self, args: SnapshotReadArgs) {
        let range_bounds = compute_bounds(self.inner.pk_indices(), current_pos);
        let range_bounds = match range_bounds {
            None => {
                yield None;
                return Ok(());
            }
            Some(range_bounds) => range_bounds,
        };

        // We use uncommitted read here, because we have already scheduled the `BackfillExecutor`
        // together with the upstream mv.
        let iter = self
            .inner
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::NoWait(args.epoch),
                row::empty(),
                range_bounds,
                ordered,
                PrefetchOptions::new_for_exhaust_iter(),
            )
            .await?
            .map(get_second);

        pin_mut!(iter);

        #[for_await]
        for chunk in iter_chunks(iter, self.inner.schema(), chunk_size) {
            yield chunk?;
        }
    }
}

impl<S: StateStore> SnapshotRead for UpstreamTableFacade<ExternalUpstreamTable<S>> {
    async fn snapshot_read(&self, args: SnapshotReadArgs) {}
}
