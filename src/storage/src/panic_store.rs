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

use std::ops::Bound;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use risingwave_hummock_sdk::HummockReadEpoch;

use crate::error::StorageResult;
use crate::storage_value::StorageValue;
use crate::store::*;

/// A panic state store. If a workload is fully in-memory, we can use this state store to
/// ensure that no data is stored in the state store and no serialization will happen.
#[derive(Clone, Default)]
pub struct PanicStateStore;

impl StateStoreRead for PanicStateStore {
    type IterStream = PanicStateStoreStream;

    #[allow(clippy::unused_async)]
    async fn get(
        &self,
        _key: Bytes,
        _epoch: u64,
        _read_options: ReadOptions,
    ) -> StorageResult<Option<Bytes>> {
        panic!("should not read from the state store!");
    }

    #[allow(clippy::unused_async)]
    async fn iter(
        &self,
        _key_range: IterKeyRange,
        _epoch: u64,
        _read_options: ReadOptions,
    ) -> StorageResult<Self::IterStream> {
        panic!("should not read from the state store!");
    }
}

impl StateStoreWrite for PanicStateStore {
    #[allow(clippy::unused_async)]
    async fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
        _write_options: WriteOptions,
    ) -> StorageResult<usize> {
        panic!("should not read from the state store!");
    }
}

impl LocalStateStore for PanicStateStore {
    type IterStream<'a> = PanicStateStoreStream;

    #[allow(clippy::unused_async)]
    async fn may_exist(
        &self,
        _key_range: IterKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<bool> {
        panic!("should not call may_exist from the state store!");
    }

    #[allow(clippy::unused_async)]
    async fn get(&self, _key: Bytes, _read_options: ReadOptions) -> StorageResult<Option<Bytes>> {
        panic!("should not operate on the panic state store!");
    }

    #[allow(clippy::unused_async)]
    async fn iter(
        &self,
        _key_range: IterKeyRange,
        _read_options: ReadOptions,
    ) -> StorageResult<Self::IterStream<'_>> {
        panic!("should not operate on the panic state store!");
    }

    fn insert(
        &mut self,
        _key: Bytes,
        _new_val: Bytes,
        _old_val: Option<Bytes>,
    ) -> StorageResult<()> {
        panic!("should not operate on the panic state store!");
    }

    fn delete(&mut self, _key: Bytes, _old_val: Bytes) -> StorageResult<()> {
        panic!("should not operate on the panic state store!");
    }

    #[allow(clippy::unused_async)]
    async fn flush(
        &mut self,
        _delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
    ) -> StorageResult<usize> {
        panic!("should not operate on the panic state store!");
    }

    fn epoch(&self) -> u64 {
        panic!("should not operate on the panic state store!");
    }

    fn is_dirty(&self) -> bool {
        panic!("should not operate on the panic state store!");
    }

    fn init(&mut self, _epoch: u64) {
        panic!("should not operate on the panic state store!");
    }

    fn seal_current_epoch(&mut self, _next_epoch: u64) {
        panic!("should not operate on the panic state store!")
    }
}

impl StateStore for PanicStateStore {
    type Local = Self;

    #[allow(clippy::unused_async)]
    async fn try_wait_epoch(&self, _epoch: HummockReadEpoch) -> StorageResult<()> {
        panic!("should not wait epoch from the panic state store!");
    }

    #[allow(clippy::unused_async)]
    async fn sync(&self, _epoch: u64) -> StorageResult<SyncResult> {
        panic!("should not await sync epoch from the panic state store!");
    }

    fn seal_epoch(&self, _epoch: u64, _is_checkpoint: bool) {
        panic!("should not update current epoch from the panic state store!");
    }

    #[allow(clippy::unused_async)]
    async fn clear_shared_buffer(&self) -> StorageResult<()> {
        panic!("should not clear shared buffer from the panic state store!");
    }

    #[allow(clippy::unused_async)]
    async fn new_local(&self, _option: NewLocalOptions) -> Self::Local {
        panic!("should not call new local from the panic state store");
    }

    fn validate_read_epoch(&self, _epoch: HummockReadEpoch) -> StorageResult<()> {
        panic!("should not call validate_read_epoch from the panic state store");
    }
}

pub struct PanicStateStoreStream {}

impl Stream for PanicStateStoreStream {
    type Item = StorageResult<StateStoreIterItem>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        panic!("should not call next on panic state store stream")
    }
}
