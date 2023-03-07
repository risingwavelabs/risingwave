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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;
use risingwave_hummock_sdk::HummockReadEpoch;

use crate::error::StorageResult;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{
    define_local_state_store_associated_type, define_state_store_associated_type,
    define_state_store_read_associated_type, define_state_store_write_associated_type,
};

/// A panic state store. If a workload is fully in-memory, we can use this state store to
/// ensure that no data is stored in the state store and no serialization will happen.
#[derive(Clone, Default)]
pub struct PanicStateStore;

impl StateStoreRead for PanicStateStore {
    type IterStream = PanicStateStoreStream;

    define_state_store_read_associated_type!();

    fn get<'a>(
        &'a self,
        _key: &'a [u8],
        _epoch: u64,
        _read_options: ReadOptions,
    ) -> Self::GetFuture<'_> {
        async move {
            panic!("should not read from the state store!");
        }
    }

    fn iter(
        &self,
        _key_range: IterKeyRange,
        _epoch: u64,
        _read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move {
            panic!("should not read from the state store!");
        }
    }
}

impl StateStoreWrite for PanicStateStore {
    define_state_store_write_associated_type!();

    fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _delete_ranges: Vec<(Bytes, Bytes)>,
        _write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            panic!("should not read from the state store!");
        }
    }
}

impl LocalStateStore for PanicStateStore {
    type IterStream<'a> = PanicStateStoreStream;

    type FlushFuture<'a> = impl Future<Output = StorageResult<usize>> + 'a;
    type GetFuture<'a> = impl GetFutureTrait<'a>;
    type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;

    define_local_state_store_associated_type!();

    fn may_exist(
        &self,
        _key_range: IterKeyRange,
        _read_options: ReadOptions,
    ) -> Self::MayExistFuture<'_> {
        async move {
            panic!("should not call may_exist from the state store!");
        }
    }

    fn get<'a>(&'a self, _key: &'a [u8], _read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move {
            panic!("should not operate on the panic state store!");
        }
    }

    fn iter(&self, _key_range: IterKeyRange, _read_options: ReadOptions) -> Self::IterFuture<'_> {
        async move {
            panic!("should not operate on the panic state store!");
        }
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

    fn flush(&mut self, _delete_ranges: Vec<(Bytes, Bytes)>) -> Self::FlushFuture<'_> {
        async {
            panic!("should not operate on the panic state store!");
        }
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

    type NewLocalFuture<'a> = impl Future<Output = Self::Local> + Send + 'a;

    define_state_store_associated_type!();

    fn try_wait_epoch(&self, _epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_> {
        async move {
            panic!("should not wait epoch from the panic state store!");
        }
    }

    fn sync(&self, _epoch: u64) -> Self::SyncFuture<'_> {
        async move {
            panic!("should not await sync epoch from the panic state store!");
        }
    }

    fn seal_epoch(&self, _epoch: u64, _is_checkpoint: bool) {
        panic!("should not update current epoch from the panic state store!");
    }

    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        async move {
            panic!("should not clear shared buffer from the panic state store!");
        }
    }

    fn new_local(&self, _option: NewLocalOptions) -> Self::NewLocalFuture<'_> {
        async {
            panic!("should not call new local from the panic state store");
        }
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
