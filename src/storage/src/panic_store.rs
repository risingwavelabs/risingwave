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
use std::ops::Bound;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
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
    type IterStream = StreamTypeOfIter<PanicStateStoreIter>;

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
        _key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
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
    define_local_state_store_associated_type!();

    fn may_exist(
        &self,
        _key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        _read_options: ReadOptions,
    ) -> Self::MayExistFuture<'_> {
        async move {
            panic!("should not call may_exist from the state store!");
        }
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

    fn new_local(&self, _table_id: TableId) -> Self::NewLocalFuture<'_> {
        async {
            panic!("should not call new local from the panic state store");
        }
    }

    fn validate_read_epoch(&self, _epoch: HummockReadEpoch) -> StorageResult<()> {
        panic!("should not call validate_read_epoch from the panic state store");
    }
}

pub struct PanicStateStoreIter {}

impl StateStoreIter for PanicStateStoreIter {
    type Item = StateStoreIterItem;

    type NextFuture<'a> = impl StateStoreIterNextFutureTrait<'a>;

    fn next(&'_ mut self) -> Self::NextFuture<'_> {
        async move { unreachable!() }
    }
}
