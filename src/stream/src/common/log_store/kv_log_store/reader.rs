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
use std::ops::Bound::Unbounded;

use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_storage::store::ReadOptions;
use risingwave_storage::StateStore;

use crate::common::log_store::kv_log_store::buffer::{LogStoreBufferItem, LogStoreBufferReceiver};
use crate::common::log_store::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store::{LogReader, LogStoreReadItem, LogStoreResult};

pub struct KvLogStoreReader<S: StateStore> {
    table_id: TableId,

    state_store: S,

    serde: LogStoreRowSerde,

    rx: LogStoreBufferReceiver,
}

impl<S: StateStore> KvLogStoreReader<S> {
    pub(crate) fn new(
        table_id: TableId,
        state_store: S,
        serde: LogStoreRowSerde,
        rx: LogStoreBufferReceiver,
    ) -> Self {
        Self {
            table_id,
            state_store,
            serde,
            rx,
        }
    }
}

impl<S: StateStore> LogReader for KvLogStoreReader<S> {
    type InitFuture<'a> = impl Future<Output = LogStoreResult<u64>> + 'a;
    type NextItemFuture<'a> = impl Future<Output = LogStoreResult<LogStoreReadItem>> + 'a;
    type TruncateFuture<'a> = impl Future<Output = LogStoreResult<()>> + 'a;

    fn init(&mut self) -> Self::InitFuture<'_> {
        async move {
            let first_write_epoch = self.rx.init().await;
            // let stream = self
            //     .state_store
            //     .iter(
            //         (Unbounded, Unbounded),
            //         first_write_epoch,
            //         ReadOptions::default(),
            //     )
            //     .await?;
            // let mut peek_stream = stream.peekable();
            // pin_mut!(peek_stream);
            // let result: &(FullKey<Bytes>, Bytes) =
            //     peek_stream.peek().await.unwrap().as_ref().unwrap();
            todo!()
        }
    }

    fn next_item(&mut self) -> Self::NextItemFuture<'_> {
        async move { todo!() }
    }

    fn truncate(&mut self) -> Self::TruncateFuture<'_> {
        async move { todo!() }
    }
}
