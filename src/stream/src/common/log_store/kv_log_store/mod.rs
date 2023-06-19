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
use std::sync::Arc;

use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_pb::catalog::Table;
use risingwave_storage::store::NewLocalOptions;
use risingwave_storage::StateStore;

use crate::common::log_store::kv_log_store::buffer::new_log_store_buffer;
use crate::common::log_store::kv_log_store::reader::KvLogStoreReader;
use crate::common::log_store::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store::kv_log_store::writer::KvLogStoreWriter;
use crate::common::log_store::LogStoreFactory;

mod buffer;
mod reader;
mod serde;
mod writer;

type SeqIdType = i32;
type RowOpCodeType = i16;

const FIRST_SEQ_ID: SeqIdType = 0;

/// Readers truncate the offset at the granularity of epoch
type ReaderTruncationOffsetType = u64;

pub struct KvLogStoreFactory<S: StateStore> {
    state_store: S,

    table_catalog: Table,

    vnodes: Option<Arc<Bitmap>>,

    max_stream_chunk_count: usize,
}

impl<S: StateStore> KvLogStoreFactory<S> {
    pub fn new(
        state_store: S,
        table_catalog: Table,
        vnodes: Option<Arc<Bitmap>>,
        max_stream_chunk_count: usize,
    ) -> Self {
        Self {
            state_store,
            table_catalog,
            vnodes,
            max_stream_chunk_count,
        }
    }
}

impl<S: StateStore> LogStoreFactory for KvLogStoreFactory<S> {
    type Reader = KvLogStoreReader<S>;
    type Writer = KvLogStoreWriter<S::Local>;

    type BuildFuture = impl Future<Output = (Self::Reader, Self::Writer)>;

    fn build(self) -> Self::BuildFuture {
        async move {
            let table_id = TableId::new(self.table_catalog.id);
            let serde = LogStoreRowSerde::new(&self.table_catalog, self.vnodes);
            let local_state_store = self
                .state_store
                .new_local(NewLocalOptions {
                    table_id: TableId {
                        table_id: self.table_catalog.id,
                    },
                    is_consistent_op: false,
                    table_option: TableOption {
                        retention_seconds: None,
                    },
                    is_replicated: false,
                })
                .await;

            let (tx, rx) = new_log_store_buffer(self.max_stream_chunk_count);

            let reader = KvLogStoreReader::new(table_id, self.state_store, serde.clone(), rx);

            let writer = KvLogStoreWriter::new(table_id, local_state_store, serde, tx);

            (reader, writer)
        }
    }
}
