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
#[cfg(test)]
mod test_utils;
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

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::HummockReadEpoch;
    use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
    use risingwave_storage::store::SyncResult;
    use risingwave_storage::StateStore;

    use crate::common::log_store::kv_log_store::test_utils::{
        gen_stream_chunk, gen_test_log_store_table,
    };
    use crate::common::log_store::kv_log_store::KvLogStoreFactory;
    use crate::common::log_store::{LogReader, LogStoreFactory, LogStoreReadItem, LogWriter};

    #[tokio::test]
    async fn test_basic() {
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table();

        test_env.register_table(table.clone()).await;

        let factory = KvLogStoreFactory::new(test_env.storage.clone(), table.clone(), None, 0);
        let (mut reader, mut writer) = factory.build().await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .version()
            .max_committed_epoch
            + 1;
        writer.init(epoch1).await.unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1 + 1;
        writer.flush_current_epoch(epoch2, false).await.unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();
        let epoch3 = epoch2 + 1;
        writer.flush_current_epoch(epoch3, true).await.unwrap();

        test_env.storage.seal_epoch(epoch1, false);
        test_env.storage.seal_epoch(epoch2, true);
        let sync_result: SyncResult = test_env.storage.sync(epoch2).await.unwrap();
        assert!(!sync_result.uncommitted_ssts.is_empty());

        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch1);
                assert_eq!(stream_chunk1, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch2);
                assert_eq!(stream_chunk2, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_recovery() {
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table();

        test_env.register_table(table.clone()).await;

        let factory = KvLogStoreFactory::new(test_env.storage.clone(), table.clone(), None, 0);
        let (mut reader, mut writer) = factory.build().await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .version()
            .max_committed_epoch
            + 1;
        writer.init(epoch1).await.unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1 + 1;
        writer.flush_current_epoch(epoch2, false).await.unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();
        let epoch3 = epoch2 + 1;
        writer.flush_current_epoch(epoch3, true).await.unwrap();

        test_env.storage.seal_epoch(epoch1, false);

        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch1);
                assert_eq!(stream_chunk1, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch2);
                assert_eq!(stream_chunk2, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        test_env.commit_epoch(epoch2).await;
        // The truncate does not work because it is after the sync
        reader.truncate().await.unwrap();
        test_env
            .storage
            .try_wait_epoch(HummockReadEpoch::Committed(epoch2))
            .await
            .unwrap();

        // Recovery
        test_env.storage.clear_shared_buffer().await.unwrap();

        // Rebuild log reader and writer in recovery
        let factory = KvLogStoreFactory::new(test_env.storage.clone(), table.clone(), None, 0);
        let (mut reader, mut writer) = factory.build().await;
        writer.init(epoch3).await.unwrap();
        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch1);
                assert_eq!(stream_chunk1, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(!is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch2);
                assert_eq!(stream_chunk2, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_truncate() {
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table();

        test_env.register_table(table.clone()).await;

        let factory = KvLogStoreFactory::new(test_env.storage.clone(), table.clone(), None, 0);
        let (mut reader, mut writer) = factory.build().await;

        let stream_chunk1 = gen_stream_chunk(0);
        let stream_chunk2 = gen_stream_chunk(10);

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .version()
            .max_committed_epoch
            + 1;
        writer.init(epoch1).await.unwrap();
        writer.write_chunk(stream_chunk1.clone()).await.unwrap();
        let epoch2 = epoch1 + 1;
        writer.flush_current_epoch(epoch2, true).await.unwrap();
        writer.write_chunk(stream_chunk2.clone()).await.unwrap();

        test_env.commit_epoch(epoch1).await;

        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch1);
                assert_eq!(stream_chunk1, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch1);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        // The truncate should work because it is before the flush
        reader.truncate().await.unwrap();
        let epoch3 = epoch2 + 1;
        writer.flush_current_epoch(epoch3, true).await.unwrap();

        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch2);
                assert_eq!(stream_chunk2, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }

        // Truncation on epoch1 should work because it is before this sync
        test_env.commit_epoch(epoch2).await;
        test_env
            .storage
            .try_wait_epoch(HummockReadEpoch::Committed(epoch2))
            .await
            .unwrap();

        // Recovery
        test_env.storage.clear_shared_buffer().await.unwrap();

        // Rebuild log reader and writer in recovery
        let factory = KvLogStoreFactory::new(test_env.storage.clone(), table.clone(), None, 0);
        let (mut reader, mut writer) = factory.build().await;

        writer.init(epoch3).await.unwrap();
        let stream_chunk3 = gen_stream_chunk(20);
        writer.write_chunk(stream_chunk3.clone()).await.unwrap();

        reader.init().await.unwrap();
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch2);
                assert_eq!(stream_chunk2, read_stream_chunk);
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::Barrier { is_checkpoint }) => {
                assert_eq!(epoch, epoch2);
                assert!(is_checkpoint)
            }
            _ => unreachable!(),
        }
        match reader.next_item().await.unwrap() {
            (epoch, LogStoreReadItem::StreamChunk(read_stream_chunk)) => {
                assert_eq!(epoch, epoch3);
                assert_eq!(stream_chunk3, read_stream_chunk);
            }
            _ => unreachable!(),
        }
    }
}
