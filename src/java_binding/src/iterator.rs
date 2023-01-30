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

use std::ops::Bound::Unbounded;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::TableId;
use risingwave_common::row::{OwnedRow, RowDeserializer};
use risingwave_common::types::{DataType, ScalarImpl, ScalarRef};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{InMemObjectStore, ObjectStore, ObjectStoreImpl};
use risingwave_pb::hummock::HummockVersion;
use risingwave_storage::error::{StorageError, StorageResult};
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use risingwave_storage::hummock::store::state_store::HummockStorageIterator;
use risingwave_storage::hummock::store::version::HummockVersionReader;
use risingwave_storage::hummock::{SstableStore, TieredCache};
use risingwave_storage::monitor::HummockStateStoreMetrics;
use risingwave_storage::storage_value::StorageValue;
use risingwave_storage::store::{ReadOptions, StreamTypeOfIter};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::unbounded_channel;

pub struct Iterator {
    runtime: Runtime,
    row_serializer: RowDeserializer,
    stream: Pin<Box<StreamTypeOfIter<HummockStorageIterator>>>,
}

pub struct KeyedRow {
    key: Bytes,
    row: OwnedRow,
}

impl KeyedRow {
    pub fn key(&self) -> &[u8] {
        self.key.as_ref()
    }

    pub fn is_null(&self, idx: usize) -> bool {
        self.row[idx].is_some()
    }

    pub fn get_int64(&self, idx: usize) -> i64 {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Int64(num) => *num,
            _ => unreachable!("type is not int64 at index: {}", idx),
        }
    }

    pub fn get_utf8(&self, idx: usize) -> &str {
        match self.row[idx].as_ref().unwrap() {
            ScalarImpl::Utf8(s) => s.as_ref(),
            _ => unreachable!("type is not utf8 at index: {}", idx),
        }
    }
}

const TEST_EPOCH: u64 = 1000;
const TEST_TABLE_ID: TableId = TableId { table_id: 2333 };

fn gen_mock_schema() -> Vec<DataType> {
    vec![DataType::Int64, DataType::Varchar, DataType::Int64]
}

fn gen_mock_imm_lists() -> Vec<SharedBufferBatch> {
    let rows = vec![
        OwnedRow::new(vec![
            Some(ScalarImpl::Int64(100)),
            Some(ScalarImpl::Utf8("value_of_100".to_owned_scalar())),
            None,
        ]),
        OwnedRow::new(vec![
            Some(ScalarImpl::Int64(101)),
            Some(ScalarImpl::Utf8("value_of_101".to_owned_scalar())),
            Some(ScalarImpl::Int64(2333)),
        ]),
    ];
    let data_chunk = DataChunk::from_rows(&rows, &gen_mock_schema());
    let row_data = data_chunk.serialize();
    let kv_pairs = row_data
        .into_iter()
        .enumerate()
        .map(|(i, row)| {
            (
                Bytes::from(format!("key{:?}", i)),
                StorageValue::new_put(row),
            )
        })
        .collect_vec();

    let sorted_items = SharedBufferBatch::build_shared_buffer_item_batches(kv_pairs);
    let size = SharedBufferBatch::measure_batch_size(&sorted_items);
    let imm = SharedBufferBatch::build_shared_buffer_batch(
        TEST_EPOCH,
        sorted_items,
        size,
        vec![],
        TEST_TABLE_ID,
        None,
    );

    vec![imm]
}

impl Iterator {
    pub fn new() -> StorageResult<Self> {
        let sstable_store = Arc::new(SstableStore::new(
            Arc::new(ObjectStoreImpl::InMem(
                InMemObjectStore::new().monitored(Arc::new(ObjectStoreMetrics::unused())),
            )),
            "random".to_string(),
            1 << 10,
            1 << 10,
            TieredCache::none(),
        ));
        let reader =
            HummockVersionReader::new(sstable_store, Arc::new(HummockStateStoreMetrics::unused()));

        let runtime = tokio::runtime::Runtime::new().unwrap();

        let stream = runtime.block_on(async {
            let stream = reader
                .iter(
                    (Unbounded, Unbounded),
                    TEST_EPOCH,
                    ReadOptions {
                        prefix_hint: None,
                        ignore_range_tombstone: false,
                        retention_seconds: None,
                        table_id: TEST_TABLE_ID,
                        read_version_from_backup: false,
                    },
                    (
                        gen_mock_imm_lists(),
                        vec![],
                        PinnedVersion::new(
                            HummockVersion {
                                id: 0,
                                levels: Default::default(),
                                max_committed_epoch: 0,
                                safe_epoch: 0,
                            },
                            unbounded_channel().0,
                        ),
                    ),
                )
                .await?;
            Ok::<std::pin::Pin<Box<StreamTypeOfIter<HummockStorageIterator>>>, StorageError>(
                Box::pin(stream),
            )
        })?;

        Ok(Self {
            runtime,
            row_serializer: RowDeserializer::new(gen_mock_schema()),
            stream,
        })
    }

    pub fn next(&mut self) -> StorageResult<Option<KeyedRow>> {
        self.runtime.block_on(async {
            let item = self.stream.try_next().await?;
            Ok(match item {
                Some((key, value)) => Some(KeyedRow {
                    key: key.user_key.table_key.0,
                    row: self.row_serializer.deserialize(value)?,
                }),
                None => None,
            })
        })
    }
}
