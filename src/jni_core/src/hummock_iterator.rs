// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use risingwave_common::catalog::ColumnDesc;
use risingwave_common::config::{EvictionConfig, MetricLevel, ObjectStoreConfig};
use risingwave_common::hash::VirtualNode;
use risingwave_common::row::OwnedRow;
use risingwave_common::util::value_encoding::column_aware_row_encoding::ColumnAwareSerde;
use risingwave_common::util::value_encoding::{BasicSerde, EitherSerde, ValueRowDeserializer};
use risingwave_hummock_sdk::key::{prefixed_range_with_vnode, TableKeyRange};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_object_store::object::build_remote_object_store;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_pb::java_binding::key_range::Bound;
use risingwave_pb::java_binding::{KeyRange, ReadPlan};
use risingwave_storage::error::StorageResult;
use risingwave_storage::hummock::local_version::pinned_version::PinnedVersion;
use risingwave_storage::hummock::store::version::HummockVersionReader;
use risingwave_storage::hummock::store::HummockStorageIterator;
use risingwave_storage::hummock::{
    get_committed_read_version_tuple, CachePolicy, FileCache, SstableStore, SstableStoreConfig,
};
use risingwave_storage::monitor::{global_hummock_state_store_metrics, HummockStateStoreMetrics};
use risingwave_storage::row_serde::value_serde::ValueRowSerdeNew;
use risingwave_storage::store::{ReadOptions, StateStoreIterExt};
use risingwave_storage::table::KeyedRow;
use rw_futures_util::select_all;
use tokio::sync::mpsc::unbounded_channel;

type SelectAllIterStream = impl Stream<Item = StorageResult<KeyedRow<Bytes>>> + Unpin;
type SingleIterStream = impl Stream<Item = StorageResult<KeyedRow<Bytes>>>;

fn select_all_vnode_stream(streams: Vec<SingleIterStream>) -> SelectAllIterStream {
    select_all(streams.into_iter().map(Box::pin))
}

fn to_deserialized_stream(
    iter: HummockStorageIterator,
    row_serde: EitherSerde,
) -> SingleIterStream {
    iter.into_stream(move |(key, value)| {
        Ok(KeyedRow::new(
            key.user_key.table_key.copy_into(),
            row_serde.deserialize(value).map(OwnedRow::new)?,
        ))
    })
}

pub struct HummockJavaBindingIterator {
    stream: SelectAllIterStream,
}

impl HummockJavaBindingIterator {
    pub async fn new(read_plan: ReadPlan) -> StorageResult<Self> {
        // Note(bugen): should we forward the implementation to the `StorageTable`?
        let object_store = Arc::new(
            build_remote_object_store(
                &read_plan.object_store_url,
                Arc::new(ObjectStoreMetrics::unused()),
                "Hummock",
                Arc::new(ObjectStoreConfig::default()),
            )
            .await,
        );
        let sstable_store = Arc::new(SstableStore::new(SstableStoreConfig {
            store: object_store,
            path: read_plan.data_dir,
            block_cache_capacity: 1 << 10,
            block_cache_shard_num: 2,
            block_cache_eviction: EvictionConfig::for_test(),
            meta_cache_capacity: 1 << 10,
            meta_cache_shard_num: 2,
            meta_cache_eviction: EvictionConfig::for_test(),
            prefetch_buffer_capacity: 1 << 10,
            max_prefetch_block_number: 16,
            data_file_cache: FileCache::none(),
            meta_file_cache: FileCache::none(),
            recent_filter: None,
            state_store_metrics: Arc::new(global_hummock_state_store_metrics(
                MetricLevel::Disabled,
            )),
        }));
        let reader = HummockVersionReader::new(
            sstable_store,
            Arc::new(HummockStateStoreMetrics::unused()),
            0,
        );

        let table = read_plan.table_catalog.unwrap();
        let versioned = table.version.is_some();
        let table_columns = table
            .columns
            .into_iter()
            .map(|c| ColumnDesc::from(c.column_desc.unwrap()));

        // Decide which serializer to use based on whether the table is versioned or not.
        let row_serde: EitherSerde = if versioned {
            ColumnAwareSerde::new(
                Arc::from_iter(0..table_columns.len()),
                Arc::from_iter(table_columns),
            )
            .into()
        } else {
            BasicSerde::new(
                Arc::from_iter(0..table_columns.len()),
                Arc::from_iter(table_columns),
            )
            .into()
        };

        let mut streams = Vec::with_capacity(read_plan.vnode_ids.len());
        let key_range = read_plan.key_range.unwrap();
        let pin_version = PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&read_plan.version.unwrap()),
            unbounded_channel().0,
        );
        let table_id = read_plan.table_id.into();

        for vnode in read_plan.vnode_ids {
            let vnode = VirtualNode::from_index(vnode as usize);
            let key_range = table_key_range_from_prost(vnode, key_range.clone());
            let (key_range, read_version_tuple) = get_committed_read_version_tuple(
                pin_version.clone(),
                table_id,
                key_range,
                read_plan.epoch,
            );
            let iter = reader
                .iter(
                    key_range,
                    read_plan.epoch,
                    ReadOptions {
                        table_id,
                        cache_policy: CachePolicy::NotFill,
                        ..Default::default()
                    },
                    read_version_tuple,
                )
                .await?;
            streams.push(to_deserialized_stream(iter, row_serde.clone()));
        }

        let stream = select_all_vnode_stream(streams);

        Ok(Self { stream })
    }

    pub async fn next(&mut self) -> StorageResult<Option<KeyedRow<Bytes>>> {
        self.stream.try_next().await
    }
}

fn table_key_range_from_prost(vnode: VirtualNode, r: KeyRange) -> TableKeyRange {
    let map_bound = |b, v| match b {
        Bound::Unbounded => std::ops::Bound::Unbounded,
        Bound::Included => std::ops::Bound::Included(v),
        Bound::Excluded => std::ops::Bound::Excluded(v),
        _ => unreachable!(),
    };
    let left_bound = r.left_bound();
    let right_bound = r.right_bound();
    let left = map_bound(left_bound, r.left);
    let right = map_bound(right_bound, r.right);

    prefixed_range_with_vnode((left, right), vnode)
}
