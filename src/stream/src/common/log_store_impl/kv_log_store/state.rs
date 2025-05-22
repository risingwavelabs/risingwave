// Copyright 2025 RisingWave Labs
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

use bytes::Bytes;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::sink::log_store::LogStoreResult;
use risingwave_hummock_sdk::table_watermark::{
    VnodeWatermark, WatermarkDirection, WatermarkSerdeType,
};
use risingwave_storage::store::{
    InitOptions, LocalStateStore, SealCurrentEpochOptions, StateStoreRead,
};

use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
use crate::common::log_store_impl::kv_log_store::{
    FlushInfo, ReaderTruncationOffsetType, SeqIdType,
};

pub(crate) struct LogStoreReadState<S: StateStoreRead> {
    pub(super) table_id: TableId,
    pub(super) state_store: Arc<S>,
    pub(super) serde: LogStoreRowSerde,
}

impl<S: StateStoreRead> LogStoreReadState<S> {
    pub(crate) fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) {
        self.serde.update_vnode_bitmap(vnode_bitmap);
    }
}

pub(crate) struct LogStoreWriteState<S: LocalStateStore> {
    state_store: S,
    serde: LogStoreRowSerde,
    epoch: Option<EpochPair>,

    on_post_seal: bool,
}

pub(crate) fn new_log_store_state<S: LocalStateStore>(
    table_id: TableId,
    state_store: S,
    serde: LogStoreRowSerde,
) -> (
    LogStoreReadState<S::FlushedSnapshotReader>,
    LogStoreWriteState<S>,
) {
    let flushed_reader = state_store.new_flushed_snapshot_reader();
    (
        LogStoreReadState {
            table_id,
            state_store: Arc::new(flushed_reader),
            serde: serde.clone(),
        },
        LogStoreWriteState {
            state_store,
            serde,
            epoch: None,
            on_post_seal: false,
        },
    )
}

impl<S: LocalStateStore> LogStoreWriteState<S> {
    pub(crate) async fn init(&mut self, epoch: EpochPair) -> LogStoreResult<()> {
        self.state_store.init(InitOptions::new(epoch)).await?;
        assert_eq!(self.epoch.replace(epoch), None, "cannot init for twice");
        Ok(())
    }

    pub(crate) fn epoch(&self) -> EpochPair {
        self.epoch.expect("should have init")
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.state_store.is_dirty()
    }

    pub(crate) fn start_writer(&mut self, record_vnode: bool) -> LogStoreStateWriter<'_, S> {
        let written_vnodes = if record_vnode {
            Some(BitmapBuilder::zeroed(self.serde.vnodes().len()))
        } else {
            None
        };
        LogStoreStateWriter {
            inner: self,
            flush_info: FlushInfo::new(),
            written_vnodes,
        }
    }

    pub(crate) fn seal_current_epoch(
        &mut self,
        next_epoch: u64,
        truncate_offset: Option<ReaderTruncationOffsetType>,
    ) -> LogStorePostSealCurrentEpoch<'_, S> {
        assert!(!self.on_post_seal);
        let watermark = truncate_offset
            .map(|truncation_offset| {
                vec![VnodeWatermark::new(
                    self.serde.vnodes().clone(),
                    self.serde
                        .serialize_truncation_offset_watermark(truncation_offset),
                )]
            })
            .unwrap_or_default();
        self.state_store.seal_current_epoch(
            next_epoch,
            SealCurrentEpochOptions {
                table_watermarks: Some((
                    WatermarkDirection::Ascending,
                    watermark,
                    WatermarkSerdeType::PkPrefix,
                )),
                switch_op_consistency_level: None,
            },
        );
        let epoch = self.epoch.as_mut().expect("should have init");
        epoch.prev = epoch.curr;
        epoch.curr = next_epoch;

        self.on_post_seal = true;
        LogStorePostSealCurrentEpoch { inner: self }
    }

    pub(crate) fn aligned_init_range_start(&self) -> Option<Bytes> {
        (0..self.serde.vnodes().len())
            .flat_map(|vnode| {
                self.state_store
                    .get_table_watermark(VirtualNode::from_index(vnode))
            })
            .max()
    }
}

#[must_use]
pub(crate) struct LogStorePostSealCurrentEpoch<'a, S: LocalStateStore> {
    inner: &'a mut LogStoreWriteState<S>,
}

impl<'a, S: LocalStateStore> LogStorePostSealCurrentEpoch<'a, S> {
    pub(crate) async fn post_yield_barrier(
        self,
        new_vnodes: Option<Arc<Bitmap>>,
    ) -> LogStoreResult<&'a mut LogStoreWriteState<S>> {
        if let Some(new_vnodes) = new_vnodes {
            self.inner.serde.update_vnode_bitmap(new_vnodes.clone());
            self.inner
                .state_store
                .update_vnode_bitmap(new_vnodes.clone())
                .await?;
        }
        self.inner.on_post_seal = false;
        Ok(self.inner)
    }
}

pub(crate) type LogStoreStateWriteChunkFuture<S: LocalStateStore> =
    impl Future<Output = (LogStoreWriteState<S>, LogStoreResult<(FlushInfo, Bitmap)>)> + 'static;

impl<S: LocalStateStore> LogStoreWriteState<S> {
    pub(crate) fn into_write_chunk_future(
        mut self,
        chunk: StreamChunk,
        epoch: u64,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
    ) -> LogStoreStateWriteChunkFuture<S> {
        async move {
            let result = try {
                let mut writer = self.start_writer(true);
                writer.write_chunk(&chunk, epoch, start_seq_id, end_seq_id)?;
                let (flush_info, bitmap) = writer.finish().await?;
                (
                    flush_info,
                    bitmap.expect("should exist when pass true when start_writer"),
                )
            };
            (self, result)
        }
    }
}

pub(crate) struct LogStoreStateWriter<'a, S: LocalStateStore> {
    inner: &'a mut LogStoreWriteState<S>,
    flush_info: FlushInfo,
    written_vnodes: Option<BitmapBuilder>,
}

impl<S: LocalStateStore> LogStoreStateWriter<'_, S> {
    pub(crate) fn write_chunk(
        &mut self,
        chunk: &StreamChunk,
        epoch: u64,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
    ) -> LogStoreResult<()> {
        for (i, (op, row)) in chunk.rows().enumerate() {
            let seq_id = start_seq_id + (i as SeqIdType);
            assert!(seq_id <= end_seq_id);
            let (vnode, key, value) = self.inner.serde.serialize_data_row(epoch, seq_id, op, row);
            if let Some(written_vnodes) = &mut self.written_vnodes {
                written_vnodes.set(vnode.to_index(), true);
            }
            self.flush_info
                .flush_one(key.estimated_size() + value.estimated_size());
            self.inner.state_store.insert(key, value, None)?;
        }
        Ok(())
    }

    pub(crate) fn write_barrier(&mut self, epoch: u64, is_checkpoint: bool) -> LogStoreResult<()> {
        for vnode in self.inner.serde.vnodes().iter_vnodes() {
            let (key, value) = self
                .inner
                .serde
                .serialize_barrier(epoch, vnode, is_checkpoint);
            self.flush_info
                .flush_one(key.estimated_size() + value.estimated_size());
            self.inner.state_store.insert(key, value, None)?;
        }
        Ok(())
    }

    pub(crate) async fn finish(self) -> LogStoreResult<(FlushInfo, Option<Bitmap>)> {
        self.inner.state_store.flush().await?;
        Ok((
            self.flush_info,
            self.written_vnodes.map(|vnodes| vnodes.finish()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use futures::TryStreamExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::{EpochExt, EpochPair};
    use risingwave_hummock_test::test_utils::prepare_hummock_test_env;
    use risingwave_storage::StateStore;
    use risingwave_storage::store::{NewLocalOptions, OpConsistencyLevel};

    use crate::common::log_store_impl::kv_log_store::reader::LogStoreReadStateStreamRangeStart;
    use crate::common::log_store_impl::kv_log_store::serde::{KvLogStoreItem, LogStoreRowSerde};
    use crate::common::log_store_impl::kv_log_store::state::new_log_store_state;
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        TEST_TABLE_ID, check_rows_eq, check_stream_chunk_eq, gen_multi_vnode_stream_chunks,
        gen_test_log_store_table,
    };
    use crate::common::log_store_impl::kv_log_store::{
        KV_LOG_STORE_V2_INFO, KvLogStorePkInfo, KvLogStoreReadMetrics, LogStoreVnodeProgress, SeqId,
    };

    #[tokio::test]
    async fn test_update_vnode_bitmap() {
        let pk_info: &'static KvLogStorePkInfo = &KV_LOG_STORE_V2_INFO;
        let test_env = prepare_hummock_test_env().await;

        let table = gen_test_log_store_table(pk_info);

        test_env.register_table(table.clone()).await;

        fn build_bitmap(indexes: impl Iterator<Item = usize>) -> Arc<Bitmap> {
            let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
            for i in indexes {
                builder.set(i, true);
            }
            Arc::new(builder.finish())
        }

        let epoch1 = test_env
            .storage
            .get_pinned_version()
            .table_committed_epoch(TableId::new(table.id))
            .unwrap()
            .next_epoch();
        test_env
            .storage
            .start_epoch(epoch1, HashSet::from_iter([TableId::new(table.id)]));

        let prepare_state = async |vnodes: &Arc<Bitmap>, chunk: &StreamChunk| {
            let serde = LogStoreRowSerde::new(&table, Some(vnodes.clone()), pk_info);
            let (read_state, mut write_state) = new_log_store_state(
                TEST_TABLE_ID,
                test_env
                    .storage
                    .new_local(NewLocalOptions {
                        table_id: TEST_TABLE_ID,
                        op_consistency_level: OpConsistencyLevel::Inconsistent,
                        table_option: Default::default(),
                        is_replicated: false,
                        vnodes: vnodes.clone(),
                    })
                    .await,
                serde.clone(),
            );
            write_state
                .init(EpochPair::new_test_epoch(epoch1))
                .await
                .unwrap();
            let mut writer = write_state.start_writer(false);
            writer
                .write_chunk(chunk, epoch1, 0, (chunk.cardinality() as SeqId) - 1)
                .unwrap();
            writer.write_barrier(epoch1, true).unwrap();
            writer.finish().await.unwrap();
            let split_size = chunk.cardinality() / 2 + 1;
            let [chunk1, chunk2]: [StreamChunk; 2] = chunk.split(split_size).try_into().unwrap();

            let end_seq_id = chunk1.cardinality() as SeqId - 1;
            let (_, read_chunk, _) = read_state
                .read_flushed_chunk(
                    (**vnodes).clone(),
                    0,
                    0,
                    end_seq_id,
                    epoch1,
                    KvLogStoreReadMetrics::for_test(),
                )
                .await
                .unwrap();
            assert!(check_stream_chunk_eq(&chunk1, &read_chunk));
            (
                read_state,
                write_state,
                chunk2,
                LogStoreVnodeProgress::Aligned(vnodes.clone(), epoch1, Some(end_seq_id)),
            )
        };

        let vnodes1 = build_bitmap((0..VirtualNode::COUNT_FOR_TEST).filter(|i| i % 2 == 0));
        let vnodes2 = build_bitmap((0..VirtualNode::COUNT_FOR_TEST).filter(|i| i % 2 == 1));
        let [chunk1_1, chunk1_2] = gen_multi_vnode_stream_chunks::<2>(0, 100, pk_info);

        let (mut read_state1, mut write_state1, chunk2_1, progress1) =
            prepare_state(&vnodes1, &chunk1_1).await;
        let (read_state2, mut write_state2, chunk2_2, progress2) =
            prepare_state(&vnodes2, &chunk1_2).await;
        let epoch2 = epoch1.next_epoch();
        let post_seal1 = write_state1.seal_current_epoch(epoch2, progress1);
        let post_seal2 = write_state2.seal_current_epoch(epoch2, progress2);

        test_env.commit_epoch(epoch1).await;
        let all_vnode = build_bitmap(0..VirtualNode::COUNT_FOR_TEST);
        post_seal2.post_yield_barrier(None).await.unwrap();
        drop((read_state2, write_state2));
        post_seal1
            .post_yield_barrier(Some(all_vnode.clone()))
            .await
            .unwrap();
        read_state1.update_vnode_bitmap(all_vnode.clone());
        let mut items: Vec<_> = read_state1
            .read_persisted_log_store(
                KvLogStoreReadMetrics::for_test(),
                epoch2,
                LogStoreReadStateStreamRangeStart::Unbounded,
            )
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let (last_epoch, last_item) = items.pop().unwrap();
        assert_eq!(last_epoch, epoch1);
        let KvLogStoreItem::Barrier {
            vnodes,
            is_checkpoint: true,
        } = last_item
        else {
            unreachable!()
        };
        assert_eq!(all_vnode, vnodes);

        assert!(check_rows_eq(
            items.iter().flat_map(|(epoch, item)| {
                assert_eq!(*epoch, epoch1);
                let KvLogStoreItem::StreamChunk { chunk, .. } = item else {
                    unreachable!()
                };
                chunk.rows()
            }),
            chunk2_1.rows().chain(chunk2_2.rows())
        ));
    }
}
