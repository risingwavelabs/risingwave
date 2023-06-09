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
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{key_with_epoch, map_table_key_range};
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_pb::hummock::{KeyRange, SstableInfo};
use risingwave_storage::hummock::iterator::test_utils::{
    iterator_test_table_key_of, iterator_test_user_key_of,
};
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use risingwave_storage::hummock::store::version::{
    read_filter_for_batch, read_filter_for_local, HummockReadVersion, StagingData,
    StagingSstableInfo, VersionUpdate,
};
use risingwave_storage::hummock::test_utils::gen_dummy_batch;

use crate::test_utils::prepare_first_valid_version;

#[tokio::test]
async fn test_read_version_basic() {
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;

    let (pinned_version, _, _) =
        prepare_first_valid_version(env, hummock_manager_ref, worker_node).await;

    let mut read_version = HummockReadVersion::new(pinned_version);
    let mut epoch = 1;
    let table_id = 0;

    {
        // single imm
        let kv_pairs = gen_dummy_batch(epoch);
        let sorted_items = SharedBufferBatch::build_shared_buffer_item_batches(kv_pairs);
        let size = SharedBufferBatch::measure_batch_size(&sorted_items);
        let imm = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            sorted_items,
            size,
            vec![],
            TableId::from(table_id),
            None,
            None,
        );

        read_version.update(VersionUpdate::Staging(StagingData::ImmMem(imm)));

        let key = iterator_test_table_key_of(epoch as usize);
        let key_range = map_table_key_range((
            Bound::Included(Bytes::from(key.to_vec())),
            Bound::Included(Bytes::from(key.to_vec())),
        ));

        let (staging_imm_iter, staging_sst_iter) =
            read_version
                .staging()
                .prune_overlap(0, epoch, TableId::default(), &key_range);

        let staging_imm = staging_imm_iter.cloned().collect_vec();

        assert_eq!(1, staging_imm.len());
        assert_eq!(0, staging_sst_iter.count());
        assert!(staging_imm.iter().any(|imm| imm.min_epoch() <= epoch));
    }

    {
        // several epoch
        for _ in 0..5 {
            // epoch from 1 to 6
            epoch += 1;
            let kv_pairs = gen_dummy_batch(epoch);
            let sorted_items = SharedBufferBatch::build_shared_buffer_item_batches(kv_pairs);
            let size = SharedBufferBatch::measure_batch_size(&sorted_items);
            let imm = SharedBufferBatch::build_shared_buffer_batch(
                epoch,
                sorted_items,
                size,
                vec![],
                TableId::from(table_id),
                None,
                None,
            );

            read_version.update(VersionUpdate::Staging(StagingData::ImmMem(imm)));
        }

        for epoch in 1..epoch {
            let key = iterator_test_table_key_of(epoch as usize);
            let key_range = map_table_key_range((
                Bound::Included(Bytes::from(key.to_vec())),
                Bound::Included(Bytes::from(key.to_vec())),
            ));

            let (staging_imm_iter, staging_sst_iter) =
                read_version
                    .staging()
                    .prune_overlap(0, epoch, TableId::default(), &key_range);

            let staging_imm = staging_imm_iter.cloned().collect_vec();

            assert_eq!(1, staging_imm.len() as u64);
            assert_eq!(0, staging_sst_iter.count());
            assert!(staging_imm.iter().any(|imm| imm.min_epoch() <= epoch));
        }
    }

    {
        // test clean imm with sst update info
        let staging = read_version.staging();
        assert_eq!(6, staging.imm.len());
        let batch_id_vec_for_clear = staging
            .imm
            .iter()
            .rev()
            .map(|imm| imm.batch_id())
            .take(3)
            .rev()
            .collect::<Vec<_>>();

        let epoch_id_vec_for_clear = staging
            .imm
            .iter()
            .rev()
            .map(|imm| imm.min_epoch())
            .take(3)
            .rev()
            .collect::<Vec<_>>();

        let dummy_sst = StagingSstableInfo::new(
            vec![
                LocalSstableInfo::for_test(SstableInfo {
                    object_id: 1,
                    sst_id: 1,
                    key_range: Some(KeyRange {
                        left: key_with_epoch(iterator_test_user_key_of(1).encode(), 1),
                        right: key_with_epoch(iterator_test_user_key_of(2).encode(), 2),
                        right_exclusive: false,
                    }),
                    file_size: 1,
                    table_ids: vec![0],
                    meta_offset: 1,
                    stale_key_count: 1,
                    total_key_count: 1,
                    uncompressed_file_size: 1,
                    ..Default::default()
                }),
                LocalSstableInfo::for_test(SstableInfo {
                    object_id: 2,
                    sst_id: 2,
                    key_range: Some(KeyRange {
                        left: key_with_epoch(iterator_test_user_key_of(3).encode(), 3),
                        right: key_with_epoch(iterator_test_user_key_of(3).encode(), 3),
                        right_exclusive: false,
                    }),
                    file_size: 1,
                    table_ids: vec![0],
                    meta_offset: 1,
                    stale_key_count: 1,
                    total_key_count: 1,
                    uncompressed_file_size: 1,
                    ..Default::default()
                }),
            ],
            epoch_id_vec_for_clear,
            batch_id_vec_for_clear,
            1,
        );

        {
            read_version.update(VersionUpdate::Staging(StagingData::Sst(dummy_sst)));
        }
    }

    {
        // test clear related batch after update sst

        // after update sst
        // imm(0, 1, 2) => sst{sst_object_id: 1}
        // staging => {imm(3, 4, 5), sst[{sst_object_id: 1}, {sst_object_id: 2}]}
        let staging = read_version.staging();
        assert_eq!(3, read_version.staging().imm.len());
        assert_eq!(1, read_version.staging().sst.len());
        assert_eq!(2, read_version.staging().sst[0].sstable_infos().len());
        let remain_batch_id_vec = staging
            .imm
            .iter()
            .map(|imm| imm.batch_id())
            .collect::<Vec<_>>();
        assert!(remain_batch_id_vec.iter().any(|batch_id| *batch_id > 2));
    }

    {
        let key_range_left = iterator_test_table_key_of(0);
        let key_range_right = iterator_test_table_key_of(4_usize);

        let key_range = map_table_key_range((
            Bound::Included(Bytes::from(key_range_left)),
            Bound::Included(Bytes::from(key_range_right)),
        ));

        let (staging_imm_iter, staging_sst_iter) =
            read_version
                .staging()
                .prune_overlap(0, epoch, TableId::default(), &key_range);

        let staging_imm = staging_imm_iter.cloned().collect_vec();
        assert_eq!(1, staging_imm.len());
        assert_eq!(4, staging_imm[0].min_epoch());

        let staging_ssts = staging_sst_iter.cloned().collect_vec();
        assert_eq!(2, staging_ssts.len());
        assert_eq!(1, staging_ssts[0].get_object_id());
        assert_eq!(2, staging_ssts[1].get_object_id());
    }

    {
        let key_range_left = iterator_test_table_key_of(3);
        let key_range_right = iterator_test_table_key_of(4);

        let key_range = map_table_key_range((
            Bound::Included(Bytes::from(key_range_left)),
            Bound::Included(Bytes::from(key_range_right)),
        ));

        let (staging_imm_iter, staging_sst_iter) =
            read_version
                .staging()
                .prune_overlap(0, epoch, TableId::default(), &key_range);

        let staging_imm = staging_imm_iter.cloned().collect_vec();
        assert_eq!(1, staging_imm.len());
        assert_eq!(4, staging_imm[0].min_epoch());

        let staging_ssts = staging_sst_iter.cloned().collect_vec();
        assert_eq!(1, staging_ssts.len());
        assert_eq!(2, staging_ssts[0].get_object_id());
    }
}

#[tokio::test]
async fn test_read_filter_basic() {
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;

    let (pinned_version, _, _) =
        prepare_first_valid_version(env, hummock_manager_ref, worker_node).await;

    let read_version = Arc::new(RwLock::new(HummockReadVersion::new(pinned_version)));
    let epoch = 1;
    let table_id = 0;

    {
        // single imm
        let kv_pairs = gen_dummy_batch(epoch);
        let sorted_items = SharedBufferBatch::build_shared_buffer_item_batches(kv_pairs);
        let size = SharedBufferBatch::measure_batch_size(&sorted_items);
        let imm = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            sorted_items,
            size,
            vec![],
            TableId::from(table_id),
            None,
            None,
        );

        read_version
            .write()
            .update(VersionUpdate::Staging(StagingData::ImmMem(imm)));

        // directly prune_overlap
        let key = Bytes::from(iterator_test_table_key_of(epoch as usize));
        let key_range = map_table_key_range((Bound::Included(key.clone()), Bound::Included(key)));

        let (staging_imm, staging_sst) = {
            let read_guard = read_version.read();
            let (staging_imm_iter, staging_sst_iter) = {
                read_guard
                    .staging()
                    .prune_overlap(0, epoch, TableId::default(), &key_range)
            };

            (
                staging_imm_iter.cloned().collect_vec(),
                staging_sst_iter.cloned().collect_vec(),
            )
        };

        assert_eq!(1, staging_imm.len());
        assert_eq!(0, staging_sst.len());
        assert!(staging_imm.iter().any(|imm| imm.min_epoch() <= epoch));

        // build for local
        {
            let hummock_read_snapshot = read_filter_for_local(
                epoch,
                TableId::from(table_id),
                &key_range,
                read_version.clone(),
            )
            .unwrap();

            assert_eq!(1, hummock_read_snapshot.0.len());
            assert_eq!(0, hummock_read_snapshot.1.len());
            assert_eq!(
                read_version.read().committed().max_committed_epoch(),
                hummock_read_snapshot.2.max_committed_epoch()
            );
        }

        // build for batch
        {
            let hummock_read_snapshot = read_filter_for_batch(
                epoch,
                TableId::from(table_id),
                &key_range,
                vec![read_version.clone()],
                Arc::new(read_version.read().committed().clone()),
            )
            .unwrap();

            assert_eq!(1, hummock_read_snapshot.0.len());
            assert_eq!(0, hummock_read_snapshot.1.len());
            assert_eq!(
                read_version.read().committed().max_committed_epoch(),
                hummock_read_snapshot.2.max_committed_epoch()
            );
        }
    }
}
