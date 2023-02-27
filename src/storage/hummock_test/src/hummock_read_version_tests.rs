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

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{key_with_epoch, map_table_key_range, TableKey};
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_pb::hummock::{KeyRange, SstableInfo};
use risingwave_storage::hummock::iterator::test_utils::{
    iterator_test_table_key_of, iterator_test_user_key_of,
};
use risingwave_storage::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use risingwave_storage::hummock::store::immutable_memtable::MergedImmutableMemtable;
use risingwave_storage::hummock::store::immutable_memtable_impl::ImmutableMemtableImpl;
use risingwave_storage::hummock::store::version::StagingData::MergedImmMem;
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
        let key_range =
            map_table_key_range((Bound::Included(key.to_vec()), Bound::Included(key.to_vec())));

        let (staging_imm_iter, staging_sst_iter) =
            read_version
                .staging()
                .prune_overlap(0, epoch, TableId::default(), &key_range);

        let staging_imm = staging_imm_iter.cloned().collect_vec();

        assert_eq!(1, staging_imm.len());
        assert_eq!(0, staging_sst_iter.count());
        assert!(staging_imm.iter().any(|imm| imm.epoch() <= epoch));
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
            let key_range =
                map_table_key_range((Bound::Included(key.to_vec()), Bound::Included(key.to_vec())));

            let (staging_imm_iter, staging_sst_iter) =
                read_version
                    .staging()
                    .prune_overlap(0, epoch, TableId::default(), &key_range);

            let staging_imm = staging_imm_iter.cloned().collect_vec();

            assert_eq!(1, staging_imm.len() as u64);
            assert_eq!(0, staging_sst_iter.count());
            assert!(staging_imm.iter().any(|imm| imm.epoch() <= epoch));
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
            .map(|imm| imm.epoch())
            .take(3)
            .rev()
            .collect::<Vec<_>>();

        let dummy_sst = StagingSstableInfo::new(
            vec![
                LocalSstableInfo::for_test(SstableInfo {
                    id: 1,
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
                    divide_version: 0,
                    uncompressed_file_size: 1,
                    min_epoch: 0,
                    max_epoch: 0,
                }),
                LocalSstableInfo::for_test(SstableInfo {
                    id: 2,
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
                    divide_version: 0,
                    uncompressed_file_size: 1,
                    min_epoch: 0,
                    max_epoch: 0,
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
        // imm(0, 1, 2) => sst{sst_id: 1}
        // staging => {imm(3, 4, 5), sst[{sst_id: 1}, {sst_id: 2}]}
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
            Bound::Included(key_range_left),
            Bound::Included(key_range_right),
        ));

        let (staging_imm_iter, staging_sst_iter) =
            read_version
                .staging()
                .prune_overlap(0, epoch, TableId::default(), &key_range);

        let staging_imm = staging_imm_iter.cloned().collect_vec();
        assert_eq!(1, staging_imm.len());
        assert_eq!(4, staging_imm[0].epoch());

        let staging_ssts = staging_sst_iter.cloned().collect_vec();
        assert_eq!(2, staging_ssts.len());
        assert_eq!(1, staging_ssts[0].id);
        assert_eq!(2, staging_ssts[1].id);
    }

    {
        let key_range_left = iterator_test_table_key_of(3);
        let key_range_right = iterator_test_table_key_of(4);

        let key_range = map_table_key_range((
            Bound::Included(key_range_left),
            Bound::Included(key_range_right),
        ));

        let (staging_imm_iter, staging_sst_iter) =
            read_version
                .staging()
                .prune_overlap(0, epoch, TableId::default(), &key_range);

        let staging_imm = staging_imm_iter.cloned().collect_vec();
        assert_eq!(1, staging_imm.len());
        assert_eq!(4, staging_imm[0].epoch());

        let staging_ssts = staging_sst_iter.cloned().collect_vec();
        assert_eq!(1, staging_ssts.len());
        assert_eq!(2, staging_ssts[0].id);
    }
}

#[tokio::test]
async fn test_read_version_merge_imms() {
    let (env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
        setup_compute_env(8080).await;

    let (pinned_version, _, _) =
        prepare_first_valid_version(env, hummock_manager_ref, worker_node).await;

    let mut read_version = HummockReadVersion::new(pinned_version);
    let table_id = 0;
    let min_epoch = 1;
    let max_epoch = 7;
    let num_new_epochs = 5;
    {
        for epoch in min_epoch..=max_epoch {
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

        {
            let imms = &read_version.staging().imm;
            let mut epoch = max_epoch;
            for imm in imms.iter() {
                assert_eq!(epoch, imm.epoch());
                epoch -= 1;
            }
        }

        // check the ingested imms
        for epoch in min_epoch..=max_epoch {
            let key = iterator_test_table_key_of(epoch as usize);
            let key_range =
                map_table_key_range((Bound::Included(key.to_vec()), Bound::Included(key.to_vec())));
            let (staging_imm_iter, staging_sst_iter) =
                read_version
                    .staging()
                    .prune_overlap(0, epoch, TableId::default(), &key_range);

            let staging_imm = staging_imm_iter.cloned().collect_vec();
            assert_eq!(1, staging_imm.len());
            assert_eq!(0, staging_sst_iter.count());
            assert!(staging_imm.iter().any(|imm| imm.epoch() <= epoch));
        }
    }

    {
        let mut epoch_id_vec_for_clear = vec![];
        // merge ingested imms
        let merged_imm = if let Some(imms) = read_version.get_imms_to_merge() {
            let staging = read_version.staging();
            imms.iter()
                .for_each(|imm| epoch_id_vec_for_clear.push(imm.epoch()));

            let merged_imm =
                MergedImmutableMemtable::build_merged_imm(TableId { table_id }, imms, None);

            assert_eq!(min_epoch, merged_imm.epoch());
            // check before add merged_imm to staging
            // check imm_ids
            let merged_imm_ids = merged_imm.get_merged_imm_ids();
            let staging_imm_ids = staging.imm.iter().map(|imm| imm.batch_id()).collect_vec();
            assert_eq!(staging_imm_ids.len(), merged_imm_ids.len());

            println!(
                "staging imm ids: {:?}, merged imm ids: {:?}",
                staging_imm_ids, merged_imm_ids
            );

            // expect merged_imm_ids is a suffix of staging_imm_ids
            for (id1, id2) in staging_imm_ids.iter().zip_eq(merged_imm_ids.iter()) {
                assert_eq!(id1, id2);
            }
            Some(merged_imm)
        } else {
            None
        };

        {
            // add merged imm to read version, then all staging imms will be removed
            read_version.update(VersionUpdate::Staging(MergedImmMem(merged_imm.unwrap())));
        }

        // ingest new batches
        for epoch in (max_epoch + 1)..=(max_epoch + num_new_epochs) {
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

        let staging = read_version.staging();
        // check after add the merged_imm
        assert_eq!(num_new_epochs as usize, staging.imm.len());
        assert_eq!(1, staging.merged_imm.len());
        let imm = staging.merged_imm.back().unwrap();
        if let ImmutableMemtableImpl::MergedImm(m) = imm {
            assert_eq!(
                TableKey(iterator_test_table_key_of(min_epoch as usize).as_slice()),
                m.start_table_key()
            );
            assert_eq!(
                TableKey(iterator_test_table_key_of(max_epoch as usize).as_slice()),
                m.end_table_key()
            );
            assert_eq!(min_epoch, m.epoch());
        }

        // test merged imm will be clear after flushed to a sst
        let staging = read_version.staging();
        let mut batch_id_vec_for_clear = vec![];
        if let ImmutableMemtableImpl::MergedImm(m) = staging.merged_imm.back().unwrap() {
            batch_id_vec_for_clear.extend(m.get_merged_imm_ids());
        }

        println!(
            "epoch_id_vec_for_clear: {:?}, batch_id_vec_for_clear: {:?}",
            epoch_id_vec_for_clear, batch_id_vec_for_clear
        );

        let dummy_sst = StagingSstableInfo::new(
            vec![
                LocalSstableInfo::for_test(SstableInfo {
                    id: 1,
                    key_range: Some(KeyRange {
                        left: key_with_epoch(
                            iterator_test_user_key_of(min_epoch as usize).encode(),
                            min_epoch,
                        ),
                        right: key_with_epoch(iterator_test_user_key_of(3).encode(), 3),
                        right_exclusive: false,
                    }),
                    file_size: 1,
                    table_ids: vec![0],
                    meta_offset: 1,
                    stale_key_count: 1,
                    total_key_count: 1,
                    divide_version: 0,
                }),
                LocalSstableInfo::for_test(SstableInfo {
                    id: 2,
                    key_range: Some(KeyRange {
                        left: key_with_epoch(iterator_test_user_key_of(4).encode(), 4),
                        right: key_with_epoch(
                            iterator_test_user_key_of(max_epoch as usize).encode(),
                            max_epoch,
                        ),
                        right_exclusive: false,
                    }),
                    file_size: 1,
                    table_ids: vec![0],
                    meta_offset: 1,
                    stale_key_count: 1,
                    total_key_count: 1,
                    divide_version: 0,
                }),
            ],
            epoch_id_vec_for_clear,
            batch_id_vec_for_clear,
            1,
        );

        read_version.update(VersionUpdate::Staging(StagingData::Sst(dummy_sst)));
    }
    {
        // test clear related batch after update sst
        let staging = read_version.staging();
        assert_eq!(num_new_epochs as usize, staging.imm.len());
        assert_eq!(0, staging.merged_imm.len());
        assert_eq!(1, staging.sst.len());
        assert_eq!(2, staging.sst[0].sstable_infos().len());
    }
}

#[tokio::test]
async fn test_read_filter_of_merged_imm() {
    // TODO: test_read_filter_of_merged_imm
    // TODO: test read correctness from the read version
    // let (staging_imm_iter, staging_sst_iter) =
    //     read_version
    //         .staging()
    //         .prune_overlap(0, max_epoch, TableId::default(), &key_range);
    //
    // let mut staging_imm = staging_imm_iter.cloned().collect_vec();
    // assert_eq!(
    //     1 + (max_epoch - IMM_MERGE_THRESHOLD),
    //     staging_imm.len() as u64
    // );
    // assert_eq!(0, staging_sst_iter.count());
    // // equals to the min epoch
    // assert!(staging_imm.iter().any(|imm| imm.epoch() == 1));
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
        let key = iterator_test_table_key_of(epoch as usize);
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
        assert!(staging_imm.iter().any(|imm| imm.epoch() <= epoch));

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
