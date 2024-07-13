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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::event_handler::uploader::{
    LocalInstanceUnsyncData, UnsyncData, UnsyncEpochId, UploadTaskInput,
};
use crate::hummock::event_handler::LocalInstanceId;

#[derive(Default)]
struct EpochSpillableDataInfo {
    instance_ids: HashSet<LocalInstanceId>,
    payload_size: usize,
}

pub(super) struct Spiller<'a> {
    unsync_data: &'a mut UnsyncData,
    epoch_info: HashMap<UnsyncEpochId, EpochSpillableDataInfo>,
    unsync_epoch_id_map: HashMap<(HummockEpoch, TableId), UnsyncEpochId>,
}

impl<'a> Spiller<'a> {
    pub(super) fn new(unsync_data: &'a mut UnsyncData) -> Self {
        let unsync_epoch_id_map: HashMap<_, _> = unsync_data
            .unsync_epochs
            .iter()
            .flat_map(|(unsync_epoch_id, table_ids)| {
                let epoch = unsync_epoch_id.epoch();
                let unsync_epoch_id = *unsync_epoch_id;
                table_ids
                    .iter()
                    .map(move |table_id| ((epoch, *table_id), unsync_epoch_id))
            })
            .collect();
        let mut epoch_info: HashMap<_, EpochSpillableDataInfo> = HashMap::new();
        for instance_data in unsync_data
            .table_data
            .values()
            .flat_map(|table_data| table_data.instance_data.values())
        {
            if let Some((epoch, spill_size)) = instance_data.spillable_data_info() {
                let unsync_epoch_id = unsync_epoch_id_map
                    .get(&(epoch, instance_data.table_id))
                    .expect("should exist");
                let epoch_info = epoch_info.entry(*unsync_epoch_id).or_default();
                assert!(epoch_info.instance_ids.insert(instance_data.instance_id));
                epoch_info.payload_size += spill_size;
            }
        }
        Self {
            unsync_data,
            epoch_info,
            unsync_epoch_id_map,
        }
    }

    pub(super) fn next_spilled_payload(
        &mut self,
    ) -> Option<(HummockEpoch, UploadTaskInput, HashSet<TableId>)> {
        if let Some(unsync_epoch_id) = self
            .epoch_info
            .iter()
            .max_by_key(|(_, info)| info.payload_size)
            .map(|(unsync_epoch_id, _)| *unsync_epoch_id)
        {
            let spill_epoch = unsync_epoch_id.epoch();
            let spill_info = self
                .epoch_info
                .remove(&unsync_epoch_id)
                .expect("should exist");
            let epoch_info = &mut self.epoch_info;
            let mut payload = HashMap::new();
            let mut spilled_table_ids = HashSet::new();
            for instance_id in spill_info.instance_ids {
                let table_id = *self
                    .unsync_data
                    .instance_table_id
                    .get(&instance_id)
                    .expect("should exist");
                let instance_data = self
                    .unsync_data
                    .table_data
                    .get_mut(&table_id)
                    .expect("should exist")
                    .instance_data
                    .get_mut(&instance_id)
                    .expect("should exist");
                let instance_payload = instance_data.spill(spill_epoch);
                assert!(!instance_payload.is_empty());
                payload.insert(instance_id, instance_payload);
                spilled_table_ids.insert(table_id);

                // update the spill info
                if let Some((new_spill_epoch, size)) = instance_data.spillable_data_info() {
                    let new_unsync_epoch_id = self
                        .unsync_epoch_id_map
                        .get(&(new_spill_epoch, instance_data.table_id))
                        .expect("should exist");
                    let info = epoch_info.entry(*new_unsync_epoch_id).or_default();
                    assert!(info.instance_ids.insert(instance_id));
                    info.payload_size += size;
                }
            }
            Some((spill_epoch, payload, spilled_table_ids))
        } else {
            None
        }
    }

    pub(super) fn unsync_data(&mut self) -> &mut UnsyncData {
        self.unsync_data
    }
}

impl LocalInstanceUnsyncData {
    fn spillable_data_info(&self) -> Option<(HummockEpoch, usize)> {
        self.sealed_data
            .back()
            .or(self.current_epoch_data.as_ref())
            .and_then(|epoch_data| {
                if !epoch_data.is_empty() {
                    Some((
                        epoch_data.epoch,
                        epoch_data.imms.iter().map(|imm| imm.size()).sum(),
                    ))
                } else {
                    None
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::future::poll_fn;
    use std::ops::Deref;
    use std::task::Poll;

    use futures::FutureExt;
    use tokio::sync::oneshot;
    use risingwave_common::util::epoch::EpochExt;

    use crate::hummock::event_handler::uploader::get_payload_imm_ids;
    use crate::hummock::event_handler::uploader::test_utils::{gen_imm_with_limiter, INITIAL_EPOCH, TEST_TABLE_ID};
    use crate::hummock::event_handler::uploader::tests::{assert_uploader_pending, prepare_uploader_order_test};
    use crate::opts::StorageOpts;

    // TODO: add more tests on the spill policy
    #[tokio::test]
    async fn test_uploader_finish_in_order() {
        let config = StorageOpts {
            shared_buffer_capacity_mb: 1024 * 1024,
            shared_buffer_flush_ratio: 0.0,
            ..Default::default()
        };
        let (buffer_tracker, mut uploader, new_task_notifier) =
            prepare_uploader_order_test(&config, false);

        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let epoch3 = epoch2.next_epoch();
        let epoch4 = epoch3.next_epoch();
        uploader.start_epochs_for_test([epoch1, epoch2, epoch3, epoch4]);
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();
        let memory_limiter = Some(memory_limiter.deref());

        let instance_id1 = 1;
        let instance_id2 = 2;

        uploader.init_instance(instance_id1, TEST_TABLE_ID, epoch1);
        uploader.init_instance(instance_id2, TEST_TABLE_ID, epoch2);

        // imm2 contains data in newer epoch, but added first
        let imm2 = gen_imm_with_limiter(epoch2, memory_limiter).await;
        uploader.add_imm(instance_id2, imm2.clone());
        let imm1_1 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, imm1_1.clone());
        let imm1_2 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, imm1_2.clone());

        // imm1 will be spilled first
        let epoch1_spill_payload12 =
            HashMap::from_iter([(instance_id1, vec![imm1_2.clone(), imm1_1.clone()])]);
        let epoch2_spill_payload = HashMap::from_iter([(instance_id2, vec![imm2.clone()])]);
        let (await_start1, finish_tx1) =
            new_task_notifier(get_payload_imm_ids(&epoch1_spill_payload12));
        let (await_start2, finish_tx2) =
            new_task_notifier(get_payload_imm_ids(&epoch2_spill_payload));
        uploader.may_flush();
        await_start1.await;
        await_start2.await;

        assert_uploader_pending(&mut uploader).await;

        finish_tx2.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;

        finish_tx1.send(()).unwrap();
        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch1_spill_payload12), sst.imm_ids());
        assert_eq!(&vec![epoch1], sst.epochs());

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch2_spill_payload), sst.imm_ids());
        assert_eq!(&vec![epoch2], sst.epochs());

        let imm1_3 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, imm1_3.clone());
        let epoch1_spill_payload3 = HashMap::from_iter([(instance_id1, vec![imm1_3.clone()])]);
        let (await_start1_3, finish_tx1_3) =
            new_task_notifier(get_payload_imm_ids(&epoch1_spill_payload3));
        uploader.may_flush();
        await_start1_3.await;
        let imm1_4 = gen_imm_with_limiter(epoch1, memory_limiter).await;
        uploader.add_imm(instance_id1, imm1_4.clone());
        let epoch1_sync_payload = HashMap::from_iter([(instance_id1, vec![imm1_4.clone()])]);
        let (await_start1_4, finish_tx1_4) =
            new_task_notifier(get_payload_imm_ids(&epoch1_sync_payload));
        uploader.local_seal_epoch_for_test(instance_id1, epoch1);
        let (sync_tx1, mut sync_rx1) = oneshot::channel();
        uploader.start_sync_epoch(epoch1, sync_tx1, HashSet::from_iter([TEST_TABLE_ID]));
        await_start1_4.await;

        uploader.local_seal_epoch_for_test(instance_id1, epoch2);
        uploader.local_seal_epoch_for_test(instance_id2, epoch2);

        // current uploader state:
        // unsealed: empty
        // sealed: epoch2: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        let imm3_1 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        let epoch3_spill_payload1 = HashMap::from_iter([(instance_id1, vec![imm3_1.clone()])]);
        uploader.add_imm(instance_id1, imm3_1.clone());
        let (await_start3_1, finish_tx3_1) =
            new_task_notifier(get_payload_imm_ids(&epoch3_spill_payload1));
        uploader.may_flush();
        await_start3_1.await;
        let imm3_2 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        let epoch3_spill_payload2 = HashMap::from_iter([(instance_id2, vec![imm3_2.clone()])]);
        uploader.add_imm(instance_id2, imm3_2.clone());
        let (await_start3_2, finish_tx3_2) =
            new_task_notifier(get_payload_imm_ids(&epoch3_spill_payload2));
        uploader.may_flush();
        await_start3_2.await;
        let imm3_3 = gen_imm_with_limiter(epoch3, memory_limiter).await;
        uploader.add_imm(instance_id1, imm3_3.clone());

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        // sealed: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        uploader.local_seal_epoch_for_test(instance_id1, epoch3);
        let imm4 = gen_imm_with_limiter(epoch4, memory_limiter).await;
        uploader.add_imm(instance_id1, imm4.clone());
        assert_uploader_pending(&mut uploader).await;

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: uploaded sst([imm2])
        // syncing: epoch1: uploading: [imm1_4], [imm1_3], uploaded: sst([imm1_2, imm1_1])

        finish_tx3_1.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;
        finish_tx1_4.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;
        finish_tx1_3.send(()).unwrap();

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch1_spill_payload3), sst.imm_ids());

        assert!(poll_fn(|cx| Poll::Ready(sync_rx1.poll_unpin(cx).is_pending())).await);

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch1_sync_payload), sst.imm_ids());

        if let Ok(Ok(data)) = sync_rx1.await {
            assert_eq!(3, data.uploaded_ssts.len());
            assert_eq!(
                &get_payload_imm_ids(&epoch1_sync_payload),
                data.uploaded_ssts[0].imm_ids()
            );
            assert_eq!(
                &get_payload_imm_ids(&epoch1_spill_payload3),
                data.uploaded_ssts[1].imm_ids()
            );
            assert_eq!(
                &get_payload_imm_ids(&epoch1_spill_payload12),
                data.uploaded_ssts[2].imm_ids()
            );
        } else {
            unreachable!()
        }

        // current uploader state:
        // unsealed: epoch3: imm: imm3_3, uploading: [imm3_2], [imm3_1]
        //           epoch4: imm: imm4
        // sealed: uploaded sst([imm2])
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])

        let (sync_tx2, sync_rx2) = oneshot::channel();
        uploader.start_sync_epoch(epoch2, sync_tx2, HashSet::from_iter([TEST_TABLE_ID]));
        uploader.local_seal_epoch_for_test(instance_id2, epoch3);
        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch3_spill_payload1), sst.imm_ids());

        if let Ok(Ok(data)) = sync_rx2.await {
            assert_eq!(data.uploaded_ssts.len(), 1);
            assert_eq!(
                &get_payload_imm_ids(&epoch2_spill_payload),
                data.uploaded_ssts[0].imm_ids()
            );
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch2, uploader.test_max_synced_epoch());

        // current uploader state:
        // unsealed: epoch4: imm: imm4
        // sealed: imm: imm3_3, uploading: [imm3_2], uploaded: sst([imm3_1])
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        uploader.local_seal_epoch_for_test(instance_id1, epoch4);
        uploader.local_seal_epoch_for_test(instance_id2, epoch4);
        let epoch4_sync_payload = HashMap::from_iter([(instance_id1, vec![imm4, imm3_3])]);
        let (await_start4_with_3_3, finish_tx4_with_3_3) =
            new_task_notifier(get_payload_imm_ids(&epoch4_sync_payload));
        let (sync_tx4, mut sync_rx4) = oneshot::channel();
        uploader.start_sync_epoch(epoch4, sync_tx4, HashSet::from_iter([TEST_TABLE_ID]));
        await_start4_with_3_3.await;

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: epoch4: uploading: [imm4, imm3_3], [imm3_2], uploaded: sst([imm3_1])
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])

        assert_uploader_pending(&mut uploader).await;

        finish_tx3_2.send(()).unwrap();
        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch3_spill_payload2), sst.imm_ids());

        finish_tx4_with_3_3.send(()).unwrap();
        assert!(poll_fn(|cx| Poll::Ready(sync_rx4.poll_unpin(cx).is_pending())).await);

        let sst = uploader.next_uploaded_sst().await;
        assert_eq!(&get_payload_imm_ids(&epoch4_sync_payload), sst.imm_ids());

        if let Ok(Ok(data)) = sync_rx4.await {
            assert_eq!(3, data.uploaded_ssts.len());
            assert_eq!(
                &get_payload_imm_ids(&epoch4_sync_payload),
                data.uploaded_ssts[0].imm_ids()
            );
            assert_eq!(
                &get_payload_imm_ids(&epoch3_spill_payload2),
                data.uploaded_ssts[1].imm_ids()
            );
            assert_eq!(
                &get_payload_imm_ids(&epoch3_spill_payload1),
                data.uploaded_ssts[2].imm_ids(),
            )
        } else {
            unreachable!("should be sync finish");
        }
        assert_eq!(epoch4, uploader.test_max_synced_epoch());

        // current uploader state:
        // unsealed: empty
        // sealed: empty
        // syncing: empty
        // synced: epoch1: sst([imm1_4]), sst([imm1_3]), sst([imm1_2, imm1_1])
        //         epoch2: sst([imm2])
        //         epoch4: sst([imm4, imm3_3]), sst([imm3_2]), sst([imm3_1])
    }
}
