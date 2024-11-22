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

use std::cmp::Ordering;
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
            .max_by(
                |(UnsyncEpochId(_, table1), info1), (UnsyncEpochId(_, table2), info2)| {
                    info1.payload_size.cmp(&info2.payload_size).then_with(|| {
                        if !cfg!(test) {
                            Ordering::Equal
                        } else {
                            assert_ne!(table1, table2);
                            // enforce deterministic spill order in test
                            // smaller table id will be spilled first.
                            table2.cmp(table1)
                        }
                    })
                },
            )
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
    use std::ops::Deref;

    use futures::future::join_all;
    use futures::FutureExt;
    use itertools::Itertools;
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::EpochExt;
    use tokio::sync::oneshot;

    use crate::hummock::event_handler::uploader::test_utils::*;
    use crate::opts::StorageOpts;
    use crate::store::SealCurrentEpochOptions;

    #[tokio::test]
    async fn test_spill_in_order() {
        let config = StorageOpts {
            shared_buffer_capacity_mb: 1024 * 1024,
            shared_buffer_flush_ratio: 0.0,
            ..Default::default()
        };
        let (buffer_tracker, mut uploader, new_task_notifier) =
            prepare_uploader_order_test(&config, false);

        let table_id1 = TableId::new(1);
        let table_id2 = TableId::new(2);

        let instance_id1_1 = 1;
        let instance_id1_2 = 2;
        let instance_id2 = 3;

        let epoch1 = INITIAL_EPOCH.next_epoch();
        let epoch2 = epoch1.next_epoch();
        let epoch3 = epoch2.next_epoch();
        let epoch4 = epoch3.next_epoch();
        let memory_limiter = buffer_tracker.get_memory_limiter().clone();
        let memory_limiter = Some(memory_limiter.deref());

        // epoch1
        uploader.start_epoch(epoch1, HashSet::from_iter([table_id1]));
        uploader.start_epoch(epoch1, HashSet::from_iter([table_id2]));

        uploader.init_instance(instance_id1_1, table_id1, epoch1);
        uploader.init_instance(instance_id1_2, table_id1, epoch1);
        uploader.init_instance(instance_id2, table_id2, epoch1);

        // naming: imm<table>_<instance>_<epoch>
        let imm1_1_1 = gen_imm_inner(table_id1, epoch1, 0, memory_limiter).await;
        uploader.add_imm(instance_id1_1, imm1_1_1.clone());
        let imm1_2_1 = gen_imm_inner(table_id1, epoch1, 0, memory_limiter).await;
        uploader.add_imm(instance_id1_2, imm1_2_1.clone());
        let imm2_1 = gen_imm_inner(table_id2, epoch1, 0, memory_limiter).await;
        uploader.add_imm(instance_id2, imm2_1.clone());

        // epoch2
        uploader.start_epoch(epoch2, HashSet::from_iter([table_id1]));
        uploader.start_epoch(epoch2, HashSet::from_iter([table_id2]));

        uploader.local_seal_epoch(instance_id1_1, epoch2, SealCurrentEpochOptions::for_test());
        uploader.local_seal_epoch(instance_id1_2, epoch2, SealCurrentEpochOptions::for_test());
        uploader.local_seal_epoch(instance_id2, epoch2, SealCurrentEpochOptions::for_test());

        let imms1_1_2 = join_all(
            [0, 1, 2].map(|offset| gen_imm_inner(table_id1, epoch2, offset, memory_limiter)),
        )
        .await;
        for imm in imms1_1_2.clone() {
            uploader.add_imm(instance_id1_1, imm);
        }

        // epoch3
        uploader.start_epoch(epoch3, HashSet::from_iter([table_id1]));
        uploader.start_epoch(epoch3, HashSet::from_iter([table_id2]));

        uploader.local_seal_epoch(instance_id1_1, epoch3, SealCurrentEpochOptions::for_test());
        uploader.local_seal_epoch(instance_id1_2, epoch3, SealCurrentEpochOptions::for_test());
        uploader.local_seal_epoch(instance_id2, epoch3, SealCurrentEpochOptions::for_test());

        let imms1_2_3 = join_all(
            [0, 1, 2, 3].map(|offset| gen_imm_inner(table_id1, epoch3, offset, memory_limiter)),
        )
        .await;
        for imm in imms1_2_3.clone() {
            uploader.add_imm(instance_id1_2, imm);
        }

        // epoch4
        uploader.start_epoch(epoch4, HashSet::from_iter([table_id1, table_id2]));

        uploader.local_seal_epoch(instance_id1_1, epoch4, SealCurrentEpochOptions::for_test());
        uploader.local_seal_epoch(instance_id1_2, epoch4, SealCurrentEpochOptions::for_test());
        uploader.local_seal_epoch(instance_id2, epoch4, SealCurrentEpochOptions::for_test());

        let imm1_1_4 = gen_imm_inner(table_id1, epoch4, 0, memory_limiter).await;
        uploader.add_imm(instance_id1_1, imm1_1_4.clone());
        let imm1_2_4 = gen_imm_inner(table_id1, epoch4, 0, memory_limiter).await;
        uploader.add_imm(instance_id1_2, imm1_2_4.clone());
        let imm2_4_1 = gen_imm_inner(table_id2, epoch4, 0, memory_limiter).await;
        uploader.add_imm(instance_id2, imm2_4_1.clone());

        // uploader state:
        //          table_id1:                                      table_id2:
        //          instance_id1_1:         instance_id1_2:         instance_id2
        //  epoch1  imm1_1_1                imm1_2_1           |    imm2_1      |
        //  epoch2  imms1_1_2(size 3)                          |                |
        //  epoch3                          imms_1_2_3(size 4) |                |
        //  epoch4  imm1_1_4                imm1_2_4                imm2_4_1    |

        let (await_start1_1, finish_tx1_1) = new_task_notifier(HashMap::from_iter([
            (instance_id1_1, vec![imm1_1_1.batch_id()]),
            (instance_id1_2, vec![imm1_2_1.batch_id()]),
        ]));
        let (await_start3, finish_tx3) = new_task_notifier(HashMap::from_iter([(
            instance_id1_2,
            imms1_2_3
                .iter()
                .rev()
                .map(|imm| imm.batch_id())
                .collect_vec(),
        )]));
        let (await_start2, finish_tx2) = new_task_notifier(HashMap::from_iter([(
            instance_id1_1,
            imms1_1_2
                .iter()
                .rev()
                .map(|imm| imm.batch_id())
                .collect_vec(),
        )]));
        let (await_start1_4, finish_tx1_4) = new_task_notifier(HashMap::from_iter([
            (instance_id1_1, vec![imm1_1_4.batch_id()]),
            (instance_id1_2, vec![imm1_2_4.batch_id()]),
        ]));
        let (await_start2_1, finish_tx2_1) = new_task_notifier(HashMap::from_iter([(
            instance_id2,
            vec![imm2_1.batch_id()],
        )]));
        let (await_start2_4_1, finish_tx2_4_1) = new_task_notifier(HashMap::from_iter([(
            instance_id2,
            vec![imm2_4_1.batch_id()],
        )]));

        uploader.may_flush();
        await_start1_1.await;
        await_start3.await;
        await_start2.await;
        await_start1_4.await;
        await_start2_1.await;
        await_start2_4_1.await;

        assert_uploader_pending(&mut uploader).await;

        let imm2_4_2 = gen_imm_inner(table_id2, epoch4, 1, memory_limiter).await;
        uploader.add_imm(instance_id2, imm2_4_2.clone());

        uploader.local_seal_epoch(
            instance_id1_1,
            u64::MAX,
            SealCurrentEpochOptions::for_test(),
        );
        uploader.local_seal_epoch(
            instance_id1_2,
            u64::MAX,
            SealCurrentEpochOptions::for_test(),
        );
        uploader.local_seal_epoch(instance_id2, u64::MAX, SealCurrentEpochOptions::for_test());

        // uploader state:
        //          table_id1:                                      table_id2:
        //          instance_id1_1:         instance_id1_2:         instance_id2
        //  epoch1  spill(imm1_1_1, imm1_2_1, size 2)          |    spill(imm2_1, size 1)               |
        //  epoch2  spill(imms1_1_2, size 3)                   |                                        |
        //  epoch3                   spill(imms_1_2_3, size 4) |                                        |
        //  epoch4  spill(imm1_1_4, imm1_2_4, size 2)               spill(imm2_4_1, size 1), imm2_4_2   |

        let (sync_tx1_1, sync_rx1_1) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch1, sync_tx1_1, HashSet::from_iter([table_id1]));
        let (sync_tx2_1, sync_rx2_1) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch2, sync_tx2_1, HashSet::from_iter([table_id1]));
        let (sync_tx3_1, sync_rx3_1) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch3, sync_tx3_1, HashSet::from_iter([table_id1]));
        let (sync_tx1_2, sync_rx1_2) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch1, sync_tx1_2, HashSet::from_iter([table_id2]));
        let (sync_tx2_2, sync_rx2_2) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch2, sync_tx2_2, HashSet::from_iter([table_id2]));
        let (sync_tx3_2, sync_rx3_2) = oneshot::channel();
        uploader.start_single_epoch_sync(epoch3, sync_tx3_2, HashSet::from_iter([table_id2]));

        let (await_start2_4_2, finish_tx2_4_2) = new_task_notifier(HashMap::from_iter([(
            instance_id2,
            vec![imm2_4_2.batch_id()],
        )]));

        finish_tx2_4_1.send(()).unwrap();
        finish_tx3.send(()).unwrap();
        finish_tx2.send(()).unwrap();
        finish_tx1_4.send(()).unwrap();
        assert_uploader_pending(&mut uploader).await;

        finish_tx1_1.send(()).unwrap();
        {
            let imm_ids = HashMap::from_iter([
                (instance_id1_1, vec![imm1_1_1.batch_id()]),
                (instance_id1_2, vec![imm1_2_1.batch_id()]),
            ]);
            let sst = uploader.next_uploaded_sst().await;
            assert_eq!(&imm_ids, sst.imm_ids());
            let synced_data = sync_rx1_1.await.unwrap().unwrap();
            assert_eq!(synced_data.uploaded_ssts.len(), 1);
            assert_eq!(&imm_ids, synced_data.uploaded_ssts[0].imm_ids());
        }
        {
            let imm_ids3 = HashMap::from_iter([(
                instance_id1_2,
                imms1_2_3
                    .iter()
                    .rev()
                    .map(|imm| imm.batch_id())
                    .collect_vec(),
            )]);
            let imm_ids2 = HashMap::from_iter([(
                instance_id1_1,
                imms1_1_2
                    .iter()
                    .rev()
                    .map(|imm| imm.batch_id())
                    .collect_vec(),
            )]);
            let sst = uploader.next_uploaded_sst().await;
            assert_eq!(&imm_ids3, sst.imm_ids());
            let sst = uploader.next_uploaded_sst().await;
            assert_eq!(&imm_ids2, sst.imm_ids());
            let synced_data = sync_rx2_1.await.unwrap().unwrap();
            assert_eq!(synced_data.uploaded_ssts.len(), 1);
            assert_eq!(&imm_ids2, synced_data.uploaded_ssts[0].imm_ids());
            let synced_data = sync_rx3_1.await.unwrap().unwrap();
            assert_eq!(synced_data.uploaded_ssts.len(), 1);
            assert_eq!(&imm_ids3, synced_data.uploaded_ssts[0].imm_ids());
        }
        {
            let imm_ids1_4 = HashMap::from_iter([
                (instance_id1_1, vec![imm1_1_4.batch_id()]),
                (instance_id1_2, vec![imm1_2_4.batch_id()]),
            ]);
            let imm_ids2_1 = HashMap::from_iter([(instance_id2, vec![imm2_1.batch_id()])]);
            let imm_ids2_4_1 = HashMap::from_iter([(instance_id2, vec![imm2_4_1.batch_id()])]);
            finish_tx2_1.send(()).unwrap();
            let sst = uploader.next_uploaded_sst().await;
            assert_eq!(&imm_ids1_4, sst.imm_ids());

            // trigger the sync after the spill task is finished and acked to cover the case
            let (sync_tx4, mut sync_rx4) = oneshot::channel();
            uploader.start_single_epoch_sync(
                epoch4,
                sync_tx4,
                HashSet::from_iter([table_id1, table_id2]),
            );
            await_start2_4_2.await;

            let sst = uploader.next_uploaded_sst().await;
            assert_eq!(&imm_ids2_1, sst.imm_ids());
            let sst = uploader.next_uploaded_sst().await;
            assert_eq!(&imm_ids2_4_1, sst.imm_ids());
            let synced_data = sync_rx1_2.await.unwrap().unwrap();
            assert_eq!(synced_data.uploaded_ssts.len(), 1);
            assert_eq!(&imm_ids2_1, synced_data.uploaded_ssts[0].imm_ids());
            let synced_data = sync_rx2_2.await.unwrap().unwrap();
            assert!(synced_data.uploaded_ssts.is_empty());
            let synced_data = sync_rx3_2.await.unwrap().unwrap();
            assert!(synced_data.uploaded_ssts.is_empty());

            let imm_ids2_4_2 = HashMap::from_iter([(instance_id2, vec![imm2_4_2.batch_id()])]);

            assert!((&mut sync_rx4).now_or_never().is_none());
            finish_tx2_4_2.send(()).unwrap();
            let sst = uploader.next_uploaded_sst().await;
            assert_eq!(&imm_ids2_4_2, sst.imm_ids());
            let synced_data = sync_rx4.await.unwrap().unwrap();
            assert_eq!(synced_data.uploaded_ssts.len(), 3);
            assert_eq!(&imm_ids2_4_2, synced_data.uploaded_ssts[0].imm_ids());
            assert_eq!(&imm_ids2_4_1, synced_data.uploaded_ssts[1].imm_ids());
            assert_eq!(&imm_ids1_4, synced_data.uploaded_ssts[2].imm_ids());
        }
    }
}
