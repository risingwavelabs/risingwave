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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_common::vnode_mapping::vnode_placement::place_vnode;
use risingwave_meta_model::{TableId, WorkerId};
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::serving_table_vnode_mappings::PbServingTableVnodeMapping;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::{
    FragmentWorkerSlotMapping, FragmentWorkerSlotMappings, PbServingTableVnodeMappings,
    PbTableRefillRuntimeConfig,
};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::MetaResult;
use crate::controller::fragment::FragmentParallelismInfo;
use crate::controller::session_params::SessionParamsControllerRef;
use crate::manager::{LocalNotification, MetadataManager, NotificationManagerRef, WorkerKey};
use crate::model::FragmentId;
use crate::table_refill::build_hummock_serving_table_vnode_mappings;

pub type ServingVnodeMappingRef = Arc<ServingVnodeMapping>;

#[derive(Default)]
pub struct ServingVnodeMapping {
    serving_vnode_mappings: RwLock<HashMap<FragmentId, WorkerSlotMapping>>,
}

impl ServingVnodeMapping {
    pub fn all(&self) -> HashMap<FragmentId, WorkerSlotMapping> {
        self.serving_vnode_mappings.read().clone()
    }

    /// Upsert mapping for given fragments according to the latest `workers`.
    /// Returns (successful updates, failed updates).
    pub fn upsert(
        &self,
        streaming_parallelisms: &HashMap<FragmentId, FragmentParallelismInfo>,
        workers: &[WorkerNode],
        max_serving_parallelism: Option<u64>,
    ) -> (HashMap<FragmentId, WorkerSlotMapping>, HashSet<FragmentId>) {
        let mut serving_vnode_mappings = self.serving_vnode_mappings.write();
        let mut upserted: HashMap<FragmentId, WorkerSlotMapping> = HashMap::default();
        let mut failed: HashSet<FragmentId> = HashSet::default();
        for (fragment_id, info) in streaming_parallelisms {
            let new_mapping = {
                let old_mapping = serving_vnode_mappings.get(fragment_id);
                let max_parallelism = match info.distribution_type {
                    FragmentDistributionType::Unspecified => unreachable!(),
                    FragmentDistributionType::Single => Some(1),
                    FragmentDistributionType::Hash => None,
                }
                .or_else(|| max_serving_parallelism.map(|p| p as usize));
                place_vnode(old_mapping, workers, max_parallelism, info.vnode_count)
            };
            match new_mapping {
                None => {
                    serving_vnode_mappings.remove(fragment_id as _);
                    failed.insert(*fragment_id);
                }
                Some(mapping) => {
                    serving_vnode_mappings.insert(*fragment_id, mapping.clone());
                    upserted.insert(*fragment_id, mapping);
                }
            }
        }
        (upserted, failed)
    }

    fn remove(&self, fragment_ids: &[FragmentId]) {
        let mut mappings = self.serving_vnode_mappings.write();
        for fragment_id in fragment_ids {
            mappings.remove(fragment_id);
        }
    }
}

pub(crate) fn to_fragment_worker_slot_mapping(
    mappings: &HashMap<FragmentId, WorkerSlotMapping>,
) -> Vec<FragmentWorkerSlotMapping> {
    mappings
        .iter()
        .map(|(&fragment_id, mapping)| FragmentWorkerSlotMapping {
            fragment_id,
            mapping: Some(mapping.to_protobuf()),
        })
        .collect()
}

pub(crate) fn to_deleted_fragment_worker_slot_mapping(
    fragment_ids: impl Iterator<Item = FragmentId>,
) -> Vec<FragmentWorkerSlotMapping> {
    fragment_ids
        .map(|fragment_id| FragmentWorkerSlotMapping {
            fragment_id,
            mapping: None,
        })
        .collect()
}

pub async fn on_meta_start(
    notification_manager: NotificationManagerRef,
    metadata_manager: &MetadataManager,
    serving_vnode_mapping: ServingVnodeMappingRef,
    max_serving_parallelism: Option<u64>,
) {
    let (serving_compute_nodes, streaming_parallelisms) = fetch_serving_infos(metadata_manager)
        .await
        .expect("fail to fetch serving infos");
    let (mappings, failed) = serving_vnode_mapping.upsert(
        &streaming_parallelisms,
        &serving_compute_nodes,
        max_serving_parallelism,
    );
    tracing::debug!(
        "Initialize serving vnode mapping snapshot for fragments {:?}.",
        mappings.keys()
    );
    if !failed.is_empty() {
        tracing::warn!(
            "Fail to update serving vnode mapping for fragments {:?}.",
            failed
        );
    }
    notification_manager.notify_frontend_without_version(
        Operation::Snapshot,
        Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings {
            mappings: to_fragment_worker_slot_mapping(&mappings),
        }),
    );
}

pub(crate) async fn fetch_serving_infos(
    metadata_manager: &MetadataManager,
) -> MetaResult<(
    Vec<WorkerNode>,
    HashMap<FragmentId, FragmentParallelismInfo>,
)> {
    let parallelisms = metadata_manager
        .catalog_controller
        .fragment_parallelisms()
        .await?;
    let serving_compute_nodes = metadata_manager
        .cluster_controller
        .list_active_serving_workers()
        .await?;
    Ok((
        serving_compute_nodes,
        parallelisms
            .into_iter()
            .map(|(fragment_id, info)| (fragment_id as FragmentId, info))
            .collect(),
    ))
}

pub(crate) async fn sync_serving_table_vnode_mappings_to_hummock(
    notification_manager: &NotificationManagerRef,
    serving_vnode_mapping: &ServingVnodeMappingRef,
    workers: &[WorkerNode],
    streaming_parallelisms: &HashMap<FragmentId, FragmentParallelismInfo>,
) {
    for worker in workers {
        let Some(host) = worker.host.clone() else {
            tracing::warn!(worker_id = %worker.id, "serving worker host not found");
            continue;
        };
        let config = PbTableRefillRuntimeConfig {
            serving_table_vnode_mappings: Some(build_hummock_serving_table_vnode_mappings(
                serving_vnode_mapping,
                worker.id,
                streaming_parallelisms,
            )),
            ..Default::default()
        };
        notification_manager
            .notify_hummock_targeted_update(WorkerKey(host), Info::TableRefillRuntimeConfig(config))
            .await;
    }
}

pub fn start_serving_vnode_mapping_worker(
    notification_manager: NotificationManagerRef,
    metadata_manager: MetadataManager,
    serving_vnode_mapping: ServingVnodeMappingRef,
    session_params: SessionParamsControllerRef,
) -> (JoinHandle<()>, Sender<()>) {
    let (local_notification_tx, mut local_notification_rx) = tokio::sync::mpsc::unbounded_channel();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    notification_manager.insert_local_sender(local_notification_tx);
    let join_handle = tokio::spawn(async move {
        let reset = || async {
            let (workers, streaming_parallelisms) = fetch_serving_infos(&metadata_manager)
                .await
                .expect("fail to fetch serving infos");
            let max_serving_parallelism = session_params
                .get_params()
                .await
                .batch_parallelism()
                .map(|p| p.get());
            let (mappings, failed) = serving_vnode_mapping.upsert(
                &streaming_parallelisms,
                &workers,
                max_serving_parallelism,
            );
            tracing::debug!(
                "Update serving vnode mapping snapshot for fragments {:?}.",
                mappings.keys()
            );
            if !failed.is_empty() {
                tracing::warn!(
                    "Fail to update serving vnode mapping for fragments {:?}.",
                    failed
                );
            }
            notification_manager.notify_frontend_without_version(
                Operation::Snapshot,
                Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings {
                    mappings: to_fragment_worker_slot_mapping(&mappings),
                }),
            );
            sync_serving_table_vnode_mappings_to_hummock(
                &notification_manager,
                &serving_vnode_mapping,
                &workers,
                &streaming_parallelisms,
            )
            .await;
        };
        loop {
            tokio::select! {
                notification = local_notification_rx.recv() => {
                    match notification {
                        Some(notification) => {
                            match notification {
                                LocalNotification::WorkerNodeActivated(w) | LocalNotification::WorkerNodeDeleted(w) =>  {
                                    if w.r#type() != WorkerType::ComputeNode || !w.property.as_ref().is_some_and(|p| p.is_serving) {
                                        continue;
                                    }
                                    reset().await;
                                }
                                LocalNotification::BatchParallelismChange => {
                                    reset().await;
                                }
                                LocalNotification::ServingFragmentMappingsUpsert(fragment_ids) => {
                                    if fragment_ids.is_empty() {
                                        continue;
                                    }
                                    let (workers, streaming_parallelisms) = fetch_serving_infos(&metadata_manager)
                                        .await
                                        .expect("fail to fetch serving infos");
                                    let filtered_streaming_parallelisms = fragment_ids.iter().filter_map(|frag_id| {
                                        match streaming_parallelisms.get(frag_id) {
                                            Some(info) => Some((*frag_id, info.clone())),
                                            None => {
                                                tracing::warn!(fragment_id = %frag_id, "streaming parallelism not found");
                                                None
                                            }
                                        }
                                    }).collect();
                                    let max_serving_parallelism = session_params
                                        .get_params()
                                        .await
                                        .batch_parallelism()
                                        .map(|p|p.get());
                                    let (upserted, failed) = serving_vnode_mapping.upsert(&filtered_streaming_parallelisms, &workers, max_serving_parallelism);
                                    if !upserted.is_empty() {
                                        tracing::debug!("Update serving vnode mapping for fragments {:?}.", upserted.keys());
                                        notification_manager.notify_frontend_without_version(Operation::Update, Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings{ mappings: to_fragment_worker_slot_mapping(&upserted) }));
                                    }
                                    if !failed.is_empty() {
                                        tracing::warn!("Fail to update serving vnode mapping for fragments {:?}.", failed);
                                        notification_manager.notify_frontend_without_version(Operation::Delete, Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings{ mappings: to_deleted_fragment_worker_slot_mapping(failed.iter().cloned())}));
                                    }
                                    sync_serving_table_vnode_mappings_to_hummock(
                                        &notification_manager,
                                        &serving_vnode_mapping,
                                        &workers,
                                        &streaming_parallelisms,
                                    )
                                    .await;
                                }
                                LocalNotification::ServingFragmentMappingsDelete(fragment_ids) => {
                                    if fragment_ids.is_empty() {
                                        continue;
                                    }

                                    tracing::debug!("Delete serving vnode mapping for fragments {:?}.", fragment_ids);

                                    serving_vnode_mapping.remove(&fragment_ids);
                                    notification_manager.notify_frontend_without_version(Operation::Delete, Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings{ mappings: to_deleted_fragment_worker_slot_mapping(fragment_ids.iter().cloned()) }));
                                    let (workers, streaming_parallelisms) = fetch_serving_infos(&metadata_manager)
                                        .await
                                        .expect("fail to fetch serving infos");
                                    sync_serving_table_vnode_mappings_to_hummock(
                                        &notification_manager,
                                        &serving_vnode_mapping,
                                        &workers,
                                        &streaming_parallelisms,
                                    )
                                    .await;
                                }
                                _ => {}
                            }
                        }
                        None => {
                            return;
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    return;
                }
            }
        }
    });
    (join_handle, shutdown_tx)
}

fn append_table_vnode_mapping_for_worker(
    table_vnode_mapping: &mut HashMap<TableId, Bitmap>,
    worker_id: WorkerId,
    result_table_id: Option<TableId>,
    worker_slot_vnode_mapping: &WorkerSlotMapping,
) {
    let Some(result_table_id) = result_table_id else {
        return;
    };
    for (worker_slot_id, bitmap) in &worker_slot_vnode_mapping.to_bitmaps() {
        if worker_slot_id.worker_id() != worker_id {
            continue;
        }
        table_vnode_mapping
            .entry(result_table_id)
            .and_modify(|b| *b |= bitmap.clone())
            .or_insert_with(|| bitmap.clone());
    }
}

pub fn build_table_vnode_mapping_for_worker(
    worker_id: WorkerId,
    fragment_worker_slot_mappings: &HashMap<FragmentId, WorkerSlotMapping>,
    streaming_parallelisms: &HashMap<FragmentId, FragmentParallelismInfo>,
) -> HashMap<TableId, Bitmap> {
    let mut table_vnode_mapping = HashMap::new();
    for (fragment_id, mapping) in fragment_worker_slot_mappings {
        let Some(info) = streaming_parallelisms.get(fragment_id) else {
            tracing::warn!(%fragment_id, "streaming parallelism not found");
            continue;
        };
        append_table_vnode_mapping_for_worker(
            &mut table_vnode_mapping,
            worker_id,
            info.result_table_id,
            mapping,
        );
    }
    table_vnode_mapping
}

pub fn to_pb_serving_table_vnode_mappings(
    table_vnode_mapping: &HashMap<TableId, Bitmap>,
) -> PbServingTableVnodeMappings {
    PbServingTableVnodeMappings {
        mappings: table_vnode_mapping
            .iter()
            .map(|(table_id, bitmap)| PbServingTableVnodeMapping {
                table_id: table_id.as_raw_id(),
                bitmap: Some(bitmap.to_protobuf()),
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::hash::{VirtualNode, WorkerSlotId};
    use risingwave_meta_model::JobId;
    use risingwave_pb::common::{HostAddress, worker_node};
    use risingwave_pb::meta::subscribe_response::Info;
    use risingwave_pb::meta::{SubscribeResponse, SubscribeType};
    use tokio::sync::mpsc;

    use super::*;
    use crate::controller::SqlMetaStore;
    use crate::manager::NotificationManager;

    fn serving_worker(id: u32) -> WorkerNode {
        WorkerNode {
            id: id.into(),
            r#type: WorkerType::ComputeNode.into(),
            host: Some(HostAddress {
                host: format!("host{}", id),
                port: id as i32,
            }),
            state: worker_node::State::Running as i32,
            property: Some(worker_node::Property {
                is_serving: true,
                parallelism: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_sync_serving_table_vnode_mappings_to_hummock_targets_only_result_tables() {
        let notification_manager =
            Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let worker1 = serving_worker(1);
        let worker2 = serving_worker(2);
        let workers = vec![worker1.clone(), worker2.clone()];
        let worker_key1 = WorkerKey(worker1.host.clone().unwrap());
        let worker_key2 = WorkerKey(worker2.host.clone().unwrap());
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        notification_manager.insert_sender(SubscribeType::Hummock, worker_key1, tx1);
        notification_manager.insert_sender(SubscribeType::Hummock, worker_key2, tx2);

        let job_id = JobId::new(233);
        let table_id = job_id.as_mv_table_id();
        let fragment_id = FragmentId::new(234);
        let private_fragment_id = FragmentId::new(235);
        let sink_fragment_id = FragmentId::new(236);
        let serving_vnode_mapping = Arc::new(ServingVnodeMapping::default());
        let streaming_parallelisms = HashMap::from([
            (
                fragment_id,
                FragmentParallelismInfo {
                    result_table_id: Some(table_id),
                    distribution_type: FragmentDistributionType::Hash,
                    vnode_count: VirtualNode::COUNT_FOR_TEST,
                },
            ),
            (
                private_fragment_id,
                FragmentParallelismInfo {
                    result_table_id: None,
                    distribution_type: FragmentDistributionType::Hash,
                    vnode_count: VirtualNode::COUNT_FOR_TEST,
                },
            ),
            (
                sink_fragment_id,
                FragmentParallelismInfo {
                    result_table_id: None,
                    distribution_type: FragmentDistributionType::Hash,
                    vnode_count: VirtualNode::COUNT_FOR_TEST,
                },
            ),
        ]);
        let (upserted, failed) =
            serving_vnode_mapping.upsert(&streaming_parallelisms, &workers, None);
        assert_eq!(upserted.len(), 3);
        assert!(failed.is_empty());
        // Give the non-result fragment a distinct mapping. If it were incorrectly
        // associated with `table_id`, it would expand the table's ownership to all vnodes.
        serving_vnode_mapping.serving_vnode_mappings.write().insert(
            private_fragment_id,
            WorkerSlotMapping::new_single(WorkerSlotId::new(worker1.id, 0)),
        );

        sync_serving_table_vnode_mappings_to_hummock(
            &notification_manager,
            &serving_vnode_mapping,
            &workers,
            &streaming_parallelisms,
        )
        .await;

        let response1 = rx1.recv().await.unwrap().unwrap();
        let response2 = rx2.recv().await.unwrap().unwrap();
        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());

        let serving_bitmap_count = |response: SubscribeResponse| {
            let Some(Info::TableRefillRuntimeConfig(config)) = response.info else {
                panic!("expect table refill runtime config");
            };
            let mappings = config.serving_table_vnode_mappings.unwrap().mappings;
            assert_eq!(mappings.len(), 1);
            assert_eq!(mappings[0].table_id, table_id.as_raw_id());
            Bitmap::from(mappings[0].bitmap.clone().unwrap()).count_ones()
        };
        let count1 = serving_bitmap_count(response1);
        let count2 = serving_bitmap_count(response2);
        assert!(count1 > 0);
        assert!(count2 > 0);
        assert_eq!(count1 + count2, VirtualNode::COUNT_FOR_TEST);
    }
}
