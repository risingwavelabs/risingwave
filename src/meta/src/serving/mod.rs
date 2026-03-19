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
use risingwave_pb::common::{PbHostAddress, PbWorkerNode, WorkerNode, WorkerType};
use risingwave_pb::meta::serving_table_vnode_mappings::PbServingTableVnodeMapping;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::{
    FragmentWorkerSlotMapping, FragmentWorkerSlotMappings, PbServingTableVnodeMappings,
};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::controller::fragment::FragmentParallelismInfo;
use crate::controller::session_params::SessionParamsControllerRef;
use crate::manager::{LocalNotification, MetadataManager, NotificationManagerRef, WorkerKey};
use crate::model::FragmentId;

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
    ) -> (
        HashMap<FragmentId, WorkerSlotMapping>,
        HashMap<FragmentId, FragmentParallelismInfo>,
    ) {
        let mut serving_vnode_mappings = self.serving_vnode_mappings.write();
        let mut upserted: HashMap<FragmentId, WorkerSlotMapping> = HashMap::default();
        let mut failed: HashMap<FragmentId, FragmentParallelismInfo> = HashMap::default();
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
                    failed.insert(*fragment_id, info.clone());
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
    let (serving_compute_nodes, streaming_parallelisms) =
        fetch_serving_infos(metadata_manager).await;
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

    notify_hummock_serving_table_vnode_mapping(
        Operation::Snapshot,
        metadata_manager,
        &notification_manager,
        &mappings,
        &streaming_parallelisms,
    )
    .await;
}

async fn fetch_serving_infos(
    metadata_manager: &MetadataManager,
) -> (
    Vec<WorkerNode>,
    HashMap<FragmentId, FragmentParallelismInfo>,
) {
    // TODO: need another mechanism to refresh serving info instead of panic.
    let parallelisms = metadata_manager
        .catalog_controller
        .running_fragment_parallelisms(None)
        .expect("fail to fetch running parallelisms");
    let serving_compute_nodes = metadata_manager
        .cluster_controller
        .list_active_serving_workers()
        .await
        .expect("fail to list serving compute nodes");
    (
        serving_compute_nodes,
        parallelisms
            .into_iter()
            .map(|(fragment_id, info)| (fragment_id as FragmentId, info))
            .collect(),
    )
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
            let (workers, streaming_parallelisms) = fetch_serving_infos(&metadata_manager).await;
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
            notify_hummock_serving_table_vnode_mapping(
                Operation::Snapshot,
                &metadata_manager,
                &notification_manager,
                &mappings,
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
                                LocalNotification::FragmentMappingsUpsert(fragment_ids) => {
                                    if fragment_ids.is_empty() {
                                        continue;
                                    }
                                    let (workers, streaming_parallelisms) = fetch_serving_infos(&metadata_manager).await;
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
                                        notify_hummock_serving_table_vnode_mapping(
                                            Operation::Update,
                                            &metadata_manager,
                                            &notification_manager,
                                            &upserted,
                                            &filtered_streaming_parallelisms,
                                        ).await;
                                    }
                                    if !failed.is_empty() {
                                        tracing::warn!("Fail to update serving vnode mapping for fragments {:?}.", failed);
                                        notification_manager.notify_frontend_without_version(Operation::Delete, Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings{ mappings: to_deleted_fragment_worker_slot_mapping(failed.keys().cloned())}));
                                        notify_hummock_delete_serving_table_vnode_mapping(
                                            Operation::Delete,
                                            &metadata_manager,
                                            &notification_manager,
                                            &failed,
                                        ).await;
                                    }
                                }
                                LocalNotification::FragmentMappingsDelete(fragment_ids) => {
                                    if fragment_ids.is_empty() {
                                        continue;
                                    }

                                    tracing::debug!("Delete serving vnode mapping for fragments {:?}.", fragment_ids);

                                    // TODO(MrCroxx): Are streaming parallelisms already needed here?
                                    let (_, streaming_parallelisms) = fetch_serving_infos(&metadata_manager).await;
                                    let mut deleted : HashMap<FragmentId, FragmentParallelismInfo> = HashMap::new();
                                    for fragment_id in &fragment_ids {
                                        if let Some(info) = streaming_parallelisms.get(fragment_id) {
                                            deleted.insert(*fragment_id, info.clone());
                                        }
                                    }

                                    serving_vnode_mapping.remove(&fragment_ids);
                                    notification_manager.notify_frontend_without_version(Operation::Delete, Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings{ mappings: to_deleted_fragment_worker_slot_mapping(fragment_ids.iter().cloned()) }));
                                    notify_hummock_delete_serving_table_vnode_mapping(
                                        Operation::Delete,
                                        &metadata_manager,
                                        &notification_manager,
                                        &deleted,
                                    ).await;
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

/// Initialize an empty worker to table id to vnode bitmap mapping.
fn init_worker_table_vnode_mapping(
    active_serving_workers: &[PbWorkerNode],
) -> HashMap<WorkerId, HashMap<TableId, Bitmap>> {
    active_serving_workers
        .iter()
        .map(|worker| (worker.id, HashMap::new()))
        .collect()
}

/// Append vnode bitmap mapping to each worker for each state table id.
fn append_worker_table_vnode_mapping(
    mapping: &mut HashMap<WorkerId, HashMap<TableId, Bitmap>>,
    state_table_ids: &HashSet<TableId>,
    worker_slot_vnode_mapping: &WorkerSlotMapping,
) {
    for (worker_slot_id, bitmap) in &worker_slot_vnode_mapping.to_bitmaps() {
        let worker_id = worker_slot_id.worker_id();
        if let Some(worker_mapping) = mapping.get_mut(&worker_id) {
            for table_id in state_table_ids {
                worker_mapping
                    .entry(*table_id)
                    .and_modify(|b| *b |= bitmap.clone())
                    .or_insert_with(|| bitmap.clone());
            }
        }
    }
}

async fn notify_hummock_serving_table_vnode_mapping(
    operation: Operation,
    metadata_manager: &MetadataManager,
    notification_manager: &NotificationManagerRef,
    fragment_worker_slot_mappings: &HashMap<FragmentId, WorkerSlotMapping>,
    streaming_parallelisms: &HashMap<FragmentId, FragmentParallelismInfo>,
) {
    let active_serving_workers = metadata_manager
        .cluster_controller
        .list_active_serving_workers()
        .await
        .expect("fail to list serving compute nodes");
    let mut worker_table_vnode_mapping = init_worker_table_vnode_mapping(&active_serving_workers);
    for (fragment_id, mapping) in fragment_worker_slot_mappings {
        let state_table_ids = &streaming_parallelisms
            .get(fragment_id)
            .expect("streaming parallelism must exist")
            .state_table_ids;
        append_worker_table_vnode_mapping(
            &mut worker_table_vnode_mapping,
            state_table_ids,
            mapping,
        );
    }

    for worker in active_serving_workers {
        if let Some(table_vnode_mapping) = worker_table_vnode_mapping.get(&worker.id) {
            notification_manager.notify_hummock_with_worker_key_without_version(
                Some(WorkerKey(PbHostAddress {
                    host: worker.host.as_ref().unwrap().host.clone(),
                    port: worker.host.as_ref().unwrap().port,
                })),
                operation,
                Info::ServingTableVnodeMappings(PbServingTableVnodeMappings {
                    mappings: table_vnode_mapping
                        .iter()
                        .map(|(table_id, bitmap)| PbServingTableVnodeMapping {
                            table_id: table_id.as_raw_id(),
                            bitmap: Some(bitmap.to_protobuf()),
                        })
                        .collect(),
                }),
            );
        }
    }
}

/// delete vnode bitmap mapping to each worker for each state table id.
fn delete_worker_table_vnode_mapping(
    mapping: &mut HashMap<WorkerId, HashMap<TableId, Bitmap>>,
    state_table_ids: &HashSet<TableId>,
) {
    for table_id in state_table_ids {
        for worker_mapping in mapping.values_mut() {
            worker_mapping.insert(*table_id, Bitmap::zeros(0));
        }
    }
}

async fn notify_hummock_delete_serving_table_vnode_mapping(
    operation: Operation,
    metadata_manager: &MetadataManager,
    notification_manager: &NotificationManagerRef,
    fragment_infos: &HashMap<FragmentId, FragmentParallelismInfo>,
) {
    let active_serving_workers = metadata_manager
        .cluster_controller
        .list_active_serving_workers()
        .await
        .expect("fail to list serving compute nodes");
    let mut worker_table_vnode_mapping = init_worker_table_vnode_mapping(&active_serving_workers);
    for info in fragment_infos.values() {
        delete_worker_table_vnode_mapping(&mut worker_table_vnode_mapping, &info.state_table_ids);
    }

    for worker in active_serving_workers {
        if let Some(table_vnode_mapping) = worker_table_vnode_mapping.get(&worker.id) {
            notification_manager.notify_hummock_with_worker_key_without_version(
                Some(WorkerKey(PbHostAddress {
                    host: worker.host.as_ref().unwrap().host.clone(),
                    port: worker.host.as_ref().unwrap().port,
                })),
                operation,
                Info::ServingTableVnodeMappings(PbServingTableVnodeMappings {
                    mappings: table_vnode_mapping
                        .iter()
                        .map(|(table_id, bitmap)| PbServingTableVnodeMapping {
                            table_id: table_id.as_raw_id(),
                            bitmap: Some(bitmap.to_protobuf()),
                        })
                        .collect(),
                }),
            );
        }
    }
}
