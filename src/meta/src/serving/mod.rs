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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use risingwave_common::hash::WorkerSlotMapping;
use risingwave_common::vnode_mapping::vnode_placement::place_vnode;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::{FragmentWorkerSlotMapping, FragmentWorkerSlotMappings};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::controller::fragment::FragmentParallelismInfo;
use crate::controller::session_params::SessionParamsControllerRef;
use crate::manager::{LocalNotification, MetadataManager, NotificationManagerRef};
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
        streaming_parallelisms: HashMap<FragmentId, FragmentParallelismInfo>,
        workers: &[WorkerNode],
        max_serving_parallelism: Option<u64>,
    ) -> (HashMap<FragmentId, WorkerSlotMapping>, Vec<FragmentId>) {
        let mut serving_vnode_mappings = self.serving_vnode_mappings.write();
        Self::upsert_locked(
            &mut serving_vnode_mappings,
            streaming_parallelisms,
            workers,
            max_serving_parallelism,
        )
    }

    /// Rebuild mappings from a complete catalog snapshot.
    ///
    /// Unlike [`Self::upsert`], this removes mappings for fragments absent from the snapshot.
    pub(crate) fn reconcile(
        &self,
        streaming_parallelisms: HashMap<FragmentId, FragmentParallelismInfo>,
        workers: &[WorkerNode],
        max_serving_parallelism: Option<u64>,
    ) -> (HashMap<FragmentId, WorkerSlotMapping>, Vec<FragmentId>) {
        let mut serving_vnode_mappings = self.serving_vnode_mappings.write();
        serving_vnode_mappings
            .retain(|fragment_id, _| streaming_parallelisms.contains_key(fragment_id));
        Self::upsert_locked(
            &mut serving_vnode_mappings,
            streaming_parallelisms,
            workers,
            max_serving_parallelism,
        )
    }

    fn upsert_locked(
        serving_vnode_mappings: &mut HashMap<FragmentId, WorkerSlotMapping>,
        streaming_parallelisms: HashMap<FragmentId, FragmentParallelismInfo>,
        workers: &[WorkerNode],
        max_serving_parallelism: Option<u64>,
    ) -> (HashMap<FragmentId, WorkerSlotMapping>, Vec<FragmentId>) {
        let mut upserted: HashMap<FragmentId, WorkerSlotMapping> = HashMap::default();
        let mut failed: Vec<FragmentId> = vec![];
        for (fragment_id, info) in streaming_parallelisms {
            let new_mapping = {
                let old_mapping = serving_vnode_mappings.get(&fragment_id);
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
                    serving_vnode_mappings.remove(&fragment_id as _);
                    failed.push(fragment_id);
                }
                Some(mapping) => {
                    serving_vnode_mappings.insert(fragment_id, mapping.clone());
                    upserted.insert(fragment_id, mapping);
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
    fragment_ids: &[FragmentId],
) -> Vec<FragmentWorkerSlotMapping> {
    fragment_ids
        .iter()
        .map(|&fragment_id| FragmentWorkerSlotMapping {
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
    let (mappings, failed) = serving_vnode_mapping.reconcile(
        streaming_parallelisms,
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

async fn fetch_serving_infos(
    metadata_manager: &MetadataManager,
) -> (
    Vec<WorkerNode>,
    HashMap<FragmentId, FragmentParallelismInfo>,
) {
    let parallelisms = metadata_manager
        .catalog_controller
        .fragment_parallelisms()
        .await
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
            let (mappings, failed) = serving_vnode_mapping.reconcile(
                streaming_parallelisms,
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
                                    let (upserted, failed) = serving_vnode_mapping.upsert(filtered_streaming_parallelisms, &workers, max_serving_parallelism);
                                    if !upserted.is_empty() {
                                        tracing::debug!("Update serving vnode mapping for fragments {:?}.", upserted.keys());
                                        notification_manager.notify_frontend_without_version(Operation::Update, Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings{ mappings: to_fragment_worker_slot_mapping(&upserted) }));
                                    }
                                    if !failed.is_empty() {
                                        tracing::warn!("Fail to update serving vnode mapping for fragments {:?}.", failed);
                                        notification_manager.notify_frontend_without_version(Operation::Delete, Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings{ mappings: to_deleted_fragment_worker_slot_mapping(&failed)}));
                                    }
                                }
                                LocalNotification::ServingFragmentMappingsDelete(fragment_ids) => {
                                    if fragment_ids.is_empty() {
                                        continue;
                                    }
                                    tracing::debug!("Delete serving vnode mapping for fragments {:?}.", fragment_ids);
                                    serving_vnode_mapping.remove(&fragment_ids);
                                    notification_manager.notify_frontend_without_version(Operation::Delete, Info::ServingWorkerSlotMappings(FragmentWorkerSlotMappings{ mappings: to_deleted_fragment_worker_slot_mapping(&fragment_ids) }));
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::hash::VirtualNode;
    use risingwave_common::id::WorkerId;
    use risingwave_pb::common::{WorkerNode, WorkerType, worker_node};
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;

    use super::ServingVnodeMapping;
    use crate::controller::fragment::FragmentParallelismInfo;
    use crate::model::FragmentId;

    fn serving_worker() -> WorkerNode {
        WorkerNode {
            id: WorkerId::new(1),
            r#type: WorkerType::ComputeNode.into(),
            property: Some(worker_node::Property {
                is_serving: true,
                parallelism: 1,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn hash_parallelism() -> FragmentParallelismInfo {
        FragmentParallelismInfo {
            distribution_type: FragmentDistributionType::Hash,
            vnode_count: VirtualNode::COUNT_FOR_TEST,
        }
    }

    #[test]
    fn test_reconcile_exactly_matches_full_snapshot_and_removes_failed_placements() {
        let mapping = ServingVnodeMapping::default();
        let worker = serving_worker();
        let stale_fragment = FragmentId::new(1);
        let retained_fragment = FragmentId::new(2);
        let added_fragment = FragmentId::new(3);

        mapping.upsert(
            HashMap::from([
                (stale_fragment, hash_parallelism()),
                (retained_fragment, hash_parallelism()),
            ]),
            std::slice::from_ref(&worker),
            None,
        );
        assert!(mapping.all().contains_key(&stale_fragment));
        let retained_mapping = mapping.all()[&retained_fragment].clone();

        let (reconciled, failed) = mapping.reconcile(
            HashMap::from([
                (retained_fragment, hash_parallelism()),
                (added_fragment, hash_parallelism()),
            ]),
            &[worker],
            None,
        );
        assert!(failed.is_empty());
        assert_eq!(reconciled.len(), 2);
        assert!(reconciled.contains_key(&retained_fragment));
        assert!(reconciled.contains_key(&added_fragment));

        let mappings = mapping.all();
        assert_eq!(mappings.len(), 2);
        assert!(mappings.contains_key(&retained_fragment));
        assert!(mappings.contains_key(&added_fragment));
        assert_eq!(mappings[&retained_fragment], retained_mapping);

        let (reconciled, failed) = mapping.reconcile(
            HashMap::from([(retained_fragment, hash_parallelism())]),
            &[],
            None,
        );
        assert!(reconciled.is_empty());
        assert_eq!(failed, vec![retained_fragment]);
        assert!(mapping.all().is_empty());
    }

    #[test]
    fn test_upsert_keeps_fragments_absent_from_incremental_update() {
        let mapping = ServingVnodeMapping::default();
        let worker = serving_worker();
        let first_fragment = FragmentId::new(1);
        let second_fragment = FragmentId::new(2);

        mapping.upsert(
            HashMap::from([(first_fragment, hash_parallelism())]),
            std::slice::from_ref(&worker),
            None,
        );
        mapping.upsert(
            HashMap::from([(second_fragment, hash_parallelism())]),
            &[worker],
            None,
        );

        let mappings = mapping.all();
        assert!(mappings.contains_key(&first_fragment));
        assert!(mappings.contains_key(&second_fragment));
    }
}
