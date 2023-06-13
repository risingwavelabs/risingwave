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
use risingwave_common::hash::ParallelUnitMapping;
use risingwave_common::vnode_mapping::vnode_placement::place_vnode;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{FragmentParallelUnitMapping, FragmentParallelUnitMappings};
use tokio::sync::oneshot::Sender;
use tokio::task::JoinHandle;

use crate::manager::{
    ClusterManagerRef, FragmentManager, FragmentManagerRef, LocalNotification,
    NotificationManagerRef,
};
use crate::model::FragmentId;
use crate::storage::MetaStore;

pub type ServingVnodeMappingRef = Arc<ServingVnodeMapping>;

#[derive(Default)]
pub struct ServingVnodeMapping {
    serving_vnode_mappings: RwLock<HashMap<FragmentId, ParallelUnitMapping>>,
}

impl ServingVnodeMapping {
    pub fn all(&self) -> HashMap<FragmentId, ParallelUnitMapping> {
        self.serving_vnode_mappings.read().clone()
    }

    /// Upsert mapping for given fragments according to the latest `workers`.
    /// Returns (successful updates, failed updates).
    fn upsert(
        &self,
        streaming_fragment_mappings: impl IntoIterator<Item = FragmentParallelUnitMapping>,
        workers: &[WorkerNode],
    ) -> (HashMap<FragmentId, ParallelUnitMapping>, Vec<FragmentId>) {
        let mut serving_vnode_mappings = self.serving_vnode_mappings.write();
        let mut upserted: HashMap<FragmentId, ParallelUnitMapping> = HashMap::default();
        let mut failed: Vec<FragmentId> = vec![];
        for fragment in streaming_fragment_mappings {
            let new_mapping = {
                let old_mapping = serving_vnode_mappings.get(&fragment.fragment_id);
                // Set max serving parallelism to `streaming_parallelism`. It's not a must.
                let streaming_parallelism = match fragment.mapping.as_ref() {
                    Some(mapping) => ParallelUnitMapping::from_protobuf(mapping)
                        .iter_unique()
                        .count(),
                    None => {
                        tracing::warn!(
                            "vnode mapping for fragment {} not found",
                            fragment.fragment_id
                        );
                        1
                    }
                };
                place_vnode(old_mapping, workers, streaming_parallelism)
            };
            match new_mapping {
                None => {
                    serving_vnode_mappings.remove(&fragment.fragment_id as _);
                    failed.push(fragment.fragment_id);
                }
                Some(mapping) => {
                    serving_vnode_mappings.insert(fragment.fragment_id, mapping.clone());
                    upserted.insert(fragment.fragment_id, mapping);
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

async fn all_streaming_fragment_mappings<S: MetaStore>(
    fragment_manager: &FragmentManager<S>,
) -> Vec<FragmentParallelUnitMapping> {
    let guard = fragment_manager.get_fragment_read_guard().await;
    guard.all_running_fragment_mappings().collect()
}

fn to_fragment_parallel_unit_mapping(
    mappings: &HashMap<FragmentId, ParallelUnitMapping>,
) -> Vec<FragmentParallelUnitMapping> {
    mappings
        .iter()
        .map(|(fragment_id, mapping)| FragmentParallelUnitMapping {
            fragment_id: *fragment_id,
            mapping: Some(mapping.to_protobuf()),
        })
        .collect()
}

fn to_deleted_fragment_parallel_unit_mapping(
    fragment_ids: &[FragmentId],
) -> Vec<FragmentParallelUnitMapping> {
    fragment_ids
        .iter()
        .map(|fragment_id| FragmentParallelUnitMapping {
            fragment_id: *fragment_id,
            mapping: None,
        })
        .collect()
}

pub(crate) async fn on_meta_start<S: MetaStore>(
    notification_manager: NotificationManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
    serving_vnode_mapping: ServingVnodeMappingRef,
) {
    let streaming_fragment_mappings = all_streaming_fragment_mappings(&fragment_manager).await;
    let (mappings, _) = serving_vnode_mapping.upsert(
        streaming_fragment_mappings,
        &cluster_manager.list_active_serving_compute_nodes().await,
    );
    tracing::info!(
        "Initialize serving vnode mapping snapshot for fragments {:?}.",
        mappings.keys()
    );
    notification_manager.notify_frontend_without_version(
        Operation::Snapshot,
        Info::ServingParallelUnitMappings(FragmentParallelUnitMappings {
            mappings: to_fragment_parallel_unit_mapping(&mappings),
        }),
    );
}

pub(crate) async fn start_serving_vnode_mapping_worker<S: MetaStore>(
    notification_manager: NotificationManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
    serving_vnode_mapping: ServingVnodeMappingRef,
) -> (JoinHandle<()>, Sender<()>) {
    let (local_notification_tx, mut local_notification_rx) = tokio::sync::mpsc::unbounded_channel();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    notification_manager
        .insert_local_sender(local_notification_tx)
        .await;
    let join_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                notification = local_notification_rx.recv() => {
                    match notification {
                        Some(notification) => {
                            match notification {
                                LocalNotification::WorkerNodeActivated(w) | LocalNotification::WorkerNodeDeleted(w) =>  {
                                    if w.r#type() != WorkerType::ComputeNode || !w.property.as_ref().map_or(false, |p| p.is_serving) {
                                        continue;
                                    }
                                    let workers = cluster_manager.list_active_serving_compute_nodes().await;
                                    let all_streaming_mappings = all_streaming_fragment_mappings(&fragment_manager).await;
                                    let (mappings, _) = serving_vnode_mapping.upsert(all_streaming_mappings, &workers);
                                    tracing::info!("Update serving vnode mapping snapshot for fragments {:?}.", mappings.keys());
                                    notification_manager.notify_frontend_without_version(Operation::Snapshot, Info::ServingParallelUnitMappings(FragmentParallelUnitMappings{ mappings: to_fragment_parallel_unit_mapping(&mappings) }));
                                }
                                LocalNotification::FragmentMappingsUpsert(fragment_ids) => {
                                    if fragment_ids.is_empty() {
                                        continue;
                                    }
                                    let workers = cluster_manager.list_active_serving_compute_nodes().await;
                                    let added_streaming_mappings = all_streaming_fragment_mappings(&fragment_manager).await.into_iter().filter(|f|fragment_ids.contains(&f.fragment_id));
                                    let (upserted, failed) = serving_vnode_mapping.upsert(added_streaming_mappings, &workers);
                                    if !upserted.is_empty() {
                                        tracing::info!("Update serving vnode mapping for fragments {:?}.", upserted.keys());
                                        notification_manager.notify_frontend_without_version(Operation::Update, Info::ServingParallelUnitMappings(FragmentParallelUnitMappings{ mappings: to_fragment_parallel_unit_mapping(&upserted) }));
                                    }
                                    if !failed.is_empty() {
                                        tracing::info!("Fail to update serving vnode mapping for fragments {:?}.", failed);
                                        notification_manager.notify_frontend_without_version(Operation::Delete, Info::ServingParallelUnitMappings(FragmentParallelUnitMappings{ mappings: to_deleted_fragment_parallel_unit_mapping(&failed)}));
                                    }
                                }
                                LocalNotification::FragmentMappingsDelete(fragment_ids) => {
                                    if fragment_ids.is_empty() {
                                        continue;
                                    }
                                    tracing::info!("Delete serving vnode mapping for fragments {:?}.", fragment_ids);
                                    serving_vnode_mapping.remove(&fragment_ids);
                                    notification_manager.notify_frontend_without_version(Operation::Delete, Info::ServingParallelUnitMappings(FragmentParallelUnitMappings{ mappings: to_deleted_fragment_parallel_unit_mapping(&fragment_ids) }));
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
