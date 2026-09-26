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

use futures::future::try_join_all;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::id::WorkerId;
use risingwave_meta::hummock::HummockManagerRef;
use risingwave_meta::manager::{MetaSrvEnv, MetadataManager};
use risingwave_pb::catalog::table::Engine as TableEngine;
use risingwave_pb::meta::serving_service_server::ServingService;
use risingwave_pb::meta::{
    FragmentWorkerSlotMapping, GetServingVnodeMappingsRequest, GetServingVnodeMappingsResponse,
    WarmUpTableCacheRequest, WarmUpTableCacheResponse,
};
use risingwave_pb::stream_service::WarmUpTableCacheRequest as WorkerWarmUpTableCacheRequest;
use tonic::{Request, Response, Status};

use crate::serving::ServingVnodeMappingRef;

pub struct ServingServiceImpl {
    serving_vnode_mapping: ServingVnodeMappingRef,
    metadata_manager: MetadataManager,
    env: MetaSrvEnv,
    hummock_manager: HummockManagerRef,
}

impl ServingServiceImpl {
    pub fn new(
        serving_vnode_mapping: ServingVnodeMappingRef,
        metadata_manager: MetadataManager,
        env: MetaSrvEnv,
        hummock_manager: HummockManagerRef,
    ) -> Self {
        Self {
            serving_vnode_mapping,
            metadata_manager,
            env,
            hummock_manager,
        }
    }
}

fn worker_vnode_bitmaps<'a>(
    vnode_count: usize,
    actors: impl IntoIterator<Item = (WorkerId, Option<&'a Bitmap>)>,
) -> Result<HashMap<WorkerId, Bitmap>, String> {
    let actors = actors.into_iter().collect::<Vec<_>>();
    if let [(worker_id, None)] = actors.as_slice() {
        return Ok(HashMap::from([(*worker_id, Bitmap::ones(vnode_count))]));
    }

    let mut vnode_owners = vec![None; vnode_count];
    for (worker_id, vnode_bitmap) in actors {
        let vnode_bitmap = vnode_bitmap
            .ok_or_else(|| "an actor in a multi-actor fragment has no vnode bitmap".to_owned())?;
        if vnode_bitmap.len() != vnode_count {
            return Err(format!(
                "an actor vnode bitmap has {} vnodes instead of {vnode_count}",
                vnode_bitmap.len()
            ));
        }
        for vnode in vnode_bitmap.iter_ones() {
            if vnode_owners[vnode].replace(worker_id).is_some() {
                return Err(format!("vnode {vnode} is assigned to multiple actors"));
            }
        }
    }

    let mut builders = HashMap::<WorkerId, BitmapBuilder>::new();
    for (vnode, worker_id) in vnode_owners.into_iter().enumerate() {
        let worker_id = worker_id.ok_or_else(|| format!("vnode {vnode} has no actor"))?;
        builders
            .entry(worker_id)
            .or_insert_with(|| BitmapBuilder::zeroed(vnode_count))
            .set(vnode, true);
    }
    Ok(builders
        .into_iter()
        .map(|(worker_id, builder)| (worker_id, builder.finish()))
        .collect())
}

#[async_trait::async_trait]
impl ServingService for ServingServiceImpl {
    async fn get_serving_vnode_mappings(
        &self,
        _request: Request<GetServingVnodeMappingsRequest>,
    ) -> Result<Response<GetServingVnodeMappingsResponse>, Status> {
        let mappings = self
            .serving_vnode_mapping
            .all()
            .into_iter()
            .map(|(fragment_id, mapping)| FragmentWorkerSlotMapping {
                fragment_id,
                mapping: Some(mapping.to_protobuf()),
            })
            .collect();
        let fragment_to_table = self
            .metadata_manager
            .catalog_controller
            .fragment_job_mapping()
            .await?
            .into_iter()
            .collect();
        Ok(Response::new(GetServingVnodeMappingsResponse {
            fragment_to_table,
            worker_slot_mappings: mappings,
        }))
    }

    async fn warm_up_table_cache(
        &self,
        request: Request<WarmUpTableCacheRequest>,
    ) -> Result<Response<WarmUpTableCacheResponse>, Status> {
        let request = request.into_inner();
        if request.concurrency == 0 {
            return Err(Status::invalid_argument(
                "concurrency must be greater than 0",
            ));
        }
        let table_id = request.table_id;
        let table = self
            .metadata_manager
            .catalog_controller
            .get_table_by_id(table_id)
            .await?;
        if !table.table_type().supports_cache_warm_up() || table.engine() == TableEngine::Iceberg {
            return Err(Status::invalid_argument(format!(
                "table {table_id} does not support cache warm-up"
            )));
        }

        let worker_vnode_bitmaps = {
            let actor_infos = self.env.shared_actor_infos();
            let guard = actor_infos.read_guard();
            let fragment = guard.get_fragment(table.fragment_id).ok_or_else(|| {
                Status::failed_precondition(format!(
                    "streaming actor info for fragment {} of table {table_id} is unavailable",
                    table.fragment_id
                ))
            })?;
            if fragment.vnode_count != table.vnode_count() {
                return Err(Status::failed_precondition(format!(
                    "streaming fragment {} has {} vnodes, but table {table_id} has {}",
                    table.fragment_id,
                    fragment.vnode_count,
                    table.vnode_count()
                )));
            }
            worker_vnode_bitmaps(
                fragment.vnode_count,
                fragment
                    .actors
                    .values()
                    .map(|actor| (actor.worker_id, actor.vnode_bitmap.as_ref())),
            )
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "streaming actor info for fragment {} of table {table_id} is invalid: {error}",
                    table.fragment_id
                ))
            })?
        };

        let mut workers = self
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await?
            .into_iter()
            .map(|worker| (worker.id, worker))
            .collect::<HashMap<_, _>>();
        let worker_requests = worker_vnode_bitmaps
            .into_iter()
            .map(|(worker_id, vnode_bitmap)| {
                let worker = workers.remove(&worker_id).ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "streaming compute node {worker_id} is unavailable"
                    ))
                })?;
                Ok((worker, vnode_bitmap))
            })
            .collect::<Result<Vec<_>, Status>>()?;

        let committed_epoch = self
            .hummock_manager
            .on_current_version(|version| version.table_committed_epoch(table_id))
            .await
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "committed epoch for table {table_id} is unavailable"
                ))
            })?;

        let results = try_join_all(worker_requests.iter().map(
            |(worker, vnode_bitmap)| async move {
                let client = self.env.stream_client_pool().get(worker).await?;
                client
                    .warm_up_table_cache(WorkerWarmUpTableCacheRequest {
                        table_id,
                        vnode_bitmap: Some(vnode_bitmap.to_protobuf()),
                        committed_epoch,
                        concurrency: request.concurrency,
                    })
                    .await
            },
        ))
        .await
        .map_err(|error| {
            Status::internal(format!(
                "failed to warm up cache for table {table_id}: {error}"
            ))
        })?;

        Ok(Response::new(WarmUpTableCacheResponse {
            key_count: results.iter().map(|result| result.key_count).sum(),
            worker_count: results.len() as u32,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_vnode_bitmaps_groups_actors_on_the_same_worker() {
        let worker_1 = WorkerId::new(1);
        let worker_2 = WorkerId::new(2);
        let actor_1 = Bitmap::from_iter([true, false, false, true]);
        let actor_2 = Bitmap::from_iter([false, true, false, false]);
        let actor_3 = Bitmap::from_iter([false, false, true, false]);

        let bitmaps = worker_vnode_bitmaps(
            4,
            [
                (worker_1, Some(&actor_1)),
                (worker_1, Some(&actor_2)),
                (worker_2, Some(&actor_3)),
            ],
        )
        .unwrap();
        assert_eq!(
            bitmaps[&worker_1].iter_ones().collect::<Vec<_>>(),
            [0, 1, 3]
        );
        assert_eq!(bitmaps[&worker_2].iter_ones().collect::<Vec<_>>(), [2]);
    }

    #[test]
    fn test_worker_vnode_bitmaps_for_single_actor() {
        let worker = WorkerId::new(1);
        let bitmaps = worker_vnode_bitmaps(4, [(worker, None)]).unwrap();
        assert_eq!(
            bitmaps[&worker].iter_ones().collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
    }

    #[test]
    fn test_worker_vnode_bitmaps_rejects_invalid_actor_bitmaps() {
        let worker_1 = WorkerId::new(1);
        let worker_2 = WorkerId::new(2);
        let actor_1 = Bitmap::from_iter([true, true]);
        let actor_2 = Bitmap::from_iter([false, true]);

        assert_eq!(
            worker_vnode_bitmaps(2, [(worker_1, Some(&actor_1)), (worker_2, Some(&actor_2)),],)
                .unwrap_err(),
            "vnode 1 is assigned to multiple actors"
        );
        assert_eq!(
            worker_vnode_bitmaps(
                2,
                [(worker_1, Some(&Bitmap::from_iter([true, false, false])))]
            )
            .unwrap_err(),
            "an actor vnode bitmap has 3 vnodes instead of 2"
        );
        assert_eq!(
            worker_vnode_bitmaps(2, [(worker_1, Some(&Bitmap::from_iter([true, false])))])
                .unwrap_err(),
            "vnode 1 has no actor"
        );
    }
}
