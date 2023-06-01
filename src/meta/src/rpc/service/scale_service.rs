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

use itertools::Itertools;
use risingwave_pb::common::{Actor, ParallelUnit, WorkerType};
use risingwave_pb::meta::reschedule_request::{self, Reschedule};
use risingwave_pb::meta::scale_service_server::ScaleService;
use risingwave_pb::meta::{
    ClearWorkerNodesRequest, ClearWorkerNodesResponse, GetClusterInfoRequest,
    GetClusterInfoResponse, GetScheduleRequest, GetScheduleResponse, PauseRequest, PauseResponse,
    RescheduleRequest, RescheduleResponse, ResumeRequest, ResumeResponse,
};
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use tonic::{Request, Response, Status};

use crate::barrier::{BarrierScheduler, Command};
use crate::manager::{CatalogManagerRef, ClusterManagerRef, FragmentManagerRef};
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::stream::{GlobalStreamManagerRef, ParallelUnitReschedule, SourceManagerRef};
use crate::MetaError;

pub struct ScaleServiceImpl<S: MetaStore> {
    barrier_scheduler: BarrierScheduler<S>,
    fragment_manager: FragmentManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    source_manager: SourceManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    stream_manager: GlobalStreamManagerRef<S>,
}

impl<S> ScaleServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        barrier_scheduler: BarrierScheduler<S>,
        fragment_manager: FragmentManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        source_manager: SourceManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
        stream_manager: GlobalStreamManagerRef<S>,
    ) -> Self {
        Self {
            barrier_scheduler,
            fragment_manager,
            cluster_manager,
            source_manager,
            catalog_manager,
            stream_manager,
        }
    }
}

fn match_lists(pu_keep: &Vec<u32>, actors: &Vec<Actor>) -> (Vec<u32>, Vec<u32>) {
    let mut add_list: Vec<u32> = vec![];
    let mut remove_list: Vec<u32> = vec![];

    let mut p = 0;
    let mut a = 0;

    while p < pu_keep.len() && a < actors.len() {
        let p_id = pu_keep[p];
        let a_id = actors[a].parallel_units_id;

        if p_id < a_id {
            add_list.push(p_id);
        } else if p_id == a_id {
        } else {
            remove_list.push(a_id);
        }

        // else if?
        if p_id <= a_id {
            p = p + 1;
        } else if p_id >= a_id {
            // TODO: remove else if, just if
            a = a + 1;
        }
    }

    while p < pu_keep.len() {
        let p_id = pu_keep[p];
        add_list.push(p_id);
        p = p + 1;
    }

    while a < actors.len() {
        let a_id = actors[a].parallel_units_id;
        remove_list.push(a_id);
        a = a + 1;
    }

    (add_list, remove_list)
}

fn each_actor_in_all_first_k(
    pu_delete: &Vec<u32>,
    pu_keep: &Vec<u32>,
    schedule: GetScheduleResponse,
) -> std::collections::HashMap<u32, reschedule_request::Reschedule> {
    let mut add_pus: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
    let mut remove_pus: std::collections::HashMap<u32, Vec<u32>> = std::collections::HashMap::new();
    for fragment in schedule.get_fragment_list() {
        let mut actors = fragment.actor_list.clone();
        actors.sort_by_key(|x| x.parallel_units_id);
        let (add_list, remove_list) = match_lists(pu_keep, &actors);
        if add_list.len() > 0 {
            add_pus.insert(fragment.id, add_list);
        }
        if remove_list.len() > 0 {
            remove_pus.insert(fragment.id, remove_list);
        }
    }

    make_reschedule_req(add_pus, remove_pus)
}

fn make_reschedule_req(
    add_pus: std::collections::HashMap<u32, Vec<u32>>,
    remove_pus: std::collections::HashMap<u32, Vec<u32>>,
) -> std::collections::HashMap<u32, reschedule_request::Reschedule> {
    let mut reschedule_map: std::collections::HashMap<u32, reschedule_request::Reschedule> =
        std::collections::HashMap::new();

    for (f_id, pu_ids) in add_pus {
        reschedule_map.insert(
            f_id,
            reschedule_request::Reschedule {
                added_parallel_units: pu_ids,
                removed_parallel_units: vec![],
            },
        );
    }

    for (f_id, pu_ids) in remove_pus {
        if reschedule_map.contains_key(&f_id) {
            let tmp = reschedule_map.get_mut(&f_id).unwrap();
            tmp.removed_parallel_units = pu_ids;
        } else {
            reschedule_map.insert(
                f_id,
                reschedule_request::Reschedule {
                    added_parallel_units: vec![],
                    removed_parallel_units: pu_ids,
                },
            );
        }
    }
    reschedule_map
}

#[async_trait::async_trait]
impl<S> ScaleService for ScaleServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg_attr(coverage, no_coverage)]
    async fn pause(&self, _: Request<PauseRequest>) -> Result<Response<PauseResponse>, Status> {
        self.barrier_scheduler.run_command(Command::pause()).await?;
        Ok(Response::new(PauseResponse {}))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn resume(&self, _: Request<ResumeRequest>) -> Result<Response<ResumeResponse>, Status> {
        self.barrier_scheduler
            .run_command(Command::resume())
            .await?;
        Ok(Response::new(ResumeResponse {}))
    }

    // Removes all actors from the listed workers.
    // This is required before shutting down the workers.
    // Atomic operator. All workers are cleared using a single rescheduling plan.
    #[cfg_attr(coverage, no_coverage)]
    async fn clear_workers(
        &self,
        request: Request<ClearWorkerNodesRequest>,
    ) -> Result<Response<ClearWorkerNodesResponse>, Status> {
        // TODO: we also do this in rw cloud. We can move that functionality from cloud to meta
        // see branch arne/scaling/placement-policy meta.go ClearWorkerNode

        // TODO: Is this really atomic? How do we execute a single plan?

        println!("called clear_workers"); // TODO: remove println

        // TODO: Does meta call itself via rpc? If so, then bad
        // TODO: Do I need to retrieve the schedule every time?
        let schedule = self
            .get_schedule(Request::new(GetScheduleRequest {}))
            .await?
            .into_inner();

        // TODO: remove. Only for debugging
        println!("fragments running on workers according to schedule: ");
        for w in schedule.clone().worker_list {
            let mut actors: Vec<u32> = vec![];
            let pus = w.get_parallel_units().iter().map(|pu| pu.id).collect_vec();
            for frag in schedule.clone().get_fragment_list() {
                for act in frag.get_actor_list() {
                    let act_pu = act.parallel_units_id;
                    if pus.contains(&act_pu) {
                        actors.push(act.actor_id);
                    }
                }
            }
            actors.sort();
            println!("worker {} running actors: {:?}", w.id, actors);
        }

        println!("got schedule"); // TODO: remove println

        let mut pu_deleting: Vec<u32> = vec![];
        let pu_all = schedule
            .clone()
            .get_worker_list()
            .iter()
            .map(|worker| worker.parallel_units.iter().map(|pu| pu.id).collect_vec())
            .flatten()
            .dedup()
            .collect_vec();

        // TODO: right now this is O(n*m). We can do better than that
        for host_address in request.into_inner().host_addresses {
            // TODO: be fancy and do this with map
            // Determine worker and parallel units.
            let mut pu_list: Vec<ParallelUnit> = vec![];
            let mut found = false;
            for worker in schedule.get_worker_list() {
                if *worker.get_host()? != host_address {
                    continue;
                }
                pu_list = worker.parallel_units.clone();
                found = true;
                break;
            }

            if !found {
                return Err(Status::from(MetaError::invalid_parameter(format!(
                    "Worker with address {:?} not found",
                    host_address
                ))));
            }

            let mut pu_list = pu_list.iter().map(|pu| pu.id).collect_vec();
            pu_deleting.append(&mut pu_list);
        }

        let mut pu_deleting = pu_deleting.iter().unique().map(|u| u.clone()).collect_vec();
        let pu_all = pu_all.iter().unique().map(|u| u.clone()).collect_vec();

        // TODO: use smarter rescheduling mechanism
        // TODO: use hash maps here
        let mut pu_keep = pu_all
            .iter()
            .filter(|&x| !pu_deleting.contains(x))
            .cloned()
            .collect_vec();

        pu_deleting.sort();
        pu_keep.sort();

        // TODO: remove this and all the functions belonging to it?
        //        let reschedule_map: std::collections::HashMap<u32, reschedule_request::Reschedule>
        // =              each_actor_in_all_first_k(&pu_deleting, &pu_keep,
        // schedule.clone());
        //
        //        let response = self
        //            .reschedule(Request::new(RescheduleRequest {
        //                reschedules: reschedule_map,
        //            }))
        //            .await;
        //
        //        // TODO: remove this
        //        match response {
        //            Ok(_) => {}
        //            Err(e) => {
        //                println!("reschedule response {:?}", e);
        //                assert!(false);
        //            }
        //        }
        // return Ok(Response::new(ClearWorkerNodesResponse { status: None }));

        // Determine which fragment uses which PU on the to be cleared workers.
        let mut remove_map = std::collections::HashMap::new();
        for fragment in schedule.get_fragment_list() {
            // TODO: be fancy and do this with map?
            let mut remove_list: Vec<u32> = vec![];

            for actor in fragment.get_actor_list() {
                // Number of PU o a worker is small, iterating is ok
                let pu_id = actor.parallel_units_id;
                if pu_deleting.contains(&pu_id) {
                    remove_list.push(pu_id);
                }
            }
            if !remove_list.is_empty() {
                remove_map.insert(fragment.id, remove_list);
            }
        }

        println!("created remove_map"); // TODO: remove println
        println!("creating rescheduling request"); // TODO: remove println

        // create rescheduling request

        // TODO: import hashMap
        // TODO: maybe we can remove reschedule func in meta.go from rw-cloud?
        // TODO: maybe move this into the loop above?

        let mut reschedule_map: std::collections::HashMap<u32, reschedule_request::Reschedule> =
            std::collections::HashMap::new();
        for (fragment_id, remove_pus) in remove_map.iter_mut() {
            match reschedule_map.get_mut(fragment_id) {
                Some(reschedule) => {
                    reschedule.removed_parallel_units.append(remove_pus)
                    // reschedule.removed_parallel_units = remove_pus.clone()
                }
                None => {
                    reschedule_map.insert(
                        *fragment_id,
                        reschedule_request::Reschedule {
                            added_parallel_units: vec![], // we have to add PUs here
                            removed_parallel_units: remove_pus.clone(),
                        },
                    );
                }
            }
        }

        println!("send rescheduling request"); // TODO: remove println

        // request fails with "parallel unit x of fragment y is already in use"
        let response = self
            .reschedule(Request::new(RescheduleRequest {
                reschedules: reschedule_map,
            }))
            .await;

        // TODO: remove this
        match response {
            Ok(_) => {}
            Err(e) => {
                println!("reschedule response {:?}", e);
                assert!(false);
            }
        }

        println!("clear_workers done"); // TODO: remove println

        Ok(Response::new(ClearWorkerNodesResponse { status: None }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn get_cluster_info(
        &self,
        _: Request<GetClusterInfoRequest>,
    ) -> Result<Response<GetClusterInfoResponse>, Status> {
        let table_fragments = self
            .fragment_manager
            .list_table_fragments()
            .await?
            .iter()
            .map(|tf| tf.to_protobuf())
            .collect();

        let worker_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, None)
            .await;

        let actor_splits = self
            .source_manager
            .get_actor_splits()
            .await
            .into_iter()
            .map(|(actor_id, splits)| {
                (
                    actor_id,
                    ConnectorSplits {
                        splits: splits.iter().map(ConnectorSplit::from).collect(),
                    },
                )
            })
            .collect();

        let sources = self.catalog_manager.list_sources().await;

        let source_infos = sources.into_iter().map(|s| (s.id, s)).collect();

        Ok(Response::new(GetClusterInfoResponse {
            worker_nodes,
            table_fragments,
            actor_splits,
            source_infos,
        }))
    }

    async fn get_schedule(
        &self,
        _request: Request<GetScheduleRequest>,
    ) -> Result<Response<GetScheduleResponse>, Status> {
        let cluster_info = self
            .get_cluster_info(Request::new(GetClusterInfoRequest {}))
            .await?
            .into_inner();

        // Compile fragments
        let mut fragment_list: Vec<risingwave_pb::common::Fragment> = vec![];
        for table_fragment in cluster_info.get_table_fragments() {
            for (_, fragment) in table_fragment.get_fragments() {
                let mut actor_list: Vec<risingwave_pb::common::Actor> = vec![];
                for actor in fragment.get_actors() {
                    let id = actor.actor_id;
                    let pu_id = table_fragment
                        .get_actor_status()
                        .get(&id)
                        .expect("expected actor status") // TODO: handle gracefully
                        .get_parallel_unit()?
                        .get_id();
                    actor_list.push(risingwave_pb::common::Actor {
                        actor_id: actor.actor_id,
                        parallel_units_id: pu_id,
                    });
                }
                fragment_list.push(risingwave_pb::common::Fragment {
                    id: fragment.get_fragment_id(),
                    actor_list,
                    type_flag: fragment.fragment_type_mask,
                });
            }
        }

        Ok(Response::new(GetScheduleResponse {
            fragment_list: fragment_list,
            worker_list: cluster_info.worker_nodes,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn reschedule(
        &self,
        request: Request<RescheduleRequest>,
    ) -> Result<Response<RescheduleResponse>, Status> {
        let req = request.into_inner();

        println!("in reschedule"); // TODO: remove line

        self.stream_manager
            .reschedule_actors(
                req.reschedules
                    .into_iter()
                    .map(|(fragment_id, reschedule)| {
                        let Reschedule {
                            added_parallel_units,
                            removed_parallel_units,
                        } = reschedule;

                        (
                            fragment_id,
                            ParallelUnitReschedule {
                                added_parallel_units: added_parallel_units
                                    .into_iter()
                                    .sorted()
                                    .dedup()
                                    .collect(),
                                removed_parallel_units: removed_parallel_units
                                    .into_iter()
                                    .sorted()
                                    .dedup()
                                    .collect(),
                            },
                        )
                    })
                    .collect(),
            )
            .await?;

        Ok(Response::new(RescheduleResponse { success: true }))
    }
}
